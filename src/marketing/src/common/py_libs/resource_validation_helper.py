# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Cloud resource validation helper functions."""

import logging
import typing
import uuid

from google.cloud.exceptions import (NotFound,
                                    BadRequest,
                                    GoogleCloudError,
                                    ServerError,
                                    Unauthorized, Forbidden)
from google.cloud import bigquery, storage

from py_libs import bq_helper


class BucketConstraints:
    """Bucket Validation Constraints"""
    def __init__(self, name: str, must_be_writable: bool,
                 in_location: typing.Optional[str]):
        """Bucket Validation Constraints

        Args:
            name (str): bucket name.
            must_be_writable (bool): bucket must be writable if True.
            in_location (typing.Optional[str]): bucket must be in this location.
                                                Location may be a multi-region,
                                                and buckets in specific regions
                                                are valid if in it's in provided
                                                multi-region -
                                                e.g. "us-central1" is valid
                                                if in_location is "US" or "us"
        """
        self.name = name
        self.must_be_writable = must_be_writable
        self.in_location = in_location.upper() if in_location else None


class DatasetConstraints:
    """Dataset Validation Constraints"""
    def __init__(self, full_name: str,
                 must_exists: bool,
                 must_be_writable: bool,
                 location: str):
        """Dataset Validation Constraints

        Args:
            full_name (str): dataset full name as `project.dataset`
            must_exists (bool): dataset must exist if True.
            must_be_writable (bool): existing dataset must be writable if True.
            location (str): existing dataset must be in this location.
        """
        self.full_name = full_name
        self.must_exists = must_exists
        self.must_be_writable = must_be_writable
        self.location = location.upper()


def validate_resources(
        buckets: typing.Iterable[BucketConstraints],
        datasets: typing.Iterable[DatasetConstraints]) -> bool:
    """Validates Cloud Storage Buckets and BigQuery Datasets.

    Args:
        buckets (typing.Iterable[BucketConstraints]): bucket constraints
        datasets (typing.Iterable[DatasetConstraints]): dataset constraints

    Returns:
        bool: True if all buckets and datasets are valid.
    """
    storage_client = storage.Client()
    bq_client = bigquery.Client()
    for bucket in buckets:
        checking_on_writing = False
        try:
            bucket_object = storage_client.get_bucket(bucket.name)
            if bucket.in_location:
                bucket_location = bucket_object.location.upper()
                in_location = bucket.in_location.upper()
                if "-" in in_location:
                    valid_location = bucket_location == in_location
                else:
                    valid_location = (
                        bucket_location == in_location or
                            bucket_location.startswith(f"{in_location}-"))
                if not valid_location:
                    logging.error("🛑 Storage bucket `%s` is in "
                            "location `%s`, but expected to be in `%s`. 🛑",
                            bucket.name,
                            bucket_object.location,
                            bucket.in_location)
                    return False
            logging.info("✅ Storage bucket `%s` exists. It's location is `%s`.",
                         bucket.name, bucket_object.location)
            if bucket.must_be_writable:
                checking_on_writing = True
                temp_file_name = f"tmp_cortex_{uuid.uuid4().hex}"
                blob = bucket_object.blob(temp_file_name)
                blob.upload_from_string("Cortex!")
                logging.info("✅ Storage bucket `%s` is writable.", bucket.name)
                try:
                    blob.delete()
                except Exception:
                    logging.warning("⚠️ Couldn't delete temporary file "
                                "`gs://%s/%s`. Please delete it manually. ⚠️",
                                bucket.name, blob.name)
        except GoogleCloudError as ex:
            if isinstance(ex, NotFound):
                logging.error("🛑 Storage bucket `%s` doesn't exist. 🛑",
                              bucket.name)
            elif isinstance(ex, Unauthorized) or isinstance(ex, Forbidden):
                if checking_on_writing:
                    logging.error("🛑 Storage bucket `%s` "
                                "is not writable. 🛑", bucket.name)
                else:
                    logging.error("🛑 Access to storage bucket `%s` "
                                  "was denied. 🛑",
                                  bucket.name)
            else:
                logging.error("🛑 Error when checking on "
                              "storage bucket `%s`. 🛑", bucket.name,
                              exc_info=True)
            return False
    for dataset in datasets:
        existence = bq_helper.dataset_exists_in_location(bq_client,
                                                        dataset.full_name,
                                                        dataset.location)
        if existence == bq_helper.DatasetExistence.EXISTS_IN_ANOTHER_LOCATION:
            logging.error("🛑 Dataset `%s` is not "
                              "in location `%s`. 🛑",
                              dataset.full_name, dataset.location)
            return False
        elif (dataset.must_exists and
                    existence == bq_helper.DatasetExistence.NOT_EXISTS):
            logging.error("🛑 Dataset `%s` doesn't exist "
                          "or not accessible. 🛑",
                          dataset.full_name)
            return False
        if existence != bq_helper.DatasetExistence.NOT_EXISTS:
            logging.info("✅ Dataset `%s` exists in location `%s`.",
                         dataset.full_name, dataset.location)
        if dataset.must_be_writable and (
                existence == bq_helper.DatasetExistence.EXISTS_IN_LOCATION):
            try:
                temp_table_name = f"tmp_cortex_{uuid.uuid4().hex}"
                temp_table_schema = [("FLAG", "BOOL")]
                full_temp_table_name = f"{dataset.full_name}.{temp_table_name}"
                logging.info("Creating temporary table `%s`.",
                             full_temp_table_name)
                bq_helper.create_table(bq_client,
                                       full_temp_table_name,
                                       temp_table_schema) # type: ignore
                logging.info("✅ Dataset `%s` is writable.",
                             dataset.full_name)
                try:
                    bq_helper.delete_table(bq_client, full_temp_table_name)
                except (BadRequest, ServerError):
                    logging.warning("⚠️ Failed to delete temp table %s. "
                                    "Please delete it manually.",
                                    full_temp_table_name,
                                    exc_info=True)
            except (BadRequest, Unauthorized, Forbidden, ServerError):
                logging.error("🛑 Couldn't write to dataset `%s`. 🛑",
                              dataset.full_name)
                return False
    return True
