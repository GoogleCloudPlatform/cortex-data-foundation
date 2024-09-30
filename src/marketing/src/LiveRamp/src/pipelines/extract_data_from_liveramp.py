# Copyright 2023 Google LLC

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Functions to fetch data from LiveRamp Lookup API to BigQuery table.

This file loads data for Cortex Marketing workload from LiveRamp.
It calls LiveRamp Lookup API.
Result is loaded in the provided BigQuery table.
"""

from datetime import datetime
import logging
import time
import uuid

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery

import sys
import pathlib

__this_dir = pathlib.Path(__file__).parent.absolute()
sys.path.append(str(__this_dir))

# pylint: disable=wrong-import-position
from helpers_bq import create_temporary_tables
from helpers_bq import delete_temporary_tables
from helpers_bq import get_batch_of_rows_of_current_segment
from helpers_bq import get_segments_from_bq
from helpers_bq import insert_batch_into_request_input
from helpers_bq import insert_response_into_request_output
from helpers_bq import update_input_and_output_tables
from helpers_liveramp import extract_ramp_ids_from_response
from helpers_liveramp import fetch_rampids_for_hashed_pii_details
from helpers_liveramp import get_token
from helpers_liveramp import validate_search_strings

# LiveRamp API expects a minimum number of records in a given call.
_LIVERAMP_API_MATCH_MIN_QUOTA = 100
# Maximum number of records supported by LiveRamp API.
_BATCH_SIZE = 1000
# Delay between requests in seconds.
# This is required to not exceed the Abilitec Lookup endpoint rate limit.
# https://developers.liveramp.com/abilitec-api/reference/rate-limits
_DELAY_IN_SEC = 0.2  # 5 requests / second.

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def extract_ramp_ids_from_liveramp(project_id: str, dataset_id: str,
                                   http_timeout: int,
                                   liveramp_lookup_api_url: str,
                                   liveramp_auth_url: str,
                                   bq_connection_id: str):
    """Fetches entities from LiveRamp Lookup API.

    See the API overview below:
    https://developers.liveramp.com/rampid-api/docs/the-lookup-endpoint-1

    Args:
        project_id (str): Target BigQuery project.
        dataset_id (str): Target BigQuery dataset.
        http_timeout (int): Timeout for HTTP calls.
        liveramp_lookup_api_url (str): Lookup API URL.
        liveramp_auth_url (str): LiveRamp auth URL.
        bq_connection_id (str): BigQuery Airflow connection ID for LiverRamp.

    Raises:
        e: Any error fails the DAG run.
    """

    logger.info("Starting RampID lookup.")

    if bq_connection_id:
        # BigQuery hook created with a connection ID.
        bq_hook = BigQueryHook(bq_connection_id)
        bq_client = bq_hook.get_client(project_id)
    else:
        # When no connection ID is presented use the default connection.
        bq_client = bigquery.Client(project_id)

    token = get_token(project_id=project_id,
                      url=liveramp_auth_url,
                      timeout=http_timeout)

    segments = get_segments_from_bq(client=bq_client,
                                    project=project_id,
                                    dataset=dataset_id)

    if not segments:
        logger.info(
            "No unprocessed segments found in the input table. Skipping.")

    # Fetch LiveRamp RampIDs for each segment.
    # For each segment, create batches of records to be sent to LiveRamp
    # based on LiveRamp API min and max number of records allowed in each
    # call.
    for segment in segments:
        segment_name, segment_size = segment
        logger.info("Processing segment '%s'. Total rows: %d.", segment_name,
                    segment_size)

        # Skipping segments that contain fewer records than the minimum.
        if segment_size < _LIVERAMP_API_MATCH_MIN_QUOTA:
            logger.warning(("Segment size %d is less than LiveRamp API's "
                            "required minimum (%d). Skipping segment."),
                           segment_size, _LIVERAMP_API_MATCH_MIN_QUOTA)
            continue

        # Create a temp table and get data for the current segment.
        batches = get_batch_of_rows_of_current_segment(
            segment_name=segment_name,
            client=bq_client,
            batch_size=_BATCH_SIZE,
            project=project_id,
            dataset=dataset_id)

        last_batch = None
        current_batch = None

        for batch_number, batch in enumerate(batches):
            # For each batch of the segment, create intermediate input and
            # output tables, process one batch at a time, and update actual
            # input and output tables as the final step.

            current_batch = batch

            # Delay to limit request numbers.
            time.sleep(_DELAY_IN_SEC)

            num_of_records_in_batch = len(current_batch)

            logger.info("Processing batch %d, size %d.", batch_number,
                        num_of_records_in_batch)

            if num_of_records_in_batch >= _LIVERAMP_API_MATCH_MIN_QUOTA:
                # Store the last minimal quota of records in memory in case
                # if the next batch will contain less records
                # than minimal quota.
                last_batch = current_batch[(-1 *
                                            _LIVERAMP_API_MATCH_MIN_QUOTA):]
            else:
                current_batch = current_batch + last_batch

            # Generate uuid for current process.
            batch_id = uuid.uuid4()

            # Create tables for processing data.
            create_temporary_tables(bq_client, project_id, dataset_id, batch_id)

            # Insert batch into temporary input table.
            insert_batch_into_request_input(client=bq_client,
                                            project=project_id,
                                            dataset=dataset_id,
                                            batch_id=batch_id,
                                            batch=current_batch)

            try:
                validated_hashes = validate_search_strings(current_batch)

                # When token is expiring get a new one.
                utc_now_ts = int(datetime.utcnow().timestamp())

                if token.is_expired(utc_now_ts=utc_now_ts):
                    token = get_token(project_id=project_id,
                                      url=liveramp_auth_url,
                                      timeout=http_timeout)

                response = fetch_rampids_for_hashed_pii_details(
                    access_token=token,
                    hash_list=validated_hashes,
                    url=liveramp_lookup_api_url,
                    timeout=http_timeout)

                # Transform response to a list of tuples.
                response_data = extract_ramp_ids_from_response(
                    segment_name=segment_name, response=response)

                # Insert response data into temporary table for processing.
                insert_response_into_request_output(
                    bq_client,
                    project_id,
                    dataset_id,
                    batch_id,
                    response_data,
                )

                # Update input and output tables with new data.
                update_input_and_output_tables(client=bq_client,
                                               project=project_id,
                                               dataset=dataset_id,
                                               batch_id=batch_id)
                logger.info("Finished processing batch %d.", batch_number)

            except Exception as e:
                logger.error("Error while processing Ramp IDs. %s", e)

                raise e

            finally:
                delete_temporary_tables(client=bq_client,
                                        project=project_id,
                                        dataset=dataset_id,
                                        batch_id=batch_id)

        logger.info("Finished processing segment '%s'.", segment_name)

    logger.info("Finished RampID lookup.")
