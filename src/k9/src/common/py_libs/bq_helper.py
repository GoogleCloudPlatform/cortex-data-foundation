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
"""Library for BigQuery related functions."""

from collections import abc
from enum import Enum
import logging
import typing
import pathlib

from google.cloud import bigquery
from google.cloud.exceptions import NotFound, BadRequest
from google.api_core import retry
from google.cloud.exceptions import Conflict

logger = logging.getLogger(__name__)


def execute_sql_file(bq_client: bigquery.Client,
                     sql_file: typing.Union[str, pathlib.Path]) -> None:
    """Executes a Bigquery sql file."""
    # TODO: Convert sql_file from str to Path.
    with open(sql_file, mode="r", encoding="utf-8") as sqlf:
        sql_str = sqlf.read()
        logging.debug("Executing SQL: %s", sql_str)
        try:
            query_job = bq_client.query(sql_str)
            # Let's wait for query to complete.
            _ = query_job.result()
        except BadRequest:
            logging.error("Error when executing SQL:\n%s", sql_str)
            raise


def table_exists(bq_client: bigquery.Client, full_table_name: str) -> bool:
    """Checks if a given table exists in BigQuery."""
    try:
        bq_client.get_table(full_table_name)
        return True
    except NotFound:
        return False


def dataset_exists(bq_client: bigquery.Client, full_dataset_name: str) -> bool:
    """Checks if a given dataset exists in BigQuery."""
    try:
        bq_client.get_dataset(full_dataset_name)
        return True
    except NotFound:
        return False


DatasetExistence = Enum("DatasetExistence",
                        ["NOT_EXISTS",
                         "EXISTS_IN_LOCATION",
                         "EXISTS_IN_ANOTHER_LOCATION"])

def dataset_exists_in_location(bq_client: bigquery.Client,
                               full_dataset_name: str,
                               location: str) -> DatasetExistence:
    """Checks if a given dataset exists in BigQuery in a location."""
    try:
        dataset = bq_client.get_dataset(full_dataset_name)
        return (DatasetExistence.EXISTS_IN_LOCATION
                  if dataset.location.lower() == location.lower() # type: ignore
                  else DatasetExistence.EXISTS_IN_ANOTHER_LOCATION)
    except NotFound:
        return DatasetExistence.NOT_EXISTS


def _wait_for_bq_jobs(jobs: typing.List[typing.Union[bigquery.CopyJob,
                                                  bigquery.LoadJob]],
                      continue_if_failed: bool):
    """Waits for BigQuery jobs to finish."""

    # We can simply wait for them in a for loop
    # because we need all of them to finish.
    for job in jobs:
        try:
            job.result(retry=retry.Retry(deadline=60))
            logging.info("✅ Table %s has been loaded.", job.destination)
        except Conflict:
            logging.warning("⚠️ Table %s already exists. Skipping it.",
                        job.destination)
        except Exception:
            logging.error(
                "⛔️ Failed to load table %s.\n",
                job.destination,
                exc_info=True)
            if not continue_if_failed:
                raise


def load_tables(bq_client: bigquery.Client,
               sources: typing.Union[str, typing.List[str]],
               target_tables: typing.Union[str, typing.List[str]],
               location: str,
               continue_if_failed: bool = False,
               skip_existing_tables: bool = False,
               write_disposition: str = bigquery.WriteDisposition.WRITE_EMPTY,
               parallel_jobs: int = 5):
    """Loads data to multiple BigQuery tables.

    Args:
        bq_client (bigquery.Client): BigQuery client to use.
        sources (str|list[str]): data source URI or name.
                               Supported sources:
                                - BigQuery table name as project.dataset.table
                                - Any URI supported by load_table_from_uri
                                  for avro, csv, json and parquet files.
        target_table_full_names (str|list[str]): full target tables names as
                                      "project.dataset.table".
        location (str): BigQuery location.
        continue_if_failed (bool): continue loading tables if some jobs fail.
        skip_existing_tables (bool): Skip tables that already exist.
                                    Defaults to False.
        write_disposition (bigquery.WriteDisposition): write disposition,
                          Defaults to WRITE_EMPTY (skip if has data).
        parallel_jobs (int): maximum number of parallel jobs. Defaults to 5.
    """

    if not isinstance(sources, abc.Sequence):
        sources = [sources]
    if not isinstance(target_tables, abc.Sequence):
        target_tables = [target_tables]
    if len(target_tables) != len(sources):
        raise ValueError(("Number of source URIs must be equal to "
                          "number of target tables."))

    jobs = []
    for index, source in enumerate(sources):
        target = target_tables[index]
        logging.info("Loading table %s from %s.", target, source)

        if skip_existing_tables and table_exists(bq_client, target):
            logging.warning("⚠️ Table %s already exists. Skipping it.", target)
            continue

        if "://" in source:
            ext = source.split(".")[-1].lower()
            if ext == "avro":
                source_format = bigquery.SourceFormat.AVRO
            elif ext == "parquet":
                source_format = bigquery.SourceFormat.PARQUET
            elif ext == "csv":
                source_format = bigquery.SourceFormat.CSV
            elif ext == "json":
                source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
            else:
                raise ValueError((f"Extension `{ext}` "
                                 "is an unsupported source format."))
            job_config = bigquery.LoadJobConfig(
                autodetect=True,
                source_format=source_format,
                write_disposition=write_disposition,
            )
            load_job = bq_client.load_table_from_uri(
                source_uris=source,
                destination=target,
                job_config=job_config,
                location=location,
                retry=retry.Retry(deadline=60),
            )
        else:
            job_config = bigquery.CopyJobConfig(
                write_disposition=write_disposition)
            load_job = bq_client.copy_table(source,
                                            target,
                                            location=location,
                                            job_config=job_config,
                                            retry=retry.Retry(deadline=60))
        jobs.append(load_job)
        # If reached parallel_jobs number, wait for them to finish.
        if len(jobs) >= parallel_jobs:
            _wait_for_bq_jobs(jobs, continue_if_failed)
            jobs.clear()

    # Wait for the rest of jobs to finish.
    _wait_for_bq_jobs(jobs, continue_if_failed)


def create_dataset(bq_client: bigquery.Client,
                   dataset_name: str,
                   location: str,
                   suppress_success_logging: bool = False) -> None:
    """Creates a BigQuery dataset."""
    dataset_ref = bigquery.Dataset(dataset_name)
    dataset_ref.location = location
    try:
        bq_client.create_dataset(dataset_ref, timeout=30)
        if not suppress_success_logging:
            logging.info("✅ Dataset %s has been created in %s.",
                        dataset_name,
                        location)
    except Conflict:
        logging.warning("⚠️ Dataset %s already exists in %s. Skipping it.",
                        dataset_name,
                        location)
    except Exception:
        logging.error("⛔️ Failed to create dataset %s in %s.", dataset_name,
                      location,
                      exc_info=True)
        raise


def create_table(bq_client: bigquery.Client,
                 full_table_name: str,
                 schema_tuples_list: list[tuple[str, str]],
                 exists_ok=False) -> None:
    """Creates a BigQuery table based on given schema."""
    project, dataset_id, table_id = full_table_name.split(".")

    table_ref = bigquery.TableReference(
        bigquery.DatasetReference(project, dataset_id), table_id)

    table = bigquery.Table(
        table_ref,
        schema=[bigquery.SchemaField(t[0], t[1]) for t in schema_tuples_list])

    bq_client.create_table(table, exists_ok=exists_ok)


def get_table_list(bq_client: bigquery.Client,
                   project_id: str,
                   dataset_name: str) -> typing.List[str]:
    ds_ref = bigquery.DatasetReference(project_id, dataset_name)
    return [t.table_id for t in bq_client.list_tables(ds_ref)]


def delete_table(bq_client: bigquery.Client, full_table_name: str) -> None:
    """ Calls the BQ API to delete the table, returns nothing """
    logger.info("Deleting table `%s`.", full_table_name)
    bq_client.delete_table(full_table_name, not_found_ok=True)


def copy_dataset(bq_client: bigquery.Client,
                 source_project: str,
                 source_dataset: str,
                 target_project: str,
                 target_dataset: str,
                 location: str,
                 skip_existing_tables: bool = False,
                 write_disposition: str = (
                    bigquery.WriteDisposition.WRITE_EMPTY)):
    """Copies all tables from source dataset to target.

    Args:
        bq_client (bigquery.Client): BigQuery client to use.
        source_project (str): Source project.
        source_dataset (str): Source dataset. Must exist in specified location.
        target_project (str): Target project.
        target_dataset (str): Target dataset. Must exist in specified location.
        location (str): BigQuery location.
        continue_if_failed (bool): Continue loading tables if some jobs fail.
        skip_existing_tables (bool): Skip tables that already exist.
                                    Defaults to False.
        write_disposition (bigquery.WriteDisposition): Write disposition,
                          Defaults to WRITE_EMPTY (skip if has data).
    """
    logging.info("Copying tables from `%s.%s` to `%s.%s`.",
                 source_project, source_dataset,
                 target_project, target_dataset)
    tables = get_table_list(bq_client, source_project, source_dataset)
    source_tables = [f"{source_project}.{source_dataset}.{t}" for t in tables]
    target_tables = [f"{target_project}.{target_dataset}.{t}" for t in tables]
    load_tables(bq_client,
                source_tables, target_tables, location,
                skip_existing_tables=skip_existing_tables,
                write_disposition=write_disposition)
