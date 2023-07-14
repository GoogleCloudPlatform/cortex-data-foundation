# Copyright 2022 Google LLC
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
               write_disposition: str = bigquery.WriteDisposition.WRITE_EMPTY,
               parallel_jobs: int = 5):
    """Loads data to a BigQuery table.

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
                                            retry=retry.Retry(deadline=60))
        jobs.append(load_job)
        # If reached parallel_jobs number, wait for them to finish.
        if len(jobs) >= parallel_jobs:
            _wait_for_bq_jobs(jobs, continue_if_failed)
            jobs.clear()

    # Wait for the rest of jobs to finish.
    _wait_for_bq_jobs(jobs, continue_if_failed)


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


def copy_dataset(bq_client: bigquery.Client,
                 source_project: str,
                 source_dataset: str,
                 target_project: str,
                 target_dataset: str,
                 location: str,
                 write_disposition: str = (
                    bigquery.WriteDisposition.WRITE_EMPTY)):
    logging.info("Copying tables from `%s.%s` to `%s.%s`.",
                 source_project, source_dataset,
                 target_project, target_dataset)
    tables = get_table_list(bq_client, source_project, source_dataset)
    source_tables = [f"{source_project}.{source_dataset}.{t}" for t in tables]
    target_tables = [f"{target_project}.{target_dataset}.{t}" for t in tables]
    load_tables(bq_client,
                source_tables, target_tables,
                location, write_disposition=write_disposition)
