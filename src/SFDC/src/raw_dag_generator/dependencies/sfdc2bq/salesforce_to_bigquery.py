# Copyright 2022 Google LLC

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
""" This module provides SFDC -> BigQuery extraction code / logic """

from datetime import datetime, timezone, timedelta
import json
import logging
import tempfile
import time
import typing

from google.cloud import bigquery

from simple_salesforce import Salesforce
from simple_salesforce.util import exception_handler

from bigquery_helper import BigQueryHelper  # pylint:disable=wrong-import-position


class SalesforceToBigquery:
    """Class that handles extracting SFDC data to BigQuery"""

    _MAX_RECORDS_PER_BULK_BATCH_ = 100000
    _CSV_STREAM_CHUNK_SIZE_ = 4096
    _RECORD_STAMP_NAME_ = "Recordstamp"

    @staticmethod
    def replicate(simple_sf_connection: Salesforce,
                  api_name: str,
                  bq_client: bigquery.Client,
                  project_id: str,
                  dataset_name: str,
                  output_table_name: str,
                  text_encoding: str,
                  include_non_standard_fields: typing.Union[
                      bool, typing.Iterable[str]] = False,
                  exclude_standard_fields: typing.Iterable[str] = None) -> None:
        """Method to extract data from Salesforce to BigQuery

        Args:
            simple_sf_connection (str): Simple Salesforce connection
            api_name (str): Salesforce object name to replicate
            bq_client (str): BigQuery client
            project_id (str): destination GCP project id
            dataset_name (str): destination dataset name
            output_table_name (str): destination table
            text_encoding (str): text encoding for SFDC text fields
            include_non_standard_fields (bool, Iterable[str]|): whether to
                replicate non-standard fields, True/False or a list of names
            exclude_standard_fields (Iterable[str]): list of standard fields
                to exclude from replication
        """

        logging.info(
            "Preparing Salesforce to BigQuery Replication: %s to %s.%s.%s.",
            api_name, project_id, dataset_name, output_table_name)
        logging.info("Current encoding: %s", text_encoding)
        start_time = time.time()

        # Recordstamp is the start date/time of replication job.
        # We subtract 1 second to account for _possible_
        # implications of SFDC timestamp resolution of 1 second.
        recordstamp = datetime.now(timezone.utc) - timedelta(seconds=1)

        logging.info("Retrieving and parsing source object description")
        desc = simple_sf_connection.restful(f"sobjects/{api_name}/describe/")
        sfdc_fields = [(f["name"], f["type"].lower(), f["relationshipName"],
                        f["referenceTo"])
                       for f in desc["fields"]]  # type: ignore
        # Handling polymorphic fields
        # https://developer.salesforce.com/docs/atlas.en-us.soql_sosl.meta/soql_sosl/sforce_api_calls_soql_relationships_and_polymorph_keys.htm
        relationship_type_fields = []
        for f in sfdc_fields:
            if f[2] and len(f[3]) > 1:
                relationship_type_fields.append(
                    (f"{f[2]}.Type", "string", "", []))
        sfdc_fields.extend(relationship_type_fields)
        sfdc_field_names = [f[0].lower() for f in sfdc_fields]
        has_system_mod_stamp = "systemmodstamp" in sfdc_field_names
        has_is_deleted = "isdeleted" in sfdc_field_names
        has_is_archived = "isarchived" in sfdc_field_names

        if not has_system_mod_stamp:
            logging.fatal("⛔️ % is not supported (no SystemModstamp field).",
                          api_name)
            raise RuntimeError(
                f"{api_name} is not supported (no SystemModstamp field).")

        # SFDC-to-BQ schema
        sfdc_to_bq_field_map = {}
        # Field list for SELECT query
        source_fields = []

        target_id_field = "Id"  # Id field name in the destination BQ table

        # Replicate:
        # primitive field types,
        # https://developer.salesforce.com/docs/atlas.en-us.object_reference.meta/object_reference/primitive_data_types.htm
        # and all non-primitive fields types except `calculated`,
        # https://developer.salesforce.com/docs/atlas.en-us.object_reference.meta/object_reference/field_types.htm
        type_mapping: typing.List[typing.Tuple[typing.List[str], str]] = [
            ([
                "id", "string", "textarea", "base64", "phone", "url", "email",
                "encryptedstring", "picklist", "multipicklist", "reference",
                "junctionidlist", "datacategorygroupreference", "masterrecord",
                "combobox", "anytype"
            ], "STRING"), (["double", "currency", "percent"], "FLOAT64"),
            (["date"], "DATE"), (["time"], "TIME"), (["datetime"], "TIMESTAMP"),
            ([
                "int",
                "long",
                "byte",
            ], "INT64"), (["boolean"], "BOOL")
        ]

        if isinstance(include_non_standard_fields, typing.Iterable):
            non_standard_fields_to_include_low = [
                str(f).lower() for f in include_non_standard_fields
            ]
        else:
            non_standard_fields_to_include_low = None
        if exclude_standard_fields:
            exclude_standard_fields_low = [
                f.lower() for f in exclude_standard_fields
            ]
            # Never remove these fields
            for f in ["isdeleted", "isarchived", "id", "systemmodstamp"]:
                try:
                    exclude_standard_fields_low.remove(f)
                except ValueError:
                    pass
        else:
            exclude_standard_fields_low = []

        for f in sfdc_fields:
            # A non-standard field
            if f[0].endswith("__c"):
                if non_standard_fields_to_include_low:
                    if f[0] not in non_standard_fields_to_include_low:
                        continue
                elif isinstance(include_non_standard_fields,
                                bool) and not include_non_standard_fields:
                    continue
            # May need to remove some standard fields
            elif f[0] in exclude_standard_fields_low:
                continue
            f_type = f[1]
            target_type = None
            for m in type_mapping:
                if f_type in m[0]:
                    target_type = m[1]
                    break
            if not target_type:
                # not supported field type, most likely a compound
                continue
            # Fields mapping dictionary with original name as key and
            # values as tuples of BQ field name and BQ type.
            # Replacing dot with underscore in fields with names of polymorphic
            # types.
            sfdc_to_bq_field_map[f[0]] = (f[0].replace(".", "_"), target_type)
            source_fields.append(f[0])

        # sfdc_to_bq_field_map and source_fields are initialized at this point

        try:
            bq = BigQueryHelper(
                project_id=project_id,
                dataset_name=dataset_name,
                target_table_name=output_table_name,
                job_timestamp=recordstamp,
                id_field_name=target_id_field,
                timestamp_field_name=SalesforceToBigquery._RECORD_STAMP_NAME_,
                has_is_deleted=has_is_deleted,
                has_is_archived=has_is_archived,
                bigquery_client=bq_client)

            include_deleted = bq.incremental_ingestion

            query = SalesforceToBigquery._create_sfdc_query(
                api_name, ",".join(source_fields), recordstamp,
                bq.last_job_timestamp)

            logging.info(
                "Initializing SFDC Bulk API 2.0 job for %s with"
                " query: %s.",
                api_name,
                query,
            )

            if bq.full_ingestion:
                logging.info("This is a full replication job.")
            else:
                logging.info("This is an incremental replication job.")

            job_id = SalesforceToBigquery._bulk_start_job(
                simple_sf_connection, query, include_deleted)

            logging.info("Running SFDC job %s and loading results to BigQuery.",
                         job_id)

            # Starting a Bulk API 2.0 job.
            batches = SalesforceToBigquery._bulk_get_records(
                simple_sf_connection, job_id, text_encoding)

            added_records = SalesforceToBigquery._upload_batches_to_bq(
                bq, batches, sfdc_to_bq_field_map, text_encoding)

            logging.info("Finalizing BigQuery resources.")
            bq.finish_ingestion(added_records == 0)

            logging.info("Total records processed: %i", added_records)

            # Deleting SFDC job.
            # We can only do it now because
            # _upload_batches_to_bq dynamically retrieves results
            # from the generator returned by _bulk_get_records
            logging.info("Deleting SFDC Bulk API 2.0 job %s", job_id)
            SalesforceToBigquery._bulk_delete_job(simple_sf_connection, job_id)

        except Exception:
            logging.error(
                "⛔️ Failed to run Salesforce to BigQuery Replication.\n",
                exc_info=True,
            )
            raise

        end_time = time.time()
        logging.info(
            "Salesforce to BigQuery Replication has been completed in"
            " %f seconds.",
            end_time - start_time,
        )

    @staticmethod
    def _bulk_start_job(sfdc_connection: Salesforce,
                        query: str,
                        include_deleted: bool = False) -> str:
        """Starts Salesforce Bulk API 2.0 query job.

        Args:
            sfdc_connection (Salesforce): Salesforce connection
            query (str): Salesforce query for Bulk API 2.0
            include_deleted (bool, optional): Whether to include
                deleted records. Defaults to False.

        Returns:
            str: Job Id
        """
        operation = "queryAll" if include_deleted else "query"
        request_body = {
            "operation": operation,
            "query": query,
            "contentType": "CSV",
            "columnDelimiter": "COMMA",
            "lineEnding": "LF",
        }

        # Start a job
        job = sfdc_connection.restful(path="jobs/query",
                                      method="POST",
                                      data=json.dumps(request_body))
        return job["id"]

    @staticmethod
    def _bulk_get_records(
        sfdc_connection: Salesforce,
        job_id: str,
        text_encoding: str,
        job_status_interval: float = 10.0,
    ) -> typing.Iterable[typing.Iterable[str]]:
        """Retrieves CSV content of Salesforce Build API 2.0 query results
            as batches of CSV lines.

        Args:
            sfdc_connection (Salesforce): Salesforce connection
            job_id (str): Salesforce Bulk API 2.0 job to retrieve results from
            text_encoding (str): Text encoding to use
            job_status_interval (float, optional): Job status polling interval
                in seconds. Defaults to 10.0.

        Raises:
            RuntimeError: Job failed.

        Yields:
            Iterator[typing.Iterable[typing.Iterable[str]]]:
                iterable of the result CSV files content
                as iterables of content chunks.
        """

        # Checking for job status every job_status_interval seconds.
        job_status_path = f"jobs/query/{job_id}"
        job_running = True
        while job_running:
            time.sleep(job_status_interval)
            status = sfdc_connection.restful(path=job_status_path, method="GET")
            state = status["state"]
            if state in ["Failed", "Aborted"]:
                logging.fatal("⛔️ Operation %s %s: %s", job_id, state,
                              status["errorMessage"])
                raise RuntimeError(
                    f"Operation {job_id} {state}: {status['errorMessage']}")
            elif state == "JobComplete":
                break

        locator = None

        # Retrieve job results.
        while locator != "null":
            headers = sfdc_connection.headers.copy()
            result_path = (
                f"jobs/query/{job_id}/results?maxRecords="
                f"{SalesforceToBigquery._MAX_RECORDS_PER_BULK_BATCH_}")
            if locator:
                result_path += f"&locator={locator}"
            with sfdc_connection.session.request(
                    "GET",
                    f"{sfdc_connection.base_url}{result_path}",
                    headers=headers,
                    stream=True,
            ) as result_response:
                if result_response.status_code == 401:
                    # Auth token might have expired,
                    # Let simple-salesforce renew it
                    # by performing a restful call on the job status
                    sfdc_connection.restful(path=job_status_path, method="GET")
                    continue
                elif result_response.status_code >= 300:
                    # Error codes >= 300 mean an error,
                    # except when it's 404 and the locator is not None.
                    # It such cases, job has been deleted,
                    # and there is nothing more to get.
                    if (result_response.status_code == 404 and
                            locator is not None):
                        logging.warning(
                            "⚠️ SFDC Bulk API 2.0 job %s was deleted.", job_id)
                        locator = "null"
                    else:
                        # Let simple-salesforce handle it.
                        exception_handler(result_response, name=result_path)
                else:
                    if "Sforce-Locator" in result_response.headers:
                        locator = result_response.headers["Sforce-Locator"]
                    else:
                        # No locator means there is only one set of results,
                        # but we explicitly assign if to a special "null" value
                        # because this is what's returned when the last set
                        # was retrieved in the multiple-batch situation.
                        locator = "null"
                    result_response.encoding = text_encoding
                    yield result_response.iter_content(
                        chunk_size=SalesforceToBigquery._CSV_STREAM_CHUNK_SIZE_,
                        decode_unicode=True)

    @staticmethod
    def _bulk_delete_job(sfdc_connection: Salesforce, job_id):
        # Delete job to free up Salesforce job storage.
        job_status_path = f"jobs/query/{job_id}"
        sfdc_connection.session.request(
            "DELETE",
            f"{sfdc_connection.base_url}{job_status_path}",
            headers=sfdc_connection.headers.copy(),
        ).close()

    @staticmethod
    def _create_sfdc_query(
            api_name: str, column_list: str, job_recordstamp: datetime,
            last_record_stamp: typing.Union[datetime, None]) -> str:
        """Building SFDC query depending on the incremental logic."""

        recordstamp_str = job_recordstamp.strftime("%Y-%m-%dT%H:%M:%S.000Z")

        query = (f"SELECT {column_list} FROM {api_name} "
                 f"WHERE SystemModstamp<={recordstamp_str}")
        if last_record_stamp:
            # Maximum duration of Salesforce Apex transaction is 10 minutes.
            # By handling 10 minutes prior to the last recordstamp,
            # we account for transactions that were in-flight
            # during the prior replication
            last_record_stamp_minus_ten = last_record_stamp - timedelta(
                minutes=10)
            last_record_stamp_str = last_record_stamp_minus_ten.strftime(
                "%Y-%m-%dT%H:%M:%S.000Z")
            query += f" AND SystemModstamp>={last_record_stamp_str}"

        return query

    @staticmethod
    def _upload_batches_to_bq(bq: BigQueryHelper,
                              batches: typing.Iterable[typing.Iterable[str]],
                              sfdc_to_bq_field_map: typing.Dict[
                                  str, typing.Tuple[str, str]],
                              text_encoding: str) -> int:
        """Processes batches of Salesforce Bulk API 2.0 query.
        It retrieves CSV lines from the Bulk API batches,
        renames the header with the target names,
        appends extra fields,
        saves every batch to a CSV file,
        and calls _run_bq_load_job to load the CSV to BigQuery.

        Args:
            bq (BigQueryHelper): BigQueryHelper object to use.
            batches (typing.Iterable[typing.Iterable[str]]):
                generator returned by _bulk_get_records call.
            sfdc_to_bq_field_map (typing.Dict[str, typing.Tuple[str, str]]):
                Salesforce-to-BigQuery field name mapping dictionary.
            text_encoding: Text encoding to use.

        Returns:
            int: Number of added records.
        """
        batch_count = 0
        record_count = 0
        started_bq_ingestion = False

        for batch in batches:
            batch_count += 1
            first_line = True

            logging.info("Working on batch %i", batch_count)
            with tempfile.NamedTemporaryFile(
                    "w",
                    encoding=text_encoding,
                    prefix=f"{bq.target_table_name}_",
                    suffix=".csv",
            ) as file:
                has_valid_lines = False

                # Processing lines from the returned CSV.
                # We need to rename fields in the header (first line).

                for chunk in batch:
                    if first_line:
                        index = chunk.find("\n")
                        if index != -1:
                            first_line = False
                            if index < len(chunk) - 1:
                                has_valid_lines = True
                    else:
                        has_valid_lines = True

                    file.write(chunk)

                file.flush()

                if not started_bq_ingestion:
                    bq.start_ingestion(list(sfdc_to_bq_field_map.values()))
                    started_bq_ingestion = True

                if has_valid_lines:
                    record_count += bq.load_batch_csv(file.name)
                else:
                    logging.info("No BigQuery records in this batch.")
        return record_count
