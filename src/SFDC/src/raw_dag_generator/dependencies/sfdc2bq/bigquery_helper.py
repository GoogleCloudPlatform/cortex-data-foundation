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
""" BigQuery-specific operations for SFDC -> BigQuery ingestion.  """

from datetime import datetime, timezone, timedelta
import logging
from pathlib import Path
import typing

from google.cloud.exceptions import NotFound, GoogleCloudError
from google.cloud import bigquery


class BigQueryHelper:
    """BigQuery-specific operations of SFDC ingestion"""

    _TEMP_TABLE_EXPIRATION_DAYS_ = 1

    def __init__(
        self,
        project_id: str,
        dataset_name: str,
        target_table_name: str,
        job_timestamp: datetime,
        id_field_name: str,
        timestamp_field_name: str,
        has_is_deleted: bool,
        has_is_archived: bool,
        bigquery_client: bigquery.Client = None,
    ):
        """BigQueryHelper constructor.

        Args:
            project_id (str): Target GCP Project id.
            dataset_name (str): Target Dataset name.
            target_table_name (str): Target Table name.
            job_timestamp (datetime): Current job start time.
            id_field_name (str): Name of the Id field.
            timestamp_field_name (str): Name of the job timestamp field.
            has_is_deleted (bool): Whether the table has IsDeleted field.
            has_is_archived (bool): Whether the table has IsArchived field.
            bigquery_client (bigquery.Client, optional): BigQuery client to use.
                Defaults to None.
        """
        self.client = bigquery_client if bigquery_client else bigquery.Client()
        self.project_id = project_id
        self.dataset_name = dataset_name
        self.target_table_name = target_table_name
        self.schema = []
        self.job_config: bigquery.LoadJobConfig = None
        self.last_job_timestamp: datetime = None

        self._ingestion_started = False

        self.timestamp_field_name = timestamp_field_name
        self.id_field_name = id_field_name
        self.has_is_deleted = has_is_deleted
        self.has_is_archived = has_is_archived

        self.target_table_ref = bigquery.TableReference(
            bigquery.DatasetReference(self.project_id, self.dataset_name),
            self.target_table_name,
        )

        self.job_timestamp = job_timestamp
        timestamp_now = int(self.job_timestamp.timestamp() * 1e9)
        self.temp_table_name = f"tmp_{target_table_name}_{timestamp_now}"

        self.temp_table_ref = bigquery.TableReference(
            bigquery.DatasetReference(self.project_id, self.dataset_name),
            self.temp_table_name,
        )
        self._retrieve_last_job_timestamp()

    full_ingestion = property(lambda self: self.last_job_timestamp is None)
    """ Performing full ingestion """

    incremental_ingestion = property(lambda self: not self.full_ingestion)
    """ Performing incremental ingestion """

    def start_ingestion(self, bq_fields: typing.List[typing.Tuple[str, str]]):
        """Initializes BigQuery ingestion:
            1. Initializes table schema.
            2. Creates temporary table.
            3. Initializes bq load job configuration.

        Args:
            bq_fields (typing.List[typing.Tuple[str, str]]): Table schema
                as a list of tuples (Field Name, BigQuery Type)

        Raises:
            RuntimeError: thrown if the ingestion was started before.
        """
        if self._ingestion_started:
            raise RuntimeError("Ingestion already started.")

        logging.info(
            "Received SFDC data. Preparing BigQuery destination resources.")

        self.schema.clear()
        dest_fields_lower = [f[0].lower() for f in bq_fields]

        if self.id_field_name.lower() not in dest_fields_lower:
            self.schema.append(
                bigquery.SchemaField(name=self.id_field_name,
                                     field_type="STRING"))

        for f in bq_fields:
            self.schema.append(bigquery.SchemaField(name=f[0], field_type=f[1]))

        # Making sure the schema has IsDeleted and/or IsArchived
        # if the source SFDC object has it.
        if self.has_is_deleted and "isdeleted" not in dest_fields_lower:
            self.schema.append(
                bigquery.SchemaField(name="IsDeleted", field_type="BOOL"))
        if self.has_is_archived and "isarchived" not in dest_fields_lower:
            self.schema.append(
                bigquery.SchemaField(name="IsArchived", field_type="BOOL"))

        # Making sure timestamp field (Recordstamp)
        # id in the schema.
        if self.timestamp_field_name.lower() not in dest_fields_lower:
            self.schema.append(
                bigquery.SchemaField(name=self.timestamp_field_name,
                                     field_type="TIMESTAMP"))

        table_obj = self.client.create_table(bigquery.Table(
            self.temp_table_ref, self.schema),
                                             exists_ok=False)
        table_obj.expires = datetime.now(timezone.utc) + timedelta(
            days=BigQueryHelper._TEMP_TABLE_EXPIRATION_DAYS_)
        self.client.update_table(table_obj, ["expires"])
        self.temp_table_ref = table_obj.reference

        self.job_config = bigquery.LoadJobConfig(
            autodetect=True,
            skip_leading_rows=1,
            schema=table_obj.schema,
            schema_update_options=[
                # New fields will make it too
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
            ],
            source_format=bigquery.SourceFormat.CSV,
            allow_quoted_newlines=True,
            allow_jagged_rows=True,
            null_marker="",
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )

        self._ingestion_started = True

    def load_batch_csv(
        self,
        csv_batch_file: str,
    ) -> int:
        """Loads CSV file into BigQuery

        Args:
            csv_batch_file (str): CSV file path.

        Returns:
            int: Number of inserted rows.
        """

        if not self.schema or len(self.schema) == 0:
            raise RuntimeError("BigQuery parameters are not initialized."
                               "Use start_ingestion first.")

        logging.info(
            "Loading a data batch from %s (%i bytes) to BigQuery table %s.",
            csv_batch_file,
            Path(csv_batch_file).stat().st_size,
            self.temp_table_ref,
        )

        with open(csv_batch_file, "rb") as file:
            job = self.client.load_table_from_file(
                file,
                self.temp_table_ref,
                job_config=self.job_config,
                project=self.temp_table_ref.project,
            )
            job.result()
            logging.info("Done. %i rows were added.", job.output_rows)
            return job.output_rows

    def finish_ingestion(self, finish_empty_job: bool):
        """Finalizes BigQuery ingestion:
            1. Extends destination table schema if needed.
            2. Executes merging from temporary table to the destination.
            3. Deletes temporary table.

        Args:
            finish_empty_job (bool): True if no rows were ingested.

        """
        if not self._ingestion_started:
            raise RuntimeError("Nothing to finish. Call start_ingestion first.")

        logging.info("Committing replicated data to %s", self.target_table_ref)

        # Extending target table's schema if needed
        modified_schema = False
        # for start, it's just temp table schema
        destination_schema = self.schema.copy()
        try:
            table_obj = self.client.get_table(self.target_table_ref)
            self.target_table_ref = table_obj.reference
            # if destination table exists, merge new fields from the temp
            destination_schema = table_obj.schema
            tmp_schema = self.client.get_table(self.temp_table_ref).schema
            existing_schema_fields = [
                f.name.lower() for f in destination_schema
            ]
            tmp_schema_fields = [f.name.lower() for f in tmp_schema]
            for i in range(0, len(tmp_schema_fields)):
                field = tmp_schema_fields[i]
                if field not in existing_schema_fields and field not in [
                        "isdeleted", "isarchived"
                ]:
                    modified_schema = True
                    destination_schema.append(tmp_schema[i])

            if modified_schema:
                table_obj.schema = destination_schema
                table_obj = self.client.update_table(table_obj, ["schema"])
                destination_schema = table_obj.schema
        except NotFound:
            # Destination table doesn't exist
            # remove IsDeleted and IsArchived
            # if they present in the schema,
            # and create the destination table.
            for f in destination_schema.copy():
                if f.name.lower() in ["isdeleted", "isarchived"]:
                    destination_schema.remove(f)

            table_obj = self.client.create_table(
                bigquery.Table(self.target_table_ref, destination_schema),)
            self.target_table_ref = table_obj.reference

        # If have data to copy/merge, construct and run merging query
        try:
            if not finish_empty_job:
                # Starting a transaction
                query = "BEGIN TRANSACTION; "
                recordstamp_str = self.job_timestamp.strftime(
                    "%Y-%m-%dT%H:%M:%S.%fZ")

                if self.last_job_timestamp:
                    # Query for deleting rows with Id that are present
                    # in the temporary table.
                    delete_query = f"""
                        DELETE FROM `{self.project_id}.{self.dataset_name}.{self.target_table_name}`
                        WHERE {self.id_field_name} IN
                        (SELECT {self.id_field_name} FROM `{self.project_id}.{self.dataset_name}.{self.temp_table_name}`
                        );
                    """
                    query += delete_query

                # Query for copying data from the temp table
                # to the destination table

                # SELECT all fields except IsDeleted, IsArchived and Recordstamp
                select_fields = [
                    f.name
                    for f in destination_schema
                    if f.name.lower() not in [
                        "isdeleted", "isarchived",
                        self.timestamp_field_name.lower()
                    ]
                ]
                select_fields_str = ",".join(select_fields)
                # INSERT statement includes Recordstamp as a value.
                insert_field_str = (f"{select_fields_str},"
                                    f"{self.timestamp_field_name}")

                query += f"""
                    INSERT INTO `{self.project_id}.{self.dataset_name}.{self.target_table_name}`
                    ({insert_field_str})
                    SELECT {select_fields_str},
                    TIMESTAMP('{recordstamp_str}') AS {self.timestamp_field_name}
                    FROM `{self.project_id}.{self.dataset_name}.{self.temp_table_name}`
                """
                if self.has_is_deleted:
                    query += " WHERE NOT IsDeleted"
                if self.has_is_archived:
                    if self.has_is_deleted:
                        query += " AND NOT IsArchived"
                    else:
                        query += " WHERE NOT IsArchived"
                query += ";"

                # Committing the transaction.
                # If it fails before, BigQuery will roll it back automatically.
                query += " COMMIT TRANSACTION;"

                query_job = self.client.query(query=query,
                                              project=table_obj.project,
                                              location=table_obj.location)
                query_job.result()
                bytes_processes = query_job.total_bytes_processed
                slot_milliseconds = query_job.slot_millis

                logging.info(
                    "%s: bytes processed %f, slot seconds %f.",
                    self.target_table_ref,
                    bytes_processes,
                    slot_milliseconds / 1000,
                )

            # Delete temporary table.
            logging.info("Deleting temporary resources: %s",
                         self.temp_table_ref)
            self.client.delete_table(self.temp_table_ref)

            logging.info("Finished ingestion to %s", self.target_table_ref)

            self._ingestion_started = False
        except TimeoutError:
            logging.critical("⛔️ Operation failed with timeout error: %s\n",
                          exc_info=True)
            raise
        except GoogleCloudError:
            logging.critical("⛔️ Google Cloud Operation failed: %s\n",
                          exc_info=True)
            raise

    def _retrieve_last_job_timestamp(self):
        """Retrieves maximum value of record timestamp field
        from the destination table.
        """
        try:
            self.timestamp_field_name = self.timestamp_field_name
            table_obj = self.client.get_table(self.target_table_ref)
            self.target_table_ref = table_obj.reference

            query_job = self.client.query(
                f"SELECT MAX({self.timestamp_field_name})"
                f" FROM `{self.target_table_ref}`")

            # This query is guaranteed to return one column and one row.
            last_update_timestamp = list(query_job)[0][0]

            self.last_job_timestamp = last_update_timestamp

        except NotFound:
            logging.info(
                "Target table '%s' does not exist. It will be created.",
                self.target_table_ref,
            )
            self.last_job_timestamp = None
