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
"""BigQuery utility functions for LiveRamp data extraction."""

from typing import Any, Generator, List, Tuple
from uuid import UUID

from google.cloud.bigquery import Client


def get_segments_from_bq(client: Client, project: str,
                         dataset: str) -> List[Tuple[str, int]]:
    """Gets list of available segments."""

    segment_sql = f"""
      SELECT
        segment_name,
        COUNT(*) AS segment_size
      FROM `{project}.{dataset}.rampid_lookup_input`
      WHERE
        is_processed = FALSE
      GROUP BY
        segment_name;
    """

    data = client.query(segment_sql).result()
    segment_list = [(row[0], row[1]) for row in data]

    return segment_list


def create_temporary_tables(client: Client, project: str, dataset: str,
                            batch_id: UUID) -> None:
    """Creates temporary tables to store intermediate data.

    Args:
        client (Client): BigQuery client.
        project (str): Name of current project.
        dataset (str): Name of current dataset.
        batch_id (str): UUID generated for current batch.
    """

    query = f"""
      CREATE OR REPLACE TABLE `{project}.{dataset}.temp_request_input_{batch_id}`
      (
        id STRING,
        segment_name STRING,
        source_system_name STRING
      ) OPTIONS (
        description = 'Temporary table to store intermediate data from input table during processing.',
        expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
      );

      CREATE OR REPLACE TABLE `{project}.{dataset}.temp_request_output_{batch_id}`
      (
        segment_name STRING,
        ramp_id STRING,
      ) OPTIONS (
        description = 'Temporary table to store intermediate data from API response during processing.',
        expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
      );
    """

    client.query(query).result()


def get_batch_of_rows_of_current_segment(client: Client, project: str,
                                         dataset: str, batch_size: int,
                                         segment_name: str) -> Generator:
    """Fetches the current segment from BigQuery.

    Fetches data for current segment and yields the list of rows of the
    current page as a result.
    """

    # pylint:disable=W1401
    query = f"""
      WITH Input AS (
        SELECT DISTINCT
          id,
          segment_name,
          source_system_name,
          TRIM(REPLACE(name, '.', '')) AS name,
          TRIM(email) AS email,
          REGEXP_REPLACE(phone_number, r'[^\d]', '') AS phone_number,
          TRIM(postal_code) AS postal_code
        FROM `{project}.{dataset}.rampid_lookup_input`
        WHERE NOT is_processed
          AND segment_name = '{segment_name}'
        )
      SELECT
        id,
        segment_name,
        source_system_name,
        name,
        email,
        phone_number,
        postal_code,
        CONCAT(
          '/people/sha1/',
          TO_HEX(
            SHA1(
              LOWER(
                COALESCE(
                  CONCAT(name, ' ', email),
                  CONCAT(name, ' ', phone_number),
                  CONCAT(name, ' ', postal_code),
                  email,
                  phone_number
                )
              )
            )
          )
        ) AS search_string
      FROM Input;
    """

    result = client.query(query).result(page_size=batch_size)
    batches = result.pages
    for rows in batches:
        yield list(rows)


def insert_batch_into_request_input(client: Client, project: str, dataset: str,
                                    batch_id: UUID, batch: List[Any]) -> None:
    """Inserts rows from a given batch into a temporary processing table."""

    # Parsing data from batch to lists where each row is a separate list.
    parsed_values = []
    for row in batch:
        row_values = [row["id"], row["segment_name"], row["source_system_name"]]
        parsed_values.append(row_values)

    # Parsing list of Python list to list of values in brackets for each row.
    values_list = []
    for row in parsed_values:
        # Replace null values with "" to be able to append into list.
        row_values = [value if value is not None else "" for value in row]

        # Extracting lists to values in brackets to be able to use them
        # in INSERT INTO statement.
        # pylint:disable=C0209
        values = "('{}')".format("', '".join(row_values))
        values_list.append(values)

    # Extracting data from list.
    values_part = ", ".join(values_list)

    query = f"""
      INSERT INTO `{project}.{dataset}.temp_request_input_{batch_id}`
        (id, segment_name, source_system_name)
      VALUES {values_part}
    """

    client.query(query).result()


def insert_response_into_request_output(
        client: Client, project: str, dataset: str, batch_id: UUID,
        resolved_ids: List[Tuple[str, str]]) -> None:
    """Inserts data into the processed requests table.

    Inserts list of tuples with segment names and ramp_ids into a temporary
    table for processed requests.
    """

    # pylint:disable=C0209
    values = ", ".join("('{}', '{}')".format(*row) for row in resolved_ids)

    query = f"""
      INSERT INTO `{project}.{dataset}.temp_request_output_{batch_id}`
        (segment_name, ramp_id)
      VALUES {values}
    """

    client.query(query).result()


def update_input_and_output_tables(client: Client, project: str, dataset: str,
                                   batch_id: UUID) -> None:
    """Updates input and output tables in a transaction."""

    query = f"""
      BEGIN TRANSACTION;

      -- Fill up result table with new RampIDs.
      -- When RampID already exists only the recordstamp is updated.

      MERGE `{project}.{dataset}.rampid_lookup` AS rampid_lookup
      USING `{project}.{dataset}.temp_request_output_{batch_id}` AS request_output
        ON (
          rampid_lookup.segment_name = request_output.segment_name
          AND rampid_lookup.ramp_id = request_output.ramp_id
        )
      WHEN MATCHED THEN
        UPDATE SET
          rampid_lookup.recordstamp = current_timestamp
      WHEN NOT MATCHED THEN
        INSERT (segment_name, ramp_id, recordstamp)
        VALUES (request_output.segment_name, request_output.ramp_id,
                current_timestamp);

      -- Update is_processed flag to TRUE and processed_timestamp
      -- of the processed rows in the input table.

      UPDATE `{project}.{dataset}.rampid_lookup_input` AS rampid_lookup_input
      SET is_processed = TRUE,
        processed_timestamp = current_timestamp
      FROM `{project}.{dataset}.temp_request_input_{batch_id}` AS request_input
      WHERE
        rampid_lookup_input.id = request_input.id
        AND rampid_lookup_input.segment_name = request_input.segment_name
        AND rampid_lookup_input.source_system_name = request_input.source_system_name;

      COMMIT TRANSACTION;
    """

    # It is failing in the case of duplicates in primary keys
    # in the input table.
    # The exception is raised by BQ client. So DAG will be in failed state.
    client.query(query).result()


def delete_temporary_tables(client: Client, project: str, dataset: str,
                            batch_id: UUID) -> None:
    """Deletes temporary tables."""

    query = f"""
      DROP TABLE `{project}.{dataset}.temp_request_input_{batch_id}`;

      DROP TABLE `{project}.{dataset}.temp_request_output_{batch_id}`;
    """

    client.query(query).result()
