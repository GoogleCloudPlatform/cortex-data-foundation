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
""" Contains methods for working with BQ Views and mapping files."""

import logging
from pathlib import Path

from common.py_libs.bq_helper import table_exists
from google.cloud.bigquery import Client

from src.py_libs.schema_generator import generate_schema
from src.py_libs.schema_generator import generate_column_list
from src.py_libs.sql_generator import render_template_file
from src.raw.constants import SQL_TEMPLATE_FILE


def execute_sql_file(client: Client, sql_code: str):
    """Executes a Bigquery sql code."""
    query_job = client.query(sql_code)
    # Let's wait for query to complete.
    _ = query_job.result()


def create_view(client: Client, table_mapping_path: Path, table_name: str,
                raw_project: str, raw_dataset: str, key_fields: list[str]):
    """For a given table config, creates required view SQL,
    and if the raw table exists, executes the SQL.
    """
    logging.info("Creating view for table '%s'...", table_name)

    source_table = raw_project + "." + raw_dataset + "." + table_name
    target_view = raw_project + "." + raw_dataset + "." + table_name + "_view"

    # Check if raw table exists.
    raw_exists = table_exists(bq_client=client, full_table_name=source_table)

    if not raw_exists:
        logging.error(("Source raw table `%s` doesn't exist! \n"
                       "RAW view cannot be created."), table_name)
        raise SystemExit("⛔️ Failed to deploy RAW views.")

    # Generate dictionary schema from mapping file.
    schema = generate_schema(table_mapping_path, "view")

    # Generate a list of SQL statements for fields extraction from raw table.
    columns = generate_column_list(schema)

    template_vals = {
        "project_id_src": raw_project,
        "dataset_raw_landing_marketing_googleads": raw_dataset,
        "raw_view": table_name + "_view",
        "columns": columns,
        "row_identifiers": key_fields,
        "raw_table": table_name
    }

    # SQL file generation.
    sql_text = render_template_file(template_path=SQL_TEMPLATE_FILE,
                                    subs=template_vals)
    logging.info("Generated view SQL file.")

    try:
        logging.info("Creating view %s..", target_view)
        execute_sql_file(client=client, sql_code=sql_text)
        logging.info("✅ Created view %s.", target_view)

    except Exception as e:
        logging.error("Failed to create view '%s'.\n"
                      "ERROR: %s", target_view, str(e))
        raise SystemExit(
            "⛔️ Failed to deploy RAW view. Please check the logs.") from e
