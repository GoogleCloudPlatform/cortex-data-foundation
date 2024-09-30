# Copyright 2024 Google LLC

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Generate all necessary assets for CDC layer of CM360."""

import argparse
from datetime import datetime
from datetime import timezone
import logging
from pathlib import Path
import re
import shutil
import sys

from common.py_libs import constants
from common.py_libs import cortex_bq_client
from common.py_libs.bq_helper import create_table_from_schema
from common.py_libs.bq_helper import table_exists
from google.cloud.bigquery import Client
from google.cloud.bigquery import SchemaField

from src.cdc.constants import CDC_SQL_SCRIPTS_OUTPUT_DIR
from src.cdc.constants import CDC_SQL_TEMPLATE
from src.cdc.constants import DAG_CONFIG_INI_INPUT_PATH
from src.cdc.constants import DAG_CONFIG_INI_OUTPUT_PATH
from src.cdc.constants import DAG_TEMPLATE_PATH
from src.cdc.constants import OUTPUT_DIR_FOR_CDC
from src.constants import CDC_DATASET
from src.constants import CDC_PROJECT
from src.constants import POPULATE_TEST_DATA
from src.constants import RAW_DATASET
from src.constants import RAW_PROJECT
from src.constants import SCHEMA_DIR
from src.constants import SETTINGS
from src.py_libs.sql_generator import render_template_file
from src.py_libs.sql_generator import write_generated_sql_to_disk
from src.py_libs.utils import create_bq_schema
from src.py_libs.utils import generate_dag_from_template
from src.py_libs.utils import repr_schema
from src.py_libs.utils import TableNotFoundError


def _create_sql_output_dir_structure() -> None:
    """Creates directory for SQL scripts. Cleans up before!"""
    if CDC_SQL_SCRIPTS_OUTPUT_DIR.exists():
        shutil.rmtree(CDC_SQL_SCRIPTS_OUTPUT_DIR)

    CDC_SQL_SCRIPTS_OUTPUT_DIR.mkdir(exist_ok=False, parents=True)


def _get_sql_template(table_name: str) -> str:
    """Decides which template should be used based on table name."""

    if re.match(r"match_table_\S+", table_name):
        return "cm360_raw_to_cdc_template_match_table_files.sql"
    # Report files.
    return "cm360_raw_to_cdc_template_report_files.sql"


def _populate_test_data(client: Client, path_to_script: Path,
                        full_table_name) -> None:
    """Loads test data from RAW to CDC.

    Also checks CDC test table is empty to avoid existing data loss."""

    if not table_exists(bq_client=client, full_table_name=full_table_name):
        raise TableNotFoundError(f"Test table {full_table_name} is not found!")

    with open(path_to_script, mode="r", encoding="utf-8") as query_file:
        sql = query_file.read()
        populate_cdc_table_job = client.query(query=sql)
        populate_cdc_table_job.result()


def _create_output_dir_structure() -> None:
    """Creates directory for CDC files. Cleans up before!"""
    if OUTPUT_DIR_FOR_CDC.exists():
        shutil.rmtree(OUTPUT_DIR_FOR_CDC)

    OUTPUT_DIR_FOR_CDC.mkdir(exist_ok=False, parents=True)


def _parse_args(args):
    """Testable argument parsing."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug",
                        action="store_true",
                        help="Flag to set log level to DEBUG. Default is INFO.")

    args = parser.parse_args(args)
    return args


def main():
    parsed_args = _parse_args(sys.argv[1:])
    logging.basicConfig(
        level=logging.DEBUG if parsed_args.debug else logging.INFO)

    logging.info("Deploying CDC layer...")
    logging.info(
        "\n---------------------------------------\n"
        "Using the following parameters from config:\n"
        "  RAW_PROJECT = %s \n"
        "  RAW_DATASET = %s \n"
        "  CDC_PROJECT = %s \n"
        "  CDC_DATASET = %s \n"
        "---------------------------------------\n", RAW_PROJECT, RAW_DATASET,
        CDC_PROJECT, CDC_DATASET)

    logging.info("Creating required directories for generated files...")
    _create_output_dir_structure()
    _create_sql_output_dir_structure()

    dag_start_date = datetime.now(timezone.utc).date()
    client = cortex_bq_client.CortexBQClient(project=CDC_PROJECT)

    cdc_layer_settings = SETTINGS["raw_to_cdc_tables"]

    logging.info("Processing CDC tables...")
    for cdc_table_settings in cdc_layer_settings:
        table_name = cdc_table_settings["base_table"]

        logging.info("-- Processing table '%s' --", table_name)

        table_mapping_path = Path(SCHEMA_DIR, f"{table_name}.csv")
        full_table_name = f"{CDC_PROJECT}.{CDC_DATASET}.{table_name}"
        if table_exists(bq_client=client, full_table_name=full_table_name):
            logging.warning("❗ Table already exists.")
        else:
            logging.info("Creating table %s...", full_table_name)

            logging.info("Creating schema...")
            schema = create_bq_schema(table_mapping_path)
            # Append account_id in CDC layer.
            schema.append(SchemaField(name="account_id", field_type="INTEGER"))
            logging.debug("Table schema: %s\n", repr_schema(schema))

            partition_details = cdc_table_settings.get("partition_details")
            cluster_details = cdc_table_settings.get("cluster_details")

            create_table_from_schema(bq_client=client,
                         full_table_name=full_table_name,
                         schema=schema,
                         partition_details=partition_details,
                         cluster_details=cluster_details)

            logging.info("Table %s processed successfully.", full_table_name)


        # DAG PY file generation
        logging.info("Generating DAG python file...")
        load_frequency = cdc_table_settings["load_frequency"]
        subs = {
            "project_id": CDC_PROJECT,
            "cdc_dataset": CDC_DATASET,
            "cdc_sql_path": Path("cdc_sql_scripts", f"{table_name}.sql"),
            "load_frequency": load_frequency,
            "table_name": table_name,
            "start_date": dag_start_date,
            "runtime_labels_dict": "", # A place holder for labels dict string
        }

        # If telemetry opted in, convert CORTEX JOB LABEL dict to string
        # And assign to substitution
        if client.allow_telemetry:
            subs["runtime_labels_dict"] = str(constants.CORTEX_JOB_LABEL)

        generate_dag_from_template(
            template_file=DAG_TEMPLATE_PATH,
            generation_target_directory=OUTPUT_DIR_FOR_CDC,
            table_name=table_name,
            project_id=CDC_PROJECT,
            dataset_id=CDC_DATASET,
            layer="raw_to_cdc",
            subs=subs)
        logging.info("Generated DAG python file.")

        # DAG SQL file generation
        logging.info("Generating DAG SQL file...")
        row_identifiers = cdc_table_settings["row_identifiers"]
        sql_template_file = _get_sql_template(table_name=table_name)
        template_vals = {
            "source_project_id": RAW_PROJECT,
            "target_project_id": CDC_PROJECT,
            "row_identifiers": row_identifiers,
            "source_ds": RAW_DATASET,
            "target_ds": CDC_DATASET,
            "table": table_name,
        }
        template_file_path = Path(CDC_SQL_TEMPLATE, sql_template_file)
        sql_code = render_template_file(template_path=template_file_path,
                                        mapping_path=table_mapping_path,
                                        subs=template_vals)
        generated_sql_path = Path(CDC_SQL_SCRIPTS_OUTPUT_DIR,
                                  f"{table_name}.sql")
        write_generated_sql_to_disk(path=generated_sql_path,
                                    generated_sql=sql_code)

        logging.info("Generated DAG SQL file: %s", generated_sql_path)

        # Populate table with test data using generated SQL script.
        if POPULATE_TEST_DATA:
            logging.info("Populating table with test data...")
            _populate_test_data(client=client,
                                path_to_script=generated_sql_path,
                                full_table_name=full_table_name)
            logging.info("Test data populated!")

        logging.info("Table processed successfully.")
        logging.info("----------------------------")

    logging.info("Processed all tables successfully.")

    logging.info("Copying DAG config file...")

    shutil.copyfile(src=DAG_CONFIG_INI_INPUT_PATH,
                    dst=DAG_CONFIG_INI_OUTPUT_PATH)
    logging.info("DAG config file copied successfully.")

    logging.info("✅ CDC layer deployed successfully!")


if __name__ == "__main__":
    main()
