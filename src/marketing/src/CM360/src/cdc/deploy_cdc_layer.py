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
"""Generate all necessary assets for CDC layer of CM360."""

import argparse
import datetime
import logging
from pathlib import Path
import re
import shutil
import sys

from common.py_libs.bq_helper import table_exists
from common.py_libs.bq_materializer import add_cluster_to_table_def
from common.py_libs.bq_materializer import add_partition_to_table_def
from google.cloud.bigquery import Client
from google.cloud.bigquery import SchemaField

from src.cdc.constants import CDC_SQL_SCRIPTS_OUTPUT_DIR
from src.cdc.constants import CDC_SQL_TEMPLATE
from src.cdc.constants import DAG_TEMPLATE_DIR
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
from src.py_libs.utils import create_table_ref
from src.py_libs.utils import generate_dag_from_template
from src.py_libs.utils import repr_schema
from src.py_libs.utils import TableNotFoundError

DAG_TEMPLATE_PATH = Path(DAG_TEMPLATE_DIR, "raw_to_cdc_dag_py_template.py")


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

    now = datetime.datetime.utcnow()
    now_date = datetime.datetime(now.year, now.month, now.day)
    client = Client(project=CDC_PROJECT)

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
            logging.info("Creating table...")

            logging.info("Creating schema...")
            schema = create_bq_schema(table_mapping_path)
            # Append account_id in CDC layer.
            schema.append(SchemaField(name="account_id", field_type="INTEGER"))
            logging.debug("Table schema: %s\n", repr_schema(schema))

            table_ref = create_table_ref(schema=schema,
                                         project=CDC_PROJECT,
                                         dataset=CDC_DATASET,
                                         table_name=table_name)

            # Add partition and clustering details
            partition_details = cdc_table_settings.get("partition_details")
            if partition_details:
                table_ref = add_partition_to_table_def(table_ref, partition_details)

            cluster_details = cdc_table_settings.get("cluster_details")
            if cluster_details:
                table_ref = add_cluster_to_table_def(table_ref, cluster_details)

            client.create_table(table_ref)
            logging.info("Table is created successfully.")

        # DAG PY file generation
        logging.info("Generating DAG python file...")
        load_frequency = cdc_table_settings["load_frequency"]
        subs = {
            "project_id": CDC_PROJECT,
            "cdc_dataset": CDC_DATASET,
            "cdc_sql_path": Path("cdc_sql_scripts", f"{table_name}.sql"),
            "load_frequency": load_frequency,
            "table_name": table_name,
            "start_date": now_date,
        }
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

    logging.info("✅ CDC layer deployed successfully!")


if __name__ == "__main__":
    main()
