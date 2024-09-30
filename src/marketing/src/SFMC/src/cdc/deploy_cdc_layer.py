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
"""Deployment script for CDC layer.

Generates all necessary assets for the CDC layer of Salesforce Marketing Cloud.
"""

import argparse
from datetime import datetime
from datetime import timezone
import logging
from pathlib import Path
import sys

from common.py_libs import constants
from common.py_libs import cortex_bq_client
from common.py_libs.bq_helper import table_exists
from common.py_libs.bq_helper import create_table_from_schema
from common.py_libs.dag_generator import generate_file_from_template
from common.py_libs.schema_reader import read_bq_schema

from src.cdc.constants import CDC_SQL_SCRIPTS_OUTPUT_DIR
from src.cdc.constants import CDC_SQL_TEMPLATE_PATH
from src.cdc.constants import DAG_TEMPLATE_PATH
from src.cdc.constants import OUTPUT_DIR_FOR_CDC
from src.cdc.constants import SOURCE_TIMEZONE
from src.constants import CDC_DATASET
from src.constants import CDC_PROJECT
from src.constants import POPULATE_TEST_DATA
from src.constants import RAW_DATASET
from src.constants import RAW_PROJECT
from src.constants import SCHEMA_BQ_DATATYPE_FIELD
from src.constants import SCHEMA_DIR
from src.constants import SCHEMA_TARGET_FIELD
from src.constants import SETTINGS
from src.constants import SYSTEM_FIELDS
from src.py_libs.utils import generate_template_file
from src.py_libs.utils import populate_test_data
from src.py_libs.utils import repr_schema


def _parse_args(args):
    """Testable argument parsing."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug",
                        action="store_true",
                        help="Flag to set log level to DEBUG. Default is INFO.")

    args = parser.parse_args(args)
    return args.debug


def main(debug: bool):
    log_level = logging.DEBUG if debug else logging.INFO
    logging.getLogger().setLevel(log_level)
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

    # Creates directory for CDC files.
    OUTPUT_DIR_FOR_CDC.mkdir(exist_ok=True, parents=True)

    dag_start_date = datetime.now(timezone.utc).date()
    client = cortex_bq_client.CortexBQClient(project=CDC_PROJECT)

    if not "raw_to_cdc_tables" in SETTINGS:
        logging.warning(
            "❗ File '%s' is missing property `raw_to_cdc_tables`. "
            "Skipping CDC DAG generation.", SETTINGS)
        sys.exit()

    cdc_layer_settings = SETTINGS["raw_to_cdc_tables"]

    logging.info("Processing CDC tables...")

    for cdc_table_settings in cdc_layer_settings:
        logging.info("Checking settings...")

        missing_cdc_setting_attr = []
        required_attributes = ("load_frequency", "base_table",
                               "row_identifiers", "raw_table")

        for attr in required_attributes:
            if cdc_table_settings.get(attr) is None or cdc_table_settings.get(
                    attr) == "":
                missing_cdc_setting_attr.append(attr)

        if missing_cdc_setting_attr:
            raise ValueError(
                "Setting file is missing or has empty value for one or more"
                f" attributes: {missing_cdc_setting_attr} ")

        table_name = cdc_table_settings["base_table"]
        load_frequency = cdc_table_settings["load_frequency"]
        schema_file_name = f"{table_name}.csv"
        partition_details = cdc_table_settings.get("partition_details")
        cluster_details = cdc_table_settings.get("cluster_details")
        full_table_name = f"{CDC_PROJECT}.{CDC_DATASET}.{table_name}"
        table_mapping_path = Path(SCHEMA_DIR, schema_file_name)

        logging.info("Processing table %s", table_name)

        if table_exists(bq_client=client, full_table_name=full_table_name):
            logging.warning("❗ Table already exists.")
        else:
            logging.info("Creating schema...")

            schema = read_bq_schema(
                mapping_file=table_mapping_path,
                schema_target_field=SCHEMA_TARGET_FIELD,
                schema_bq_datatype_field=SCHEMA_BQ_DATATYPE_FIELD,
                system_fields=SYSTEM_FIELDS)

            logging.debug("CDC Table schema: %s\n", repr_schema(schema))
            logging.info("Creating table...")

            create_table_from_schema(bq_client=client,
                         full_table_name=full_table_name,
                         schema=schema,
                         partition_details=partition_details,
                         cluster_details=cluster_details)

            logging.info("Table is created successfully.")

        logging.info("Generating DAG Python file...")

        subs = {
            "cdc_project_id": CDC_PROJECT,
            "cdc_dataset": CDC_DATASET,
            "load_frequency": load_frequency,
            "table_name": table_name,
            "start_date": dag_start_date,
            "cdc_sql_path": Path("cdc_sql_scripts", f"{table_name}.sql"),
            "runtime_labels_dict": "" # A place holder for label dict string
        }

        # If telemetry opted in, convert CORTEX JOB LABEL dict to string
        # And assign to substitution
        if client.allow_telemetry:
            subs["runtime_labels_dict"] = str(constants.CORTEX_JOB_LABEL)

        dag_py_file = (f"{CDC_PROJECT}_{CDC_DATASET}"
                       f"_raw_to_cdc_{table_name}.py")
        dag_py_path = Path(OUTPUT_DIR_FOR_CDC, dag_py_file)
        generate_file_from_template(DAG_TEMPLATE_PATH, dag_py_path, **subs)

        logging.info("Generated DAG Python file.")
        logging.info("Generating DAG SQL file...")

        # DAG SQL file generation
        row_identifiers = cdc_table_settings["row_identifiers"]
        raw_table = cdc_table_settings.get("raw_table")
        table_mapping_path = Path(SCHEMA_DIR, schema_file_name)
        generated_sql_path = Path(CDC_SQL_SCRIPTS_OUTPUT_DIR,
                                  f"{table_name}.sql")

        template_vals = {
            "source_project_id": RAW_PROJECT,
            "target_project_id": CDC_PROJECT,
            "row_identifiers": row_identifiers,
            "source_ds": RAW_DATASET,
            "target_ds": CDC_DATASET,
            "target_table": table_name,
            "source_table": raw_table,
            "source_timezone": SOURCE_TIMEZONE
        }

        generate_template_file(CDC_SQL_TEMPLATE_PATH, table_mapping_path,
                               generated_sql_path, template_vals)

        logging.info("Generated DAG SQL file: %s", generated_sql_path)

        # Populate table with test data using generated SQL script.
        if POPULATE_TEST_DATA:
            logging.info("Populating table with test data...")
            populate_test_data(client=client,
                               path_to_script=generated_sql_path,
                               full_table_name=full_table_name)

            logging.info("Test data populated!")

        logging.info("Table processed successfully.")
        logging.info("----------------------------")

    logging.info("Processed all tables successfully.")
    logging.info("✅ CDC layer deployed successfully!")


if __name__ == "__main__":
    parsed_args = _parse_args(sys.argv[1:])
    main(parsed_args)
