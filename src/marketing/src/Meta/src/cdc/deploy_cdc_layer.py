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
"""Generates all necessary assets for CDC layer."""

import argparse
from datetime import datetime
from datetime import timezone
import logging
from pathlib import Path
import shutil
import sys

from common.py_libs import constants
from common.py_libs import cortex_bq_client
from common.py_libs.bq_helper import create_table_from_schema
from common.py_libs.bq_helper import table_exists
from common.py_libs.dag_generator import generate_file_from_template
from common.py_libs.jinja import apply_jinja_params_dict_to_file
from common.py_libs.schema_reader import read_bq_schema
from common.py_libs.schema_reader import read_field_type_mapping

from src.cdc.constants import CDC_OUTPUT_DIR
from src.cdc.constants import CDC_SQL_OUTPUT_DIR
from src.cdc.constants import CDC_SQL_TEMPLATE_PATH
from src.cdc.constants import DAG_CONFIG_INI_INPUT_PATH
from src.cdc.constants import DAG_CONFIG_INI_OUTPUT_PATH
from src.cdc.constants import DAG_TEMPLATE_PATH
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

def _create_output_dir_structure() -> None:
    """Creates directory for CDC files."""
    CDC_OUTPUT_DIR.mkdir(exist_ok=True, parents=True)
    CDC_SQL_OUTPUT_DIR.mkdir(exist_ok=True, parents=True)


def _parse_debug(args):
    """Testable argument parsing."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug",
                        action="store_true",
                        help="Flag to set log level to DEBUG. Default is INFO.")

    args = parser.parse_args(args)
    return args.debug


def main():
    """Main function placeholder."""

    debug = _parse_debug(sys.argv[1:])
    logging.basicConfig(level=logging.DEBUG if debug else logging.INFO)

    logging.info("Deploying CDC layer...")
    logging.info(
        "\n---------------------------------------\n"
        "Using the following parameters from config:\n"
        "  RAW_PROJECT = %s\n"
        "  RAW_DATASET = %s\n"
        "  CDC_PROJECT = %s\n"
        "  CDC_DATASET = %s\n"
        "---------------------------------------\n", RAW_PROJECT, RAW_DATASET,
        CDC_PROJECT, CDC_DATASET)

    logging.info("Creating required directories for generated files...")

    _create_output_dir_structure()

    dag_start_date = datetime.now(timezone.utc).date()

    bq_client = cortex_bq_client.CortexBQClient(project=CDC_PROJECT)

    if not "raw_to_cdc_tables" in SETTINGS:
        logging.warning(
            "❗ File '%s' is missing property `raw_to_cdc_tables`. "
            "Skipping CDC DAG generation.", SETTINGS)
        sys.exit()

    cdc_layer_settings = SETTINGS.get("raw_to_cdc_tables")

    logging.info("Processing CDC tables...")
    for cdc_table_settings in cdc_layer_settings:
        # Making sure all required setting attributes are provided.
        missing_cdc_setting_attr = []
        for attr in ("base_table", "row_identifiers", "load_frequency"):
            if not cdc_table_settings.get(attr):
                missing_cdc_setting_attr.append(attr)
        if missing_cdc_setting_attr:
            raise ValueError(
                "Setting file is missing or has empty value for one or more "
                f"required attributes: {missing_cdc_setting_attr} ")

        table_name = cdc_table_settings.get("base_table")
        load_frequency = cdc_table_settings.get("load_frequency")
        row_identifiers = cdc_table_settings.get("row_identifiers")
        partition_details = cdc_table_settings.get("partition_details")
        cluster_details = cdc_table_settings.get("cluster_details")

        logging.info("Processing table %s", table_name)

        table_mapping_path = Path(SCHEMA_DIR, f"{table_name}.csv")
        full_table_name = f"{CDC_PROJECT}.{CDC_DATASET}.{table_name}"

        if table_exists(bq_client=bq_client, full_table_name=full_table_name):
            logging.warning("❗ Table already exists.")
        else:
            logging.info("Creating table %s...", full_table_name)

            schema = read_bq_schema(
                mapping_file=table_mapping_path,
                schema_target_field=SCHEMA_TARGET_FIELD,
                schema_bq_datatype_field=SCHEMA_BQ_DATATYPE_FIELD,
                system_fields=SYSTEM_FIELDS)

            create_table_from_schema(bq_client=bq_client,
                         full_table_name=full_table_name,
                         schema=schema,
                         partition_details=partition_details,
                         cluster_details=cluster_details)

            logging.info("Table %s processed successfully.", full_table_name)

        logging.info("Generating DAG Python file...")

        # DAG Python file generation.

        subs = {
            "sql_path": Path("cdc_sql_scripts", f"{table_name}.sql"),
            "project_id": CDC_PROJECT,
            "dataset": CDC_DATASET,
            "load_frequency": load_frequency,
            "table_name": table_name,
            "start_date": dag_start_date,
            "runtime_labels_dict": "" # A place holder for labels dict string
        }

        # If telemetry opted in, convert CORTEX JOB LABEL dict to string
        # And assign to substitution
        if bq_client.allow_telemetry:
            subs["runtime_labels_dict"] = str(constants.CORTEX_JOB_LABEL)

        table_name_as_identifier = table_name.replace(".", "_")
        dag_py_file = (f"{CDC_PROJECT}_{CDC_DATASET}"
                       f"_raw_to_cdc_{table_name_as_identifier}.py")
        dag_py_path = Path(CDC_OUTPUT_DIR, dag_py_file)
        generate_file_from_template(DAG_TEMPLATE_PATH, dag_py_path, **subs)

        logging.info("Generated DAG Python file.")

        # DAG SQL file generation.
        logging.info("Generating DAG SQL file...")
        template_vals = {
            "source_project_id": RAW_PROJECT,
            "target_project_id": CDC_PROJECT,
            "row_identifiers": row_identifiers,
            "source_ds": RAW_DATASET,
            "target_ds": CDC_DATASET,
            "target_table": table_name,
            "source_table": table_name,
        }

        field_type_mapping = read_field_type_mapping(
                                    mapping_file = table_mapping_path,
                                    schema_target_field = SCHEMA_TARGET_FIELD,
                                    schema_bq_datatype_field =\
                                          SCHEMA_BQ_DATATYPE_FIELD,
                                    system_fields=SYSTEM_FIELDS)

        jinja_dict = template_vals | {
            "meta_field_type_mapping": field_type_mapping
        }

        sql_code = apply_jinja_params_dict_to_file(CDC_SQL_TEMPLATE_PATH,
                                                   jinja_dict)

        generated_sql_path = Path(CDC_SQL_OUTPUT_DIR, f"{table_name}.sql")

        # Writes generated SQL object to the given path.
        with open(generated_sql_path, "w", encoding="utf-8") as f:
            f.write(sql_code)

        logging.info("Generated DAG SQL file: %s", generated_sql_path)

        # Populates table with test data using generated SQL script.
        if POPULATE_TEST_DATA:
            logging.info("Populating table with test data...")
            populate_cdc_table_job = bq_client.query(query=sql_code)
            populate_cdc_table_job.result()
            logging.info("Test data populated.")

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
