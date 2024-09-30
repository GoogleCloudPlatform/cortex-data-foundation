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
"""Generate all necessary assets for CDC layer of GoogleAds."""

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

from google.cloud.bigquery import Client
from google.cloud.exceptions import NotFound
from src.cdc.constants import CDC_SQL_SCRIPTS_OUTPUT_DIR
from src.cdc.constants import CDC_SQL_TEMPLATE_PATH
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
from src.py_libs.schema_generator import convert_to_bq_schema
from src.py_libs.schema_generator import generate_schema
from src.py_libs.sql_generator import render_template_file
from src.py_libs.sql_generator import write_generated_sql_to_disk
from src.py_libs.table_creation_utils import repr_schema
from src.py_libs.table_creation_utils import TableNotFoundError

def _generate_dag_from_template(template_file: Path,
                                generation_target_directory: Path,
                                project_id: str, dataset_id: str,
                                table_name: str, subs: dict):
    """Generates DAG code from template file.
    Each DAG is responsible for loading one table.

    Args:
        template_file (Path): Path of the template file.
        generation_target_directory (Path): Directory where files are generated.
        project_id (str): Id of GCP project.
        dataset_id (str): Id of BigQuery dataset.
        table_name (str): The table name which is loaded by this dag.
        subs (dict): DAG template substitutions.
    """
    output_dag_py_file = Path(
        generation_target_directory,
        (f"{project_id}_{dataset_id}"
         f"_raw_to_cdc_{table_name.replace('.', '_')}.py"))
    generate_file_from_template(template_file, output_dag_py_file, **subs)


def _create_sql_output_dir_structure() -> None:
    """Creates directory for SQL scripts. Cleans up before!"""
    if CDC_SQL_SCRIPTS_OUTPUT_DIR.exists():
        shutil.rmtree(CDC_SQL_SCRIPTS_OUTPUT_DIR)
    CDC_SQL_SCRIPTS_OUTPUT_DIR.mkdir(exist_ok=False, parents=True)


def _populate_test_data(client: Client, path_to_script: Path, project: str,
                        dataset: str, table_name: str) -> None:
    """Loads test data from RAW to CDC."""
    full_table_name = f"{project}.{dataset}.{table_name}"

    try:
        client.get_table(full_table_name)
    except NotFound as e:
        raise TableNotFoundError(
            f"Test table {full_table_name} is not found!") from e

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

    level = logging.DEBUG if parsed_args.debug else logging.INFO
    logging.getLogger().setLevel(level)

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
    _create_sql_output_dir_structure()

    dag_start_date = datetime.now(timezone.utc).date()
    client = cortex_bq_client.CortexBQClient(project=CDC_PROJECT)

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
        for attr in ("table_name", "raw_table", "key", "schema_file",
                     "load_frequency"):
            if cdc_table_settings.get(attr) is None or cdc_table_settings.get(
                    attr) == "":
                missing_cdc_setting_attr.append(attr)
        if missing_cdc_setting_attr:
            raise ValueError(
                "Setting file is missing or has empty value for one or more "
                f"attributes: {missing_cdc_setting_attr} ")

        table_name = cdc_table_settings.get("table_name")
        raw_table = cdc_table_settings.get("raw_table")
        key = cdc_table_settings.get("key")
        schema_file = cdc_table_settings.get("schema_file")
        load_frequency = cdc_table_settings.get("load_frequency")
        partition_details = cdc_table_settings.get("partition_details")
        cluster_details = cdc_table_settings.get("cluster_details")

        table_mapping_path = Path(SCHEMA_DIR, schema_file)

        logging.info("-- Processing table '%s' --", table_name)

        logging.info("Creating schema...")
        # Generating dictionary schema that will contain
        # a hierarchy of nested columns.
        dict_schema = generate_schema(table_mapping_path, "table")
        # Converting dictionary schema to BQ schema.
        bq_schema = convert_to_bq_schema(dict_schema)
        logging.debug("CDC Table schema: %s\n", repr_schema(bq_schema))

        logging.info("Creating CDC table...")
        full_table_name = CDC_PROJECT + "." + CDC_DATASET + "." + table_name
        # Check if CDC table exists.
        cdc_table_exists = table_exists(client, full_table_name)
        if not cdc_table_exists:
            create_table_from_schema(bq_client=client,
                                     full_table_name=full_table_name,
                                     schema=bq_schema,
                                     partition_details=partition_details,
                                     cluster_details=cluster_details)
            logging.info("Table is created successfully.")
        else:
            logging.warning("❗ Table already exists. Not creating table.")

        # DAG PY file generation
        logging.info("Generating DAG python file...")
        cdc_sql_name = f"{table_name}.sql"
        subs = {
            "cdc_sql_path": Path("cdc_sql_scripts", cdc_sql_name),
            "load_frequency": load_frequency,
            "table_name": table_name,
            "cdc_dataset": CDC_DATASET,
            "project_id": CDC_PROJECT,
            "start_date": dag_start_date,
            "runtime_labels_dict": "" # A place holder for label dict string
        }

        # If telemetry opted in, convert CORTEX JOB LABEL dict to string
        # And assign to substitution
        if client.allow_telemetry:
            subs["runtime_labels_dict"] = str(constants.CORTEX_JOB_LABEL)

        _generate_dag_from_template(
            template_file=DAG_TEMPLATE_PATH,
            generation_target_directory=OUTPUT_DIR_FOR_CDC,
            project_id=CDC_PROJECT,
            dataset_id=CDC_DATASET,
            table_name=table_name,
            subs=subs)

        logging.info("Generated DAG python file.")

        # DAG SQL file generation
        logging.info("Generating DAG SQL file...")
        key_list = key.split(",")
        key_clause = " AND ".join([f"T.{key}=S.{key}" for key in key_list])
        insert_clause = ", ".join([f"{field.name}" for field in bq_schema])
        set_clause = ", ".join(
            [f"T.{field.name}=S.{field.name}" for field in bq_schema])
        template_vals = {
            "source_project_id": RAW_PROJECT,
            "source_ds": RAW_DATASET,
            "table": table_name,
            "temp_table": table_name + "_temp",
            "target_project_id": CDC_PROJECT,
            "target_ds": CDC_DATASET,
            "view": raw_table + "_view",
            "target_table": table_name,
            "row_identifiers": key_clause,
            "columns": insert_clause,
            "columns_identifiers": set_clause
        }
        # pylint: disable=E1120
        sql_code = render_template_file(template_path=CDC_SQL_TEMPLATE_PATH,
                                        subs=template_vals)
        generated_sql_path = Path(CDC_SQL_SCRIPTS_OUTPUT_DIR, cdc_sql_name)
        write_generated_sql_to_disk(path=generated_sql_path,
                                    generated_sql=sql_code)
        logging.info("Generated DAG SQL file: %s", generated_sql_path)

        # Populate table with test data using generated SQL script.
        if POPULATE_TEST_DATA:
            logging.info("Populating table with test data...")
            _populate_test_data(client=client,
                                path_to_script=generated_sql_path,
                                project=CDC_PROJECT,
                                dataset=CDC_DATASET,
                                table_name=table_name)
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
