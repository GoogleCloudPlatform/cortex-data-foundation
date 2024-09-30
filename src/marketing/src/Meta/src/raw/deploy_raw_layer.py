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
"""
Generates DAG and related files needed to transfer data from Meta API
to BigQuery RAW dataset.
"""

import argparse
from datetime import datetime
from datetime import timezone
import logging
from pathlib import Path
import shutil
import sys
from typing import Any, Dict

from common.py_libs import cortex_bq_client
from common.py_libs.bq_helper import create_table_from_schema
from common.py_libs.bq_helper import table_exists
from common.py_libs.dag_generator import generate_file_from_template
from common.py_libs.schema_reader import read_bq_schema
from src.constants import PROJECT_REGION
from src.constants import RAW_DATASET
from src.constants import RAW_PROJECT
from src.constants import SCHEMA_DIR
from src.constants import SCHEMA_TARGET_FIELD
from src.constants import SETTINGS
from src.constants import SETTINGS_FILE
from src.constants import SYSTEM_FIELDS
from src.raw.constants import DAG_TEMPLATE_FILE
from src.raw.constants import DEPENDENCIES_INPUT_DIR
from src.raw.constants import DEPENDENCIES_OUTPUT_DIR
from src.raw.constants import OUTPUT_DIR_FOR_RAW
from src.raw.constants import REQUESTS_DIR
from src.raw.constants import REQUESTS_OUTPUT_DIR
from src.raw.constants import SCHEMAS_OUTPUT_DIR

def _parse_args(args) -> Dict[str, Any]:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pipeline-temp-bucket", type=str, required=True)
    parser.add_argument("--pipeline-staging-bucket", type=str, required=True)
    parser.add_argument("--debug",
                        action="store_true",
                        help="Flag to set log level to DEBUG. Default is INFO.")
    args = parser.parse_args(args)
    return vars(args)


def _create_output_dir_structure() -> None:
    OUTPUT_DIR_FOR_RAW.mkdir(exist_ok=True, parents=True)
    DEPENDENCIES_OUTPUT_DIR.mkdir(exist_ok=True, parents=True)
    SCHEMAS_OUTPUT_DIR.mkdir(exist_ok=True, parents=True)
    REQUESTS_OUTPUT_DIR.mkdir(exist_ok=True, parents=True)


def main():
    args = _parse_args(sys.argv[1:])
    logging.basicConfig(level=logging.DEBUG if args["debug"] else logging.INFO)

    logging.info("Deploying RAW layer...")
    logging.info(
        "\n---------------------------------------\n"
        "Using the following parameters from config:\n"
        "  RAW_PROJECT = %s \n"
        "  RAW_DATASET = %s \n"
        "---------------------------------------\n", RAW_PROJECT, RAW_DATASET)

    if not "source_to_raw_tables" in SETTINGS:
        logging.warning(
            "File '%s' is missing property `source_to_raw_tables`. "
            "Skipping RAW DAG generation.", SETTINGS_FILE.name)
        sys.exit()

    logging.info("Processing tables...")

    _create_output_dir_structure()

    logging.info("Copying schema files...")
    shutil.copytree(src=SCHEMA_DIR, dst=SCHEMAS_OUTPUT_DIR, dirs_exist_ok=True)
    logging.info("Copying request fields files...")
    shutil.copytree(src=REQUESTS_DIR,
                    dst=REQUESTS_OUTPUT_DIR,
                    dirs_exist_ok=True)

    bq_client = cortex_bq_client.CortexBQClient(project=RAW_PROJECT)

    raw_layer_settings = SETTINGS.get("source_to_raw_tables")
    for raw_table_settings in raw_layer_settings:

        logging.info("Checking settings...")

        missing_raw_setting_attr = []
        for attr in ("load_frequency", "base_table", "entity_type",
                     "object_endpoint"):
            if raw_table_settings.get(attr) is None or raw_table_settings.get(
                    attr) == "":
                missing_raw_setting_attr.append(attr)
        if missing_raw_setting_attr:
            raise ValueError(
                "Setting file is missing or has empty value for one or more "
                f"attributes: {missing_raw_setting_attr} ")

        all_entity_types = ["adaccount", "dimension", "fact"]
        entity_type = raw_table_settings.get("entity_type")
        if entity_type not in all_entity_types:
            raise ValueError(f"{entity_type} is not valid entity type. "
                             f"Possible values: {all_entity_types}.")

        load_frequency = raw_table_settings.get("load_frequency")
        table_name = raw_table_settings.get("base_table")
        object_endpoint = raw_table_settings.get("object_endpoint")
        object_id_column = raw_table_settings.get("object_id_column")
        breakdowns = raw_table_settings.get("breakdowns")
        action_breakdowns = raw_table_settings.get("action_breakdowns")
        partition_details = raw_table_settings.get("partition_details")
        cluster_details = raw_table_settings.get("cluster_details")

        logging.info("Processing table %s", table_name)

        table_mapping_path = Path(SCHEMA_DIR, f"{table_name}.csv")
        full_table_name = f"{RAW_PROJECT}.{RAW_DATASET}.{table_name}"

        if table_exists(bq_client=bq_client, full_table_name=full_table_name):
            logging.warning("❗ Table already exists.")
        else:
            logging.info("Creating table %s...", table_name)

            schema = read_bq_schema(mapping_file=table_mapping_path,
                                    schema_target_field=SCHEMA_TARGET_FIELD,
                                    system_fields=SYSTEM_FIELDS,
                                    schema_bq_datatype_field=None)

            create_table_from_schema(bq_client=bq_client,
                         full_table_name=full_table_name,
                         schema=schema,
                         partition_details=partition_details,
                         cluster_details=cluster_details)

            logging.info("Table %s processed successfully.", table_name)

        logging.info("Generating DAG Python file for %s", table_name)

        dag_start_date = datetime.now(timezone.utc).date()
        pipeline_setup_file = Path(DEPENDENCIES_OUTPUT_DIR.stem, "setup.py")

        subs = {
            "project_id": RAW_PROJECT,
            "dataset": RAW_DATASET,
            "table_name": table_name,
            "entity_type": entity_type,
            "load_frequency": load_frequency,
            "object_endpoint": object_endpoint,
            "object_id_column": object_id_column or "",
            "breakdowns": breakdowns or "",
            "action_breakdowns": action_breakdowns or "",
            "start_date": dag_start_date,
            "schemas_dir": SCHEMAS_OUTPUT_DIR.stem,
            "requests_dir": REQUESTS_OUTPUT_DIR.stem,
            "pipeline_staging_bucket": args["pipeline_staging_bucket"],
            "pipeline_temp_bucket": args["pipeline_temp_bucket"],
            "pipeline_setup": str(pipeline_setup_file),
            "project_region": PROJECT_REGION,
        }

        output_dag_py_filename = (
            f"{RAW_PROJECT}_{RAW_DATASET}"
            f"_extract_to_raw_{table_name.replace('.', '_')}.py")
        output_dag_py_path = Path(OUTPUT_DIR_FOR_RAW, output_dag_py_filename)
        generate_file_from_template(DAG_TEMPLATE_FILE, output_dag_py_path,
                                    **subs)

        logging.info("Generated DAG Python file.")

    logging.info("All tables processed successfully.")
    logging.info("Copying dependencies...")

    shutil.copytree(src=DEPENDENCIES_INPUT_DIR,
                    dst=DEPENDENCIES_OUTPUT_DIR,
                    dirs_exist_ok=True)

    logging.info("Dependencies copied successfully.")
    logging.info("✅ RAW layer deployed successfully!")


if __name__ == "__main__":
    main()
