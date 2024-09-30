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
"""Deployment script for RAW layer.

Generates DAG and related files needed to transfer data from
Salesforce Marketing Cloud to BigQuery RAW dataset.
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
from common.py_libs.bq_helper import table_exists
from common.py_libs.bq_helper import create_table_from_schema
from common.py_libs.dag_generator import generate_file_from_template
from common.py_libs.schema_reader import read_bq_schema

from src.constants import FILE_TRANSFER_BUCKET
from src.constants import PROJECT_REGION
from src.constants import RAW_DATASET
from src.constants import RAW_PROJECT
from src.constants import SCHEMA_DIR
from src.constants import SCHEMA_TARGET_FIELD
from src.constants import SETTINGS
from src.constants import SYSTEM_FIELDS
from src.py_libs.utils import repr_schema
from src.raw.constants import DAG_TEMPLATE_PATH
from src.raw.constants import DEPENDENCIES_INPUT_DIR
from src.raw.constants import DEPENDENCIES_OUTPUT_DIR
from src.raw.constants import OUTPUT_DIR_FOR_RAW
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


def main():
    """Main function."""
    args = _parse_args(sys.argv[1:])
    logging.basicConfig(level=logging.DEBUG if args["debug"] else logging.INFO)

    logging.info("Deploying RAW layer...")
    logging.info(
        "\n---------------------------------------\n"
        "Using the following parameters from config:\n"
        "  RAW_PROJECT = %s \n"
        "  RAW_DATASET = %s \n"
        "---------------------------------------\n", RAW_PROJECT, RAW_DATASET)

    logging.info("Processing tables...")

    _create_output_dir_structure()

    logging.info("Copying schema files...")

    shutil.copytree(src=SCHEMA_DIR, dst=SCHEMAS_OUTPUT_DIR, dirs_exist_ok=True)

    if not "source_to_raw_tables" in SETTINGS:
        logging.warning(
            "File '%s' is missing property `source_to_raw_tables`. "
            "Skipping RAW DAG generation.", SETTINGS)
        sys.exit()

    raw_layer_settings = SETTINGS.get("source_to_raw_tables")

    client = cortex_bq_client.CortexBQClient(project=RAW_PROJECT)

    for raw_table_settings in raw_layer_settings:

        logging.info("Checking settings...")

        missing_raw_setting_attr = []
        for attr in ("load_frequency", "base_table", "partition_details"):
            if raw_table_settings.get(attr) is None or raw_table_settings.get(
                    attr) == "":
                missing_raw_setting_attr.append(attr)
        if missing_raw_setting_attr:
            raise ValueError(
                "Setting file is missing or has empty value for one or more "
                f"attributes: {missing_raw_setting_attr} ")

        load_frequency = raw_table_settings.get("load_frequency")
        table_name = raw_table_settings.get("base_table")
        file_pattern = raw_table_settings.get("file_pattern")
        partition_details = raw_table_settings.get("partition_details")
        cluster_details = raw_table_settings.get("cluster_details")

        logging.debug("Processing table %s", table_name)

        table_mapping_path = Path(SCHEMA_DIR, f"{table_name}.csv")
        full_table_name = f"{RAW_PROJECT}.{RAW_DATASET}.{table_name}"

        if table_exists(bq_client=client, full_table_name=full_table_name):
            logging.warning("❗ Table already exists.")
        else:
            logging.info("Creating schema...")

            schema = read_bq_schema(mapping_file=table_mapping_path,
                                    schema_target_field=SCHEMA_TARGET_FIELD,
                                    system_fields=SYSTEM_FIELDS)

            logging.debug("RAW Table schema: %s\n", repr_schema(schema))
            logging.info("Creating RAW table...")

            create_table_from_schema(bq_client=client,
                                     full_table_name=full_table_name,
                                     schema=schema,
                                     partition_details=partition_details,
                                     cluster_details=cluster_details)

            logging.info("Table %s is created successfully.", table_name)

        # DAG Python file generation
        logging.info("Generating DAG Python file for %s", table_name)

        dag_start_date = datetime.now(timezone.utc).date()
        pipeline_setup_file = Path(DEPENDENCIES_OUTPUT_DIR.stem, "setup.py")
        pipeline_file = Path(DEPENDENCIES_OUTPUT_DIR.stem,
                             "source_to_raw_pipeline.py")

        subs = {
            "project_id": RAW_PROJECT,
            "raw_dataset": RAW_DATASET,
            "table_name": table_name,
            "load_frequency": load_frequency,
            "start_date": dag_start_date,
            "pipeline_file": str(pipeline_file),
            "pipeline_setup": str(pipeline_setup_file),
            "pipeline_staging_bucket": args["pipeline_staging_bucket"],
            "pipeline_temp_bucket": args["pipeline_temp_bucket"],
            "project_region": PROJECT_REGION,
            "file_pattern": file_pattern,
            "schema_file": table_mapping_path.name,
            "schemas_dir": SCHEMAS_OUTPUT_DIR.stem,
            "data_transfer_bucket": FILE_TRANSFER_BUCKET
        }

        output_dag_py_file = (
            f"{RAW_PROJECT}_{RAW_DATASET}"
            f"_extract_to_raw_{table_name.replace('.', '_')}.py")
        output_dag_py_path = Path(OUTPUT_DIR_FOR_RAW, output_dag_py_file)
        generate_file_from_template(DAG_TEMPLATE_PATH, output_dag_py_path,
                                    **subs)

        logging.info("Generated DAG Python file.")

    logging.info("Processed all tables successfully.")
    logging.info("Copying dependencies...")

    shutil.copytree(src=DEPENDENCIES_INPUT_DIR,
                    dst=DEPENDENCIES_OUTPUT_DIR,
                    dirs_exist_ok=True)

    logging.info("Copied dependencies files successfully.")
    logging.info("✅ RAW layer deployed successfully!")


if __name__ == "__main__":
    main()
