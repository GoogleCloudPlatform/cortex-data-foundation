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
Generates DAG and related files needed to copy/move Ads data from
Google Adwords system to BigQuery RAW dataset.
"""

import argparse
from datetime import datetime
from datetime import timezone
import logging
from pathlib import Path
import shutil
import sys

from src.constants import POPULATE_TEST_DATA
from src.constants import PROJECT_REGION
from src.constants import RAW_DATASET
from src.constants import RAW_PROJECT
from src.constants import SCHEMA_DIR
from src.constants import SETTINGS
from src.py_libs.table_creation_utils import create_bq_schema
from src.py_libs.table_creation_utils import repr_schema
from src.raw.constants import DAG_TEMPLATE_FILE
from src.raw.constants import DEPENDENCIES_INPUT_DIR
from src.raw.constants import DEPENDENCIES_OUTPUT_DIR
from src.raw.constants import OUTPUT_DIR_FOR_RAW
from src.raw.constants import SCHEMAS_OUTPUT_DIR
from src.raw.constants import SQL_OUTPUT_DIR
from src.raw.view_creation_utils import create_view

from common.py_libs import cortex_bq_client
from common.py_libs.bq_helper import create_table_from_schema
from common.py_libs.bq_helper import table_exists
from common.py_libs.dag_generator import generate_file_from_template


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
    new_table_name = table_name.replace(".", "_")
    output_dag_py_file = Path(
        generation_target_directory,
        f"{project_id}_{dataset_id}_extract_to_raw_{new_table_name}.py")
    generate_file_from_template(template_file, output_dag_py_file, **subs)


def _create_output_dir_structure() -> None:
    OUTPUT_DIR_FOR_RAW.mkdir(exist_ok=True, parents=True)
    SCHEMAS_OUTPUT_DIR.mkdir(exist_ok=True, parents=True)
    DEPENDENCIES_OUTPUT_DIR.mkdir(exist_ok=True, parents=True)
    SQL_OUTPUT_DIR.mkdir(exist_ok=True, parents=True)


def _parse_args() -> tuple[str, str, str]:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pipeline-temp-bucket", type=str, required=True)
    parser.add_argument("--pipeline-staging-bucket", type=str, required=True)
    parser.add_argument("--debug",
                        action="store_true",
                        help="Flag to set log level to DEBUG. Default is INFO.")
    args = parser.parse_args()
    return args.pipeline_temp_bucket, args.pipeline_staging_bucket,\
        args.debug


def main():
    pipeline_arguments = _parse_args()
    pipeline_temp_location, pipeline_staging_location,\
        debug_arg = pipeline_arguments

    level = logging.DEBUG if debug_arg else logging.INFO
    logging.getLogger().setLevel(level)

    logging.info("Deploying raw layer...")
    logging.info(
        "\n---------------------------------------\n"
        "Using the following parameters from config:\n"
        "  RAW_PROJECT = %s\n"
        "  RAW_DATASET = %s\n"
        "---------------------------------------\n", RAW_PROJECT, RAW_DATASET)

    logging.info("Creating required directories for generated files...")
    _create_output_dir_structure()

    logging.info("Copying schema files...")
    shutil.copytree(src=SCHEMA_DIR, dst=SCHEMAS_OUTPUT_DIR, dirs_exist_ok=True)

    dag_start_date = datetime.now(timezone.utc).date()
    client = cortex_bq_client.CortexBQClient(project=RAW_PROJECT)

    if not "source_to_raw_tables" in SETTINGS:
        logging.warning(
            "❗ property `source_to_raw_tables` missing in settings file. "
            "Skipping raw DAG generation.")
        sys.exit()

    raw_layer_settings = SETTINGS.get("source_to_raw_tables")

    logging.info("Processing raw tables...")
    for raw_table_settings in raw_layer_settings:

        # Making sure all required setting attributes are provided.
        missing_raw_setting_attr = []
        for attr in ("load_frequency", "key", "resource_type", "api_name",
                     "schema_file", "table_name"):
            if raw_table_settings.get(attr) is None or raw_table_settings.get(
                    attr) == "":
                missing_raw_setting_attr.append(attr)
        if missing_raw_setting_attr:
            raise ValueError(
                "Setting file is missing or has empty value for one or more "
                f"attributes: {missing_raw_setting_attr} ")

        load_frequency = raw_table_settings.get("load_frequency")
        key_fields = raw_table_settings.get("key").split(",")
        resource_type = raw_table_settings.get("resource_type")
        api_name = raw_table_settings.get("api_name")
        schema_file = raw_table_settings.get("schema_file")
        table_name = raw_table_settings.get("table_name")
        partition_details = raw_table_settings.get("partition_details")
        cluster_details = raw_table_settings.get("cluster_details")

        logging.info("-- Processing table '%s' --", table_name)

        table_mapping_path = Path(SCHEMA_DIR, schema_file)

        logging.info("Creating raw table...")
        full_table_name = RAW_PROJECT + "." + RAW_DATASET + "." + table_name
        # Check if raw table exists.
        raw_table_exists = table_exists(client, full_table_name)
        if not raw_table_exists:
            logging.info("Creating schema...")
            table_schema = create_bq_schema(table_mapping_path)
            logging.debug("Raw table schema: %s\n", repr_schema(table_schema))
            create_table_from_schema(bq_client=client,
                         schema=table_schema,
                         full_table_name=full_table_name,
                         partition_details=partition_details,
                         cluster_details=cluster_details)
            logging.info("Table is created successfully.")
        else:
            logging.warning("❗ Table already exists. Not creating table.")

        # DAG PY file generation
        logging.info("Generating DAG python file...")
        subs = {
            "project_id":
                RAW_PROJECT,
            "api_name":
                api_name,
            "raw_dataset":
                RAW_DATASET,
            "table_name":
                table_name,
            "load_frequency":
                load_frequency,
            "resource_type":
                resource_type,
            "start_date":
                dag_start_date,
            "schema_file":
                table_mapping_path.name,
            "pipeline_temp_location":
                pipeline_temp_location,
            "pipeline_staging_location":
                pipeline_staging_location,
            "project_region":
                PROJECT_REGION,
            "schemas_dir":
                SCHEMAS_OUTPUT_DIR.stem,
            "pipeline_file":
                str(
                    Path(DEPENDENCIES_OUTPUT_DIR.stem,
                         "ads_source_to_raw_pipeline.py")),
            "pipeline_setup":
                str(Path(DEPENDENCIES_OUTPUT_DIR.stem, "setup.py")),
        }
        _generate_dag_from_template(
            template_file=DAG_TEMPLATE_FILE,
            generation_target_directory=OUTPUT_DIR_FOR_RAW,
            project_id=RAW_PROJECT,
            dataset_id=RAW_DATASET,
            table_name=table_name,
            subs=subs)
        logging.info("Generated dag python file.")

        # Creating view that transforms raw data into format that's usable
        # in CDC layer according to schema defined in settings.
        #
        # View creation is performed in two scenarios:
        # a. If raw table is created above.
        # b. If testData flag is true, then raw table is already created by test
        #    harness. We still need to create the views.
        if POPULATE_TEST_DATA or not raw_table_exists:
            logging.info("Creating raw view...")
            try:
                create_view(client=client,
                            table_mapping_path=table_mapping_path,
                            table_name=table_name,
                            raw_project=RAW_PROJECT,
                            raw_dataset=RAW_DATASET,
                            key_fields=key_fields)
            except Exception as e:
                logging.error("Failed to create raw view '%s'.\n"
                              "ERROR: %s", table_name, str(e))
                raise SystemExit(
                    "⛔️ Failed to deploy raw views. Please check the logs."
                ) from e
            logging.info("View created successfully.")

        logging.info("Table processed successfully.")
        logging.info("-----------------------------")

    logging.info("Processed all tables successfully.")

    logging.info("Copying dependencies...")
    shutil.copytree(src=DEPENDENCIES_INPUT_DIR,
                    dst=DEPENDENCIES_OUTPUT_DIR,
                    dirs_exist_ok=True)
    logging.info("Copied dependencies files successfully.")

    logging.info("✅ Raw layer deployed successfully!")


if __name__ == "__main__":
    main()
