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
Generates DAG and related files needed to fetch Ads data using TikTok API
for Business to BigQuery RAW dataset.
"""

import argparse
from datetime import datetime
from datetime import timezone
import logging
from pathlib import Path
import shutil
import sys

from common.py_libs import cortex_bq_client
from common.py_libs.bq_helper import table_exists
from common.py_libs.bq_helper import create_table_from_schema
from common.py_libs.dag_generator import generate_file_from_template

from src.constants import PROJECT_REGION
from src.constants import RAW_DATASET
from src.constants import RAW_PROJECT
from src.constants import SCHEMA_DIR
from src.constants import SETTINGS
from src.py_libs.table_creation_utils import create_bq_schema
from src.raw.constants import DAG_TEMPLATE_FILE
from src.raw.constants import DEPENDENCIES_INPUT_DIR
from src.raw.constants import DEPENDENCIES_OUTPUT_DIR
from src.raw.constants import OUTPUT_DIR_FOR_RAW
from src.raw.constants import SCHEMAS_OUTPUT_DIR


def _generate_dag_from_template(template_file: Path,
                                generation_target_directory: Path,
                                table_name: str, subs: dict):
    """Generates DAG code from template file.
    Each DAG is responsible for loading one table.

    Args:
        template_file (Path): Path of the template file.
        generation_target_directory (Path): Directory where files are generated.
        table_name (str): The table name which is loaded by this dag.
        subs (dict): DAG template substitutions.
    """
    output_dag_py_file_name = (
        f"{RAW_PROJECT}_{RAW_DATASET}_"
        f"extract_to_raw_{table_name.replace('.', '_')}.py")

    output_dag_py_file = Path(generation_target_directory,
                              output_dag_py_file_name)

    logging.info("Generating a DAG file for '%s'", table_name)

    generate_file_from_template(template_file, output_dag_py_file, **subs)


def _create_output_dir_structure() -> None:
    OUTPUT_DIR_FOR_RAW.mkdir(exist_ok=True, parents=True)
    SCHEMAS_OUTPUT_DIR.mkdir(exist_ok=True, parents=True)
    DEPENDENCIES_OUTPUT_DIR.mkdir(exist_ok=True, parents=True)


def _parse_args(args) -> tuple[str, str, str]:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pipeline-temp-bucket", type=str, required=True)
    parser.add_argument("--pipeline-staging-bucket", type=str, required=True)
    parser.add_argument("--debug",
                        action="store_true",
                        help="Flag to set log level to DEBUG. Default is INFO.")
    args = parser.parse_args(args)
    return args.pipeline_temp_bucket, args.pipeline_staging_bucket,\
        args.debug


def main(parsed_args):
    pipeline_temp_location, pipeline_staging_location,\
        debug = parsed_args

    logging.basicConfig(level=logging.DEBUG if debug else logging.INFO)

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
    dag_start_date = datetime.now(timezone.utc).date()

    logging.info("Generating raw DAGs...")

    client = cortex_bq_client.CortexBQClient(project=RAW_PROJECT)

    if not "source_to_raw_tables" in SETTINGS:
        logging.warning(
            "File '%s' is missing property `source_to_raw_tables`. "
            "Skipping RAW DAG generation.", SETTINGS)
        sys.exit()

    raw_layer_settings = SETTINGS.get("source_to_raw_tables")

    for raw_table_settings in raw_layer_settings:

        logging.info("Checking settings...")

        missing_raw_setting_attr = []
        for attr in ("load_frequency", "base_table", "schema_file"):
            if raw_table_settings.get(attr) is None or raw_table_settings.get(
                    attr) == "":
                missing_raw_setting_attr.append(attr)
        if missing_raw_setting_attr:
            raise ValueError(
                "Setting file is missing or has empty value for one or more "
                f"attributes: {missing_raw_setting_attr} ")

        load_frequency = raw_table_settings.get("load_frequency")
        table_name = raw_table_settings.get("base_table")
        schema_file = raw_table_settings.get("schema_file")
        partition_details = raw_table_settings.get("partition_details")
        cluster_details = raw_table_settings.get("cluster_details")

        logging.debug("Processing table %s", table_name)

        table_mapping_path = Path(SCHEMA_DIR, schema_file)
        full_table_name = f"{RAW_PROJECT}.{RAW_DATASET}.{table_name}"

        if table_exists(bq_client=client, full_table_name=full_table_name):
            logging.warning("❗ Table already exists.")

        else:
            logging.info("Creating schema...")

            table_schema = create_bq_schema(mapping_file=table_mapping_path,
                                            layer="raw")

            logging.info("Creating RAW table...")

            create_table_from_schema(bq_client=client,
                                     full_table_name=full_table_name,
                                     schema=table_schema,
                                     partition_details=partition_details,
                                     cluster_details=cluster_details)

            logging.info("Table %s.%s.%s has been created.", RAW_PROJECT,
                         RAW_DATASET, table_name)

        subs = {
            "project_id":
                RAW_PROJECT,
            "raw_dataset":
                RAW_DATASET,
            "table_name":
                table_name,
            "load_frequency":
                load_frequency,
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
                         "tiktok_source_to_raw_pipeline.py")),
            "pipeline_setup":
                str(Path(DEPENDENCIES_OUTPUT_DIR.stem, "setup.py")),
        }

        logging.info("Generated dag python file")

        _generate_dag_from_template(
            template_file=DAG_TEMPLATE_FILE,
            generation_target_directory=OUTPUT_DIR_FOR_RAW,
            table_name=table_name,
            subs=subs)

    logging.info("Done generating raw DAGs.")
    logging.info("Copying dependencies...")

    shutil.copytree(src=DEPENDENCIES_INPUT_DIR,
                    dst=DEPENDENCIES_OUTPUT_DIR,
                    dirs_exist_ok=True)

    logging.info("Done preparing the directory for Airflow.")


if __name__ == "__main__":
    deploy_arguments = _parse_args(sys.argv[1:])
    main(deploy_arguments)
