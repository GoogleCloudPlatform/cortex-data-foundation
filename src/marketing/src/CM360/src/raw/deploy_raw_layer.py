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
"""
Generates DAG and related files needed to copy/move CM360 data from
DataTransferV2.0 bucket to BigQuery RAW dataset.
"""
import argparse
from datetime import datetime
from datetime import timezone
import logging
from pathlib import Path
import shutil
import sys

from common.py_libs import cortex_bq_client
from common.py_libs.bq_helper import create_table_from_schema
from common.py_libs.bq_helper import table_exists

from src.constants import DATATRANSFER_BUCKET
from src.constants import PROJECT_REGION
from src.constants import RAW_DATASET
from src.constants import RAW_PROJECT
from src.constants import SCHEMA_DIR
from src.constants import SETTINGS
from src.py_libs.utils import create_bq_schema
from src.py_libs.utils import generate_dag_from_template
from src.py_libs.utils import repr_schema
from src.raw.constants import DAG_TEMPLATE_DIR
from src.raw.constants import DEPENDENCIES_INPUT_DIR
from src.raw.constants import DEPENDENCIES_OUTPUT_DIR
from src.raw.constants import OUTPUT_DIR_FOR_RAW
from src.raw.constants import SCHEMAS_OUTPUT_DIR

_DAG_TEMPLATE_PATH = Path(DAG_TEMPLATE_DIR, "source_to_raw_dag_py_template.py")


def _create_output_dir_structure() -> None:
    OUTPUT_DIR_FOR_RAW.mkdir(exist_ok=True, parents=True)
    SCHEMAS_OUTPUT_DIR.mkdir(exist_ok=True, parents=True)
    DEPENDENCIES_OUTPUT_DIR.mkdir(exist_ok=True, parents=True)


def _parse_args(args):
    parser = argparse.ArgumentParser()
    parser.add_argument("--pipeline-temp-bucket", type=str, required=True)
    parser.add_argument("--pipeline-staging-bucket", type=str, required=True)
    parser.add_argument("--debug",
                        action="store_true",
                        help="Flag to set log level to DEBUG. Default is INFO.")
    args = parser.parse_args(args)
    return args


def main():
    """Main function.

    This function expects --pipeline-temp-bucket and --pipeline-staging-bucket
    parameters for running the beam pipeline.
    """

    parsed_args = _parse_args(sys.argv[1:])
    logging.basicConfig(
        level=logging.DEBUG if parsed_args.debug else logging.INFO)
    logging.info("Parsed args %s", parsed_args)

    logging.info("Deploying raw layer...")
    logging.info(
        "\n---------------------------------------\n"
        "Using the following parameters from config:\n"
        "  RAW_PROJECT = %s \n"
        "  RAW_DATASET = %s \n"
        "---------------------------------------\n", RAW_PROJECT, RAW_DATASET)

    logging.info("Creating required directories for generated files...")
    _create_output_dir_structure()

    logging.info("Copying schema files...")
    shutil.copytree(src=SCHEMA_DIR, dst=SCHEMAS_OUTPUT_DIR, dirs_exist_ok=True)

    dag_start_date = datetime.now(timezone.utc).date()
    client = cortex_bq_client.CortexBQClient(project=RAW_PROJECT)

    if not "source_to_raw_tables" in SETTINGS:
        logging.warning(
            "❗ Property `source_to_raw_tables` missing in settings file. "
            "Skipping cdc DAG generation.")
        sys.exit()

    raw_layer_settings = SETTINGS["source_to_raw_tables"]

    logging.info("Processing RAW tables...")
    for raw_table_settings in raw_layer_settings:
        table_name = raw_table_settings["base_table"]

        logging.info(" -- Processing table '%s' --", table_name)

        table_mapping_path = Path(SCHEMA_DIR, f"{table_name}.csv")
        full_table_name = f"{RAW_PROJECT}.{RAW_DATASET}.{table_name}"

        if table_exists(bq_client=client, full_table_name=full_table_name):
            logging.warning("❗ Table already exists.")
        else:
            logging.info("Creating raw table...")

            logging.info("Creating schema...")
            schema = create_bq_schema(table_mapping_path)
            logging.debug("Raw table schema: %s\n", repr_schema(schema))

            partition_details = raw_table_settings.get("partition_details")
            cluster_details = raw_table_settings.get("cluster_details")

            create_table_from_schema(bq_client=client,
                         full_table_name=full_table_name,
                         schema=schema,
                         partition_details=partition_details,
                         cluster_details=cluster_details)

            logging.info("Table is created successfully.")

        # DAG PY file generation
        logging.info("Generating DAG python file...")
        load_frequency = raw_table_settings["load_frequency"]
        file_pattern = raw_table_settings["file_pattern"]
        pipeline_temp_location = parsed_args.pipeline_temp_bucket
        pipeline_staging_location = parsed_args.pipeline_staging_bucket
        subs = {
            "project_id":
                RAW_PROJECT,
            "raw_dataset":
                RAW_DATASET,
            "table_name":
                table_name,
            "load_frequency":
                load_frequency,
            "file_pattern":
                file_pattern,
            "start_date":
                dag_start_date,
            "datatransfer_bucket":
                DATATRANSFER_BUCKET,
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
                         "cm360_source_to_raw_pipeline.py")),
            "pipeline_setup":
                str(Path(DEPENDENCIES_OUTPUT_DIR.stem, "setup.py")),
        }
        generate_dag_from_template(
            template_file=_DAG_TEMPLATE_PATH,
            generation_target_directory=OUTPUT_DIR_FOR_RAW,
            project_id=RAW_PROJECT,
            dataset_id=RAW_DATASET,
            table_name=table_name,
            layer="extract_to_raw",
            subs=subs)
        logging.info("Generated dag python file.")

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
