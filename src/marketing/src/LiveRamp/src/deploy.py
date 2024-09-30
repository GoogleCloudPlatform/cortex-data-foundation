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
Generates DAG and related files needed to copy/move data from LiveRamp system
to BigQuery dataset.
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
from common.py_libs.dag_generator import generate_file_from_template
from common.py_libs.jinja import apply_jinja_params_dict_to_file

from src.constants import DAG_TEMPLATE_FILE
from src.constants import DATASET
from src.constants import DDL_SQL_FILES
from src.constants import DEPENDENCIES_INPUT_DIR
from src.constants import DEPENDENCIES_OUTPUT_DIR
from src.constants import OUTPUT_DIR
from src.constants import PROJECT


def _generate_dag_from_template(template_file: Path,
                                generation_target_directory: Path,
                                subs: dict) -> None:
    """Generates DAG code from template file.
    Each DAG is responsible for loading one table.

    Args:
        template_file (Path): Path of the template file.
        generation_target_directory (Path): Directory where files are generated.
        subs (dict): DAG template substitutions.
    """
    output_dag_py_file = Path(generation_target_directory,
                              "marketing_liveramp_extract_to_bq.py")

    generate_file_from_template(template_file, output_dag_py_file, **subs)


def _create_output_dir_structure() -> None:
    OUTPUT_DIR.mkdir(exist_ok=True, parents=True)
    DEPENDENCIES_OUTPUT_DIR.mkdir(exist_ok=True, parents=True)


def _parse_args(args) -> str:
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug",
                        action="store_true",
                        help="Flag to set log level to DEBUG. Default is INFO.")
    args = parser.parse_args(args)
    return args.debug


def main(parsed_args):
    debug = parsed_args

    logging.basicConfig(level=logging.DEBUG if debug else logging.INFO)

    logging.info(
        "\n---------------------------------------\n"
        "Using the following parameters from config:\n"
        "  PROJECT = %s \n"
        "  DATASET = %s \n"
        "---------------------------------------\n", PROJECT, DATASET)
    logging.info("Processing tables...")

    _create_output_dir_structure()

    dag_start_date = datetime.now(timezone.utc).date()

    bq_client = cortex_bq_client.CortexBQClient()

    # Get table name and SQL file for current table.
    for sql_file in DDL_SQL_FILES:

        sql_subs = {
            "project_id_src": PROJECT,
            "marketing_liveramp_datasets_cdc": DATASET,
        }

        full_table_name = f"{PROJECT}.{DATASET}.{sql_file.stem}"

        if table_exists(bq_client=bq_client, full_table_name=full_table_name):
            logging.warning("❗ Table already exists.")
        else:
            logging.info("Processing table %s", sql_file)

            sql_code = apply_jinja_params_dict_to_file(input_file=sql_file,
                                                       jinja_data_dict=sql_subs)

            logging.debug("Creating table %s ...", sql_file)

            query_job = bq_client.query(sql_code)

            # Let's wait for the query to complete.
            _ = query_job.result()

            logging.debug("Table %s.%s.%s has been created.", PROJECT, DATASET,
                          sql_file)

            logging.info("Table processed successfully.")

    py_subs = {
        "project_id": PROJECT,
        "dataset": DATASET,
        "start_date": dag_start_date
    }

    logging.info("Generating DAG...")
    _generate_dag_from_template(template_file=DAG_TEMPLATE_FILE,
                                generation_target_directory=OUTPUT_DIR,
                                subs=py_subs)

    logging.info("Done generating DAG.")

    logging.info("-----------------------------")

    logging.info("Copying dependencies...")

    shutil.copytree(src=DEPENDENCIES_INPUT_DIR,
                    dst=DEPENDENCIES_OUTPUT_DIR,
                    dirs_exist_ok=True)

    logging.info("Done preparing the directory for Airflow.")

    logging.info("✅ LiveRamp module deployed successfully!")


if __name__ == "__main__":
    deploy_arguments = _parse_args(sys.argv[1:])
    main(deploy_arguments)
