# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Creates CDC tables and related DAGS."""

import jinja2
import logging
import os
import pathlib
import sys
import yaml

from concurrent import futures

# Make sure common modules are in Python path
sys.path.append(str(pathlib.Path(__file__).parent.parent))

# pylint:disable=wrong-import-position

from common.py_libs.configs import load_config_file_from_env

from generate_query import generate_runtime_view
from generate_query import create_cdc_table
from generate_query import generate_cdc_dag_files
from generate_query import validate_table_configs
from generate_query import client as generate_query_bq_client

# File containing list and settings for cdc tables to be copied
# The path is relative to "src" directory.
_CONFIG_FILE = "cdc_settings.yaml"

# This script generates a bunch of DAG and SQL files.
# We store them locally, before they get copied over to GCS buckets.
# TODO: These are declared in two places - here and and in generate_query.
#       It needs a cleanup.
_GENERATED_DAG_DIR = "generated_dag"
_GENERATED_SQL_DIR = "generated_sql"


def process_table(table_config: dict, source_dataset: str, target_dataset: str,
                  gen_test: str, allow_telemetry: bool) -> None:
    try:

        table_name = table_config.get("base_table")
        raw_table = source_dataset + "." + table_name
        logging.info("== Processing table %s ==", raw_table)

        if "target_table" in table_config:
            target_table = table_config["target_table"]
        else:
            target_table = table_name

        cdc_table = target_dataset + "." + target_table

        partition_details = table_config.get("partition_details")
        cluster_details = table_config.get("cluster_details")

        load_frequency = table_config.get("load_frequency")
        if load_frequency == "RUNTIME":
            generate_runtime_view(raw_table, cdc_table)
        else:
            create_cdc_table(raw_table, cdc_table, partition_details,
                             cluster_details)
            # Create files (python and sql) that will be used later to
            # create DAG in GCP that will refresh CDC tables from RAW
            # tables.
            logging.info("Generating required files for DAG with %s ",
                         cdc_table)
            generate_cdc_dag_files(raw_table, cdc_table, load_frequency,
                                   gen_test, allow_telemetry)

        logging.info("✅ == Processed %s ==", raw_table)
    except Exception as e:
        logging.error("⛔️ Error generating dag/sql for %s.\nError: %s",
                      raw_table, str(e))
        raise SystemExit(
            "⛔️ Error while generating sql and dags. Please check the logs."
        ) from e


def main():
    logging.basicConfig()
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    logging.info("Starting config_reader...")

    if not sys.argv[1]:
        raise SystemExit("ERROR: No Source Project argument provided!")
    source_project = sys.argv[1]
    generate_query_bq_client.project = source_project

    if not sys.argv[2]:
        raise SystemExit("ERROR: No Source Dataset argument provided!")
    source_dataset = source_project + "." + sys.argv[2]

    if not sys.argv[3]:
        raise SystemExit("ERROR: No Target Dataset argument provided!")
    target_dataset = source_project + "." + sys.argv[3]

    if not sys.argv[4]:
        raise SystemExit("ERROR: No Test flag argument provided")
    gen_test = sys.argv[4]

    if not sys.argv[5]:
        logging.info("SQL Flavour not provided. Defaulting to ECC.")
        sql_flavour = "ECC"
    else:
        sql_flavour = sys.argv[5]

    os.makedirs(_GENERATED_DAG_DIR, exist_ok=True)
    os.makedirs(_GENERATED_SQL_DIR, exist_ok=True)

    try:
        # Load config from environment variable
        cortex_config = load_config_file_from_env()

        # Gets allowTelemetry config and defaults to True
        allow_telemetry = cortex_config.get("allowTelemetry", True)
    except (FileNotFoundError, PermissionError):
        # File used by telemetry only and the path assumes execution
        # using Cloud Build.
        #
        # Access while executing locally cannot be assumed. If file is not
        # available then continue execution with telemetry not allowed.
        #
        # File not accessible or found, setting allow_telemetry to false.
        allow_telemetry = False

    # Read settings from settings file.
    with open(_CONFIG_FILE, encoding="utf-8") as settings_file:
        t = jinja2.Template(settings_file.read(),
                            trim_blocks=True,
                            lstrip_blocks=True)
        resolved_configs = t.render({"sql_flavour": sql_flavour})

    try:
        configs = yaml.load(resolved_configs, Loader=yaml.SafeLoader)
    except Exception as e:
        logging.error("⛔️ Error reading '%s' file.\nError %s:", _CONFIG_FILE,
                      str(e))
        raise SystemExit() from e

    table_configs = configs["data_to_replicate"]

    # Let's make sure table settings are specified correctly in the settings
    # files.
    # NOTE: We are doing this separately, and ahead of actual processing to
    # make sure we capture any settings error early.
    error_message = validate_table_configs(table_configs)
    if error_message:
        exit_message = (f"⛔ Invalid configurations in '{_CONFIG_FILE}'!! "
                        f"Reason: {error_message}")
        logging.error(exit_message)
        raise SystemExit()

    # Process each table entry in the settings to create CDC table/view.
    # This is done in parallel using multiple threads.
    pool = futures.ThreadPoolExecutor(10)
    threads = []
    for table_config in table_configs:
        threads.append(
            pool.submit(process_table, table_config, source_dataset,
                        target_dataset, gen_test, allow_telemetry))
    if len(threads) > 0:
        logging.info("Waiting for all tasks to complete...")
        futures.wait(threads)

    # In order to capture error from any of the threads,
    # we need to access the result. If any individual thread
    # throws an exception, it will be caught with this call.
    # Otherwise, system will always exit with SUCCESS.
    for t in threads:
        _ = t.result()

    logging.info("✅ config_reader done.")


if __name__ == "__main__":
    main()
