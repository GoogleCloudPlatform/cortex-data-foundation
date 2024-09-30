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
Generates DAG and related files needed to copy/move Salesforce data from
Salesforce system to BigQuery CDC dataset.
"""

import csv
import datetime
import json
import logging
import shutil
import sys
import yaml
from pathlib import Path

from common.py_libs import cortex_bq_client
from common.py_libs.bq_helper import table_exists, create_table
from common.py_libs.configs import load_config_file
from common.py_libs.dag_generator import generate_file_from_template


# NOTE: All paths here are relative to the root directory, unless specified
# otherwise.

_THIS_DIR = Path(__file__).resolve().parent

# Config file containing various parameters.
_CONFIG_FILE = Path(_THIS_DIR, "../../config/config.json")

# Settings file containing tables to be copied from SFDC.
_SETTINGS_FILE = Path(_THIS_DIR, "../../config/ingestion_settings.yaml")

# Directory under which all the generated dag files and related files
# will be created.
_GENERATED_DAG_DIR = "generated_dag/sfdc/raw"

# Directory that has all the dependencies for python dag code
_DEPENDENCIES_INPUT_DIR = Path(_THIS_DIR, "dependencies")
_DEPENDENCIES_OUTPUT_DIR = Path(_GENERATED_DAG_DIR, "dag_dependencies")

# Template files
_TEMPLATE_DIR = Path(_THIS_DIR, "templates")


def process_table(bq_client, table_config, raw_dataset, raw_project):

    api_name = table_config["api_name"]
    base_table = table_config["base_table"].lower()

    logging.info("  Generating files for '%s'", base_table)

    python_template_file = Path(_TEMPLATE_DIR, "airflow_dag_sfdc_to_raw.py")

    output_dag_py_file = Path(
        _GENERATED_DAG_DIR,
        (raw_project + "_" + raw_dataset + "_sfdc_extract_to_raw_"
         + base_table.replace(".", "_") + ".py"))

    today = datetime.datetime.now()
    load_frequency = table_config["load_frequency"]
    subs = {
        "project_id": raw_project,
        "raw_dataset": raw_dataset,
        "base_table": base_table,
        "api_name": api_name,
        "load_frequency": load_frequency,
        "year": today.year,
        "month": today.month,
        "day": today.day
    }

    generate_file_from_template(python_template_file, output_dag_py_file,
                                **subs)

    logging.info("      Generated dag python file")

    raw_table = raw_project + "." + raw_dataset + "." + base_table
    if not table_exists(bq_client, raw_table):
        logging.info(
            "Raw table %s doesn't exists. "
            "Creating one according to the schema mapping.", raw_table)
        schema_file = Path(_THIS_DIR,
                           f"../table_schema/{base_table}.csv").absolute()
        schema_list = []
        has_recordstamp = False
        with open(
                schema_file,
                encoding="utf-8",
                newline="",
        ) as csv_file:
            for row in csv.DictReader(csv_file, delimiter=","):
                source_name = row["SourceField"]
                target_name = row["TargetField"]
                if "recordstamp" in [source_name.lower(), target_name.lower()]:
                    has_recordstamp = True
                schema_list.append((source_name, row["DataType"]))
        # If we handle raw tables, we need Recordstamp field.
        if not has_recordstamp:
            schema_list.append(("Recordstamp", "TIMESTAMP"))
        create_table(bq_client, raw_table, schema_list)


def main():
    logging.basicConfig(level=logging.INFO)
    logging.info("Generating raw dags...")

    # Lets load configs to get various parameters needed for the dag generation.
    config_dict = load_config_file(_CONFIG_FILE)
    logging.info(
        "\n---------------------------------------\n"
        "Using the following config:\n %s"
        "\n---------------------------------------\n",
        json.dumps(config_dict, indent=4))

    raw_project = config_dict.get("projectIdSource")
    raw_dataset = config_dict.get("SFDC").get("datasets").get("raw")
    location = config_dict.get("location", "US")

    logging.info(
        "\n---------------------------------------\n"
        "Using the following parameters from config:\n"
        "  raw_project = %s \n"
        "  raw_dataset = %s \n"
        "  location = %s \n"
        "---------------------------------------\n", raw_project, raw_dataset,
        location)

    Path(_GENERATED_DAG_DIR).mkdir(exist_ok=True, parents=True)
    Path(_DEPENDENCIES_OUTPUT_DIR).mkdir(exist_ok=True, parents=True)

    # Process tables based on configs from settings file
    logging.info("Reading configs...")

    if not Path(_SETTINGS_FILE).is_file():
        logging.warning(
            "File '%s' does not exist. Skipping Raw DAG generation.",
            _SETTINGS_FILE)
        sys.exit()

    with open(_SETTINGS_FILE, encoding="utf-8") as settings_file:
        configs = yaml.load(settings_file, Loader=yaml.SafeLoader)

    if not configs:
        logging.warning("File '%s' is empty. Skipping Raw DAG generation.",
                        _SETTINGS_FILE)
        sys.exit()

    if not "salesforce_to_raw_tables" in configs:
        logging.warning(
            "File '%s' is missing property `salesforce_to_raw_tables`. "
            "Skipping Raw DAG generation.", _SETTINGS_FILE)
        sys.exit()

    logging.info("Processing tables...")

    bq_client = cortex_bq_client.CortexBQClient()

    table_configs = configs["salesforce_to_raw_tables"]
    for table_config in table_configs:
        process_table(bq_client, table_config, raw_dataset, raw_project)

    # Copy Dependencies for the DAG Python files too
    logging.info("Copying dependencies...")
    shutil.copytree(src=_DEPENDENCIES_INPUT_DIR,
                    dst=_DEPENDENCIES_OUTPUT_DIR,
                    dirs_exist_ok=True)

    logging.info("Done generating raw dags.")


if __name__ == "__main__":
    main()
