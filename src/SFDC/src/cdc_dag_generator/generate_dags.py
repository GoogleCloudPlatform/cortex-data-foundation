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
RAW dataset to CDC dataset.
"""

import csv
import datetime
import json
import logging
import sys
import yaml
from pathlib import Path
import typing

from common.py_libs import constants
from common.py_libs import cortex_bq_client
from common.py_libs.bq_helper import execute_sql_file
from common.py_libs.cdc import create_cdc_table
from common.py_libs.configs import load_config_file
from common.py_libs.dag_generator import generate_file_from_template

# NOTE: All paths here are relative to the root directory, unless specified
# otherwise.

_THIS_DIR = Path(__file__).resolve().parent

# Config file containing various parameters.
_CONFIG_FILE = Path(_THIS_DIR, "../../config/config.json")

# Settings file containing tables to be copied from SFDC.
_SETTINGS_FILE = Path(_THIS_DIR, "../../config/ingestion_settings.yaml")

_TEMPLATE_FILE_PREFIX = "sfdc_raw_to_cdc_"
_TEMPLATE_SQL_NAME = "template"

# Directory under which all the generated dag files and related files
# will be created.
_GENERATED_DAG_DIR = "generated_dag/sfdc/cdc"
# Directory under which all the generated sql files will be created.
_GENERATED_DAG_SQL_DIR = "generated_sql/sfdc/cdc/sql_scripts"

# Directory containing various template files.
_TEMPLATE_DIR = Path(_THIS_DIR, "templates")
# Directory containing various template files.
_SQL_TEMPLATE_DIR = Path(_TEMPLATE_DIR, "sql")


def process_table(bq_client, table_setting, project_id, raw_dataset,
                  cdc_dataset, load_test_data):
    """For a given table config, creates required tables as well as
    dag and related files. """

    base_table = table_setting["base_table"].lower()
    raw_table = table_setting["raw_table"]
    api_name = table_setting["api_name"]

    schema_file = Path(_THIS_DIR,
                       f"../table_schema/{base_table}.csv").absolute()

    logging.info("__ Processing table '%s' __", base_table)

    sfdc_to_bq_field_map: typing.Dict[str, typing.Tuple[str, str]] = {}

    # TODO: Check Config File schema.
    with open(
            schema_file,
            encoding="utf-8",
            newline="",
    ) as csv_file:
        for row in csv.DictReader(csv_file, delimiter=","):
            sfdc_to_bq_field_map[row["SourceField"]] = (row["TargetField"],
                                                        row["DataType"])
    source_fields_lower = [f.lower() for f in sfdc_to_bq_field_map]
    target_fields_lower = [f[0].lower() for f in sfdc_to_bq_field_map.values()]

    # Making sure important fields are in the destination.
    # Work on Id field first.
    if "id" not in source_fields_lower:
        # No Id field in Raw. Trying to one via the target name
        # which is supposed to be {api_name}Id
        id_name = f"{api_name}id".lower()
        if id_name in target_fields_lower:
            id_index = target_fields_lower.index(id_name)
        else:
            raise ValueError(
                (f"Cannot find the Id field for {base_table}. "
                 f"It must be mapped to {api_name}Id field in CDC."))
    else:
        id_index = source_fields_lower.index("id")
    source_id_field_name = list(sfdc_to_bq_field_map.keys())[id_index]
    id_field_name = sfdc_to_bq_field_map[source_id_field_name][0]
    # Work on SystemModstamp.
    if "systemmodstamp" not in source_fields_lower:
        if "systemmodstamp" in target_fields_lower:
            sms_index = target_fields_lower.index("systemmodstamp")
        else:
            raise ValueError(
                (f"Cannot find the SystemModstamp field for {base_table}. "
                 "It must be mapped to SystemModstamp field in CDC."))
    else:
        sms_index = source_fields_lower.index("systemmodstamp")
    source_sms_field_name = list(sfdc_to_bq_field_map.keys())[sms_index]
    sms_field_name = sfdc_to_bq_field_map[source_sms_field_name][0]

    # Create CDC table if needed.
    #############################
    try:
        create_cdc_table(bq_client, table_setting, project_id, cdc_dataset,
                         list(sfdc_to_bq_field_map.values()))
    except Exception as e:
        logging.error("Failed while processing table '%s'.\n"
                      "ERROR: %s", base_table, str(e))
        raise SystemExit(
            "⛔️ Failed to deploy CDC. Please check the logs.") from e

    generated_file_prefix = project_id + "_" + cdc_dataset + "_raw_to_cdc_"

    # Python file generation
    #########################
    python_template_file = Path(_TEMPLATE_DIR, "airflow_dag_raw_to_cdc.py")
    output_py_file_name = (generated_file_prefix +
                           base_table.replace(".", "_") + ".py")
    output_py_file = Path(_GENERATED_DAG_DIR, output_py_file_name)

    today = datetime.datetime.now()
    load_frequency = table_setting["load_frequency"]

    py_subs = {
        "project_id": project_id,
        "raw_dataset": raw_dataset,
        "cdc_dataset": cdc_dataset,
        "base_table": base_table,
        "load_frequency": load_frequency,
        "year": today.year,
        "month": today.month,
        "day": today.day,
        "runtime_labels_dict": "", # A place holder for label dict
    }

    # Add bq_labels to py_subs dict if telemetry is allowed
    # Convert CORTEX_JOB_LABEL dict to str for substitution purposes
    if bq_client.allow_telemetry:
        py_subs["runtime_labels_dict"] = str(constants.CORTEX_JOB_LABEL)

    generate_file_from_template(python_template_file, output_py_file, **py_subs)

    logging.info("Generated dag python files")

    # SQL file generation
    #########################
    sql_template_file_name = (_TEMPLATE_FILE_PREFIX + _TEMPLATE_SQL_NAME +
                              ".sql")
    sql_file_name = (base_table.replace(".", "_") +
                     ".sql")
    sql_template_file = Path(_SQL_TEMPLATE_DIR, sql_template_file_name)
    output_sql_file = Path(_GENERATED_DAG_SQL_DIR, sql_file_name)

    field_assignments = [
        f"`{f[0]}` AS `{f[1][0]}`" for f in sfdc_to_bq_field_map.items()
    ]
    target_fields = [f"`{f[0]}`" for f in sfdc_to_bq_field_map.values()]

    sql_subs = {
        "source_table": project_id + "." + raw_dataset + "." + raw_table,
        "target_table": project_id + "." + cdc_dataset + "." + base_table,
        "source_id": source_id_field_name,
        "target_id": id_field_name,
        "source_systemmodstamp": source_sms_field_name,
        "target_systemmodstamp": sms_field_name,
        "target_fields": ",\n  ".join(target_fields),
        "field_assignments": ",\n  ".join(field_assignments)
    }

    generate_file_from_template(sql_template_file, output_sql_file, **sql_subs)
    logging.info("Generated DAG SQL file.")

    # If test data is needed, we want to populate the CDC table from data in
    # the RAW tables. Let's use the DAG SQL file to do that.
    if str(load_test_data).lower() == "true":
        try:
            execute_sql_file(bq_client, output_sql_file)
            logging.info("Populated CDC table with test data.")
        except Exception as e:
            logging.error("Failed to populate CDC table '%s'.\n"
                          "ERROR: %s", (cdc_dataset + "." + base_table), str(e))
            raise SystemExit(
                "⛔️ Failed to deploy CDC. Please check the logs.") from e

    logging.info("__ Table '%s' processed.__", base_table)


def main():
    logging.basicConfig(level=logging.INFO)
    logging.info("Generating CDC tables and DAGS...")

    # Lets load configs to get various parameters needed for the dag generation.
    config_dict = load_config_file(_CONFIG_FILE)
    logging.info(
        "\n---------------------------------------\n"
        "Using the following config:\n %s"
        "\n---------------------------------------\n",
        json.dumps(config_dict, indent=4))

    # Read params from the config
    project_id = config_dict.get("projectIdSource")
    raw_dataset = config_dict.get("SFDC").get("datasets").get("raw")
    cdc_dataset = config_dict.get("SFDC").get("datasets").get("cdc")
    load_test_data = config_dict.get("testData")

    logging.info(
        "\n---------------------------------------\n"
        "Using the following parameters from config:\n"
        "  source_project = %s \n"
        "  raw_dataset = %s \n"
        "  cdc_dataset = %s \n"
        "  load_test_data = %s \n"
        "---------------------------------------\n", project_id, raw_dataset,
        cdc_dataset, load_test_data)

    Path(_GENERATED_DAG_DIR).mkdir(exist_ok=True, parents=True)
    Path(_GENERATED_DAG_SQL_DIR).mkdir(exist_ok=True, parents=True)

    # Process tables based on table settings from settings file
    logging.info("Reading table settings...")

    if not Path(_SETTINGS_FILE).is_file():
        logging.warning(
            "File '%s' does not exist. Skipping CDC DAG generation.",
            _SETTINGS_FILE)
        sys.exit()

    with open(_SETTINGS_FILE, encoding="utf-8") as settings_file:
        settings = yaml.load(settings_file, Loader=yaml.SafeLoader)

    if not settings:
        logging.warning("File '%s' is empty. Skipping CDC DAG generation.",
                        _SETTINGS_FILE)
        sys.exit()

    # TODO: Check settings file schema.

    if not "raw_to_cdc_tables" in settings:
        logging.warning(
            "File '%s' is missing property `raw_to_cdc_tables`. "
            "Skipping CDC DAG generation.", _SETTINGS_FILE)
        sys.exit()

    logging.info("Processing tables...")

    bq_client = cortex_bq_client.CortexBQClient()

    table_settings = settings["raw_to_cdc_tables"]
    for table_setting in table_settings:
        process_table(bq_client, table_setting, project_id, raw_dataset,
                      cdc_dataset, load_test_data)

    logging.info("Done generating CDC tables and DAGs.")


if __name__ == "__main__":
    main()
