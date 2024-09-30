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
Generates views to project Salesforce RAW dataset table fields
to CDC according to the defined CDC schema.
"""

import csv
import json
import logging
from pathlib import Path
import sys
import typing
import yaml

from common.py_libs import cortex_bq_client
from common.py_libs.bq_helper import execute_sql_file, table_exists
from common.py_libs.configs import load_config_file
from common.py_libs.dag_generator import generate_file_from_template

# NOTE: All paths here are relative to the root directory, unless specified
# otherwise.

_THIS_DIR = Path(__file__).resolve().parent

# Config file containing various parameters.
_CONFIG_FILE = Path(_THIS_DIR, "../../config/config.json")

# Settings file containing tables to be copied from SFDC.
_SETTINGS_FILE = Path(_THIS_DIR, "../../config/ingestion_settings.yaml")

# Directory containing table schema mapping files
_SCHEMA_MAPPING_DIR = Path(_THIS_DIR, "../table_schema")

_GENERATED_FILE_PREFIX = "sfdc_raw_to_cdc_"
_TEMPLATE_SQL_NAME = "view_template"

# Directory under which all the generated sql files will be created.
# Note that for all SFDC CDC views, SQL generated are temporary and will not
# be copied to the output bucket.
_GENERATED_VIEW_SQL_DIR = "generated_view_sql"

# Directory containing various template files.
_TEMPLATE_DIR = Path(_THIS_DIR, "templates")
# Directory containing various template files.
_SQL_TEMPLATE_DIR = Path(_TEMPLATE_DIR, "sql")


def process_table(bq_client, table_setting, project_id, raw_dataset,
                  cdc_dataset):
    """For a given table config, creates required view SQL,
    and if the raw table exists, executes the SQL.
    """

    base_table = table_setting["base_table"].lower()
    raw_table = table_setting["raw_table"]

    source_table = project_id + "." + raw_dataset + "." + raw_table
    target_view = project_id + "." + cdc_dataset + "." + base_table
    raw_exists = table_exists(bq_client, source_table)

    if not raw_exists:
        logging.error(("Source raw table `%s` doesn't exist! \n"
                       "CDC view cannot be created."), source_table)
        raise SystemExit("⛔️ Failed to deploy CDC views.")

    schema_file = Path(_SCHEMA_MAPPING_DIR, f"{base_table}.csv").absolute()

    logging.info("__ Processing view '%s' __", base_table)

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

    # SQL file generation
    #########################
    sql_template_file_name = (_GENERATED_FILE_PREFIX + _TEMPLATE_SQL_NAME +
                              ".sql")
    sql_file_name = (base_table.replace(".", "_") +
                     "_view.sql")
    sql_template_file = Path(_SQL_TEMPLATE_DIR, sql_template_file_name)
    output_sql_file = Path(_GENERATED_VIEW_SQL_DIR, sql_file_name)

    field_assignments = [
        f"`{f[0]}` AS `{f[1][0]}`" for f in sfdc_to_bq_field_map.items()
    ]

    sql_subs = {
        "source_table": source_table,
        "target_view": target_view,
        "field_assignments": ",\n    ".join(field_assignments)
    }

    generate_file_from_template(sql_template_file, output_sql_file, **sql_subs)
    logging.info("Generated CDC view SQL file.")

    try:
        if table_exists(bq_client, target_view):
            logging.warning(("⚠️ View or table %s already exists. "
                             "Skipping it."), target_view)
        else:
            logging.info("Creating view %s", target_view)
            execute_sql_file(bq_client, output_sql_file)
            logging.info("✅ Created CDC view %s.", target_view)
    except Exception as e:
        logging.error("Failed to create CDC view '%s'.\n"
                      "ERROR: %s", target_view, str(e))
        raise SystemExit(
            "⛔️ Failed to deploy CDC views. Please check the logs.") from e

    logging.info("__ View '%s' processed.__", base_table)


def main():
    logging.basicConfig(level=logging.INFO)
    logging.info("Generating CDC views...")

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

    logging.info(
        "\n---------------------------------------\n"
        "Using the following parameters from config:\n"
        "  source_project_id = %s \n"
        "  raw_dataset = %s \n"
        "  cdc_dataset = %s \n"
        "---------------------------------------\n", project_id, raw_dataset,
        cdc_dataset)

    Path(_GENERATED_VIEW_SQL_DIR).mkdir(exist_ok=True, parents=True)

    # Process tables based on table settings from settings file
    logging.info("Reading table settings...")

    if not Path(_SETTINGS_FILE).is_file():
        logging.warning(
            "File '%s' does not exist. Skipping CDC view generation.",
            _SETTINGS_FILE)
        sys.exit()

    with open(_SETTINGS_FILE, encoding="utf-8") as settings_file:
        settings = yaml.load(settings_file, Loader=yaml.SafeLoader)

    if not settings:
        logging.warning("File '%s' is empty. Skipping CDC view generation.",
                        _SETTINGS_FILE)
        sys.exit()

    # TODO: Check settings file schema.

    if not "raw_to_cdc_tables" in settings:
        logging.warning(
            "File '%s' is missing property `raw_to_cdc_tables`. "
            "Skipping CDC view generation.", _SETTINGS_FILE)
        sys.exit()
    table_settings = settings["raw_to_cdc_tables"]

    bq_client = cortex_bq_client.CortexBQClient()

    logging.info("Processing tables...")

    for table_setting in table_settings:
        process_table(bq_client, table_setting, project_id, raw_dataset,
                      cdc_dataset)

    logging.info("Done generating CDC views.")


if __name__ == "__main__":
    main()
