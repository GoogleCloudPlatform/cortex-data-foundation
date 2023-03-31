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
Generates individual DAG and related files needed to copy/move Salesforce data
from RAW dataset to CDC dataset.
"""

import argparse
import json
import logging
import shutil
import yaml

from jinja2 import Environment, FileSystemLoader
from pathlib import Path

from common.py_libs import bq_materializer
from common.py_libs import configs

from google.cloud import bigquery

# NOTE: All paths here are relative to the root directory, unless specified
# otherwise.

# Directory this file is executed from.
_CWD = Path.cwd()

# Directory where this file resides.
_THIS_DIR = Path(__file__).resolve().parent

# Directory where all generated files will be created.
_GENERATED_FILES_DIR = Path(_CWD, "generated_reporting_build_files")

# Template containing a build step that will create one bq object from a file.
_CLOUDBUILD_TEMPLATE_DIR = Path(_THIS_DIR, "templates")
_CLOUDBUILD_TEMPLATE_FILE = "cloudbuild_create_bq_objects.yaml.jinja"

# All supported functional modules
_CORTEX_MODULES = ["SAP", "SFDC", "IMS"]


def _create_reporting_dataset(full_dataset_name: str, location: str) -> None:
    """Creates BQ reporting dataset if needed."""
    logging.info("Creating '%s' dataset if needed...", full_dataset_name)
    client = bigquery.Client()
    ds = bigquery.Dataset(full_dataset_name)
    ds.location = location
    ds = client.create_dataset(ds, exists_ok=True, timeout=30)


def _create_jinja_data_file(module_name: str, config_dict: dict) -> None:
    """Generates jinja data file that will be used to create BQ objects."""

    jinja_file_name = "bq_sql_jinja_data" + "_" + module_name + ".json"
    jinja_module_data_file = Path(_GENERATED_FILES_DIR, jinja_file_name)

    logging.info("Creating jinja data file '%s'...", jinja_module_data_file)

    with open(jinja_module_data_file, "w", encoding="utf-8") as f:
        jinja_data_file_dict = {
            "project_id_src": config_dict["projectIdSource"],
            "project_id_tgt": config_dict["projectIdTarget"],
        }
        if module_name == "SFDC":
            jinja_data_file_dict.update({
                "dataset_raw_landing_sfdc":
                    config_dict["SFDC"]["datasets"]["raw"],
                "dataset_cdc_processed_sfdc":
                    config_dict["SFDC"]["datasets"]["cdc"],
                "dataset_reporting_tgt_sfdc":
                    config_dict["SFDC"]["datasets"]["reporting"],
                "currencies":
                    ",".join(f"'{currency}'"
                             for currency in config_dict["currencies"]),
                "languages":
                    ",".join(
                        f"'{language}'" for language in config_dict["languages"]
                    )
            })
        if module_name == "SAP":
            jinja_data_file_dict.update({
                # raw datasets
                "dataset_raw_landing":
                    config_dict["SAP"]["datasets"]["raw"],
                "dataset_raw_landing_ecc":
                    config_dict["SAP"]["datasets"]["rawECC"],
                "dataset_raw_landing_s4":
                    config_dict["SAP"]["datasets"]["rawS4"],
                # cdc datasets
                "dataset_cdc_processed":
                    config_dict["SAP"]["datasets"]["cdc"],
                "dataset_cdc_processed_ecc":
                    config_dict["SAP"]["datasets"]["cdcECC"],
                "dataset_cdc_processed_s4":
                    config_dict["SAP"]["datasets"]["cdcS4"],
                # reporting datasets
                "dataset_reporting_tgt":
                    config_dict["SAP"]["datasets"]["reporting"],
                # mandt
                "mandt":
                    config_dict["SAP"]["mandt"],
                "mandt_ecc":
                    config_dict["SAP"]["mandtECC"],
                "mandt_s4":
                    config_dict["SAP"]["mandtS4"],
                # Misc
                "sql_flavor":
                    config_dict["SAP"]["SQLFlavor"],
                "sql_flavour":
                    config_dict["SAP"]["SQLFlavor"],
                # TODO: Update SAP sql to use currency jinja variable in a
                # readable way - e.g. "IN {{ currencies }}"
                "currency":
                    "IN (" +
                    ",".join(f"'{currency}'"
                             for currency in config_dict["currencies"]) + ")",
                "language":
                    "IN (" +
                    ",".join(f"'{language}'"
                             for language in config_dict["languages"]) + ")"
            })
        else:
            raise ValueError(
                f"Module '{module_name}' is not yet supported for jinja yet.")

        f.write(json.dumps(jinja_data_file_dict, indent=4))

    logging.info("Jinja template data file '%s' created successfully.",
                 jinja_module_data_file)


def _process_reporting_settings(bq_objects_settings: list,
                                wait_for_prev_step: bool) -> list:
    """Creates a list containing build file settings from bq_object settings."""
    build_file_settings = []
    for bq_object_setting in bq_objects_settings:
        logging.debug("bq_object_setting = '%s'", bq_object_setting)
        sql_file = bq_object_setting.get("sql_file")

        build_file_settings.append({
            "sql_file": sql_file,
            "wait_for_prev_step": wait_for_prev_step,
            # NOTE: Using json.dumps to handle double quotes in settings
            "bq_object_setting": json.dumps(bq_object_setting)
        })
    return build_file_settings


def _create_build_files(module_name: str, config_dict: dict,
                        reporting_settings: dict, reporting_dataset_name: str,
                        load_test_data: bool) -> None:
    """Generates cloud build files that will create reporting BQ tables."""

    logging.info("Creating build files that will create bq objects...")

    logging.debug("module_name = '%s'", module_name)

    build_files_master_list = []

    independent_objects_settings = reporting_settings.get(
        "bq_independent_objects")
    dependent_objects_settings = reporting_settings.get("bq_dependent_objects")

    # Process independent tables first, accounting for Turbo Mode.
    if independent_objects_settings:
        wait_for_prev_step = not config_dict["turboMode"]
        build_files_master_list.extend(
            _process_reporting_settings(independent_objects_settings,
                                        wait_for_prev_step))

    # Process dependent tables.
    if dependent_objects_settings:
        wait_for_prev_step = True
        build_files_master_list.extend(
            _process_reporting_settings(dependent_objects_settings,
                                        wait_for_prev_step))

    # Since cloud build limits 100 steps in one build file, let's split
    # our list such that each list contains at the most 95 entries, as we will
    # have some extra steps too.
    # Each of these lists will be used to generate one "big" build
    # file that will create Reporting BQ objects one object at a time.
    build_files_lists = [
        build_files_master_list[x:x + 95]
        for x in range(0, len(build_files_master_list), 95)
    ]

    # Generate one build file for each list, using Jinja.
    environment = Environment(loader=FileSystemLoader(_CLOUDBUILD_TEMPLATE_DIR))
    build_file_template = environment.get_template(_CLOUDBUILD_TEMPLATE_FILE)
    build_file_counter = 0
    for build_files_list in build_files_lists:
        build_file_counter += 1
        build_file_text = build_file_template.render({
            "module_name": module_name,
            "reporting_dataset_name": reporting_dataset_name,
            "load_test_data": load_test_data,
            "build_files_list": build_files_list
        })

        build_file_num = f"{build_file_counter:02d}"
        build_file = Path(
            _GENERATED_FILES_DIR,
            #pylint: disable=line-too-long
            f"cloudbuild.{module_name}.reporting.create_bq_objects.{build_file_num}.yaml"
        )

        logging.debug("Creating build file : '%s'", build_file)
        logging.debug("Build File Text =  '%s'", build_file_text)

        with open(build_file, "a", encoding="utf-8") as bf:
            bf.write(build_file_text)


def _get_reporting_settings(reporting_settings_file: str) -> dict:
    """Parses settings file and returns settings dict after validations."""
    logging.info("Loading report settings file '%s'...",
                 reporting_settings_file)

    with open(reporting_settings_file,
              encoding="utf-8") as reporting_settings_fp:
        reporting_settings = yaml.safe_load(reporting_settings_fp)

    if reporting_settings is None:
        raise ValueError("'reporting_settings' file is empty.")

    logging.debug("reporting settings for this module : \n%s",
                  json.dumps(reporting_settings, indent=4))

    # Validate reporting settings.
    # Since this setting file contains two separate materializer settings, we
    # validate both of them.
    independent_tables_settings = reporting_settings.get(
        "bq_independent_objects")
    dependent_tables_settings = reporting_settings.get("bq_dependent_objects")

    # At least one of the two sections needs to be present.
    if (independent_tables_settings is None and
            dependent_tables_settings is None):
        raise ValueError(
            "'bq_independent_objects' and 'bq_dependent_setting' both "
            "can not be empty.")

    for settings in [independent_tables_settings, dependent_tables_settings]:
        if settings:
            bq_materializer.validate_reporting_materializer_settings(settings)

    return reporting_settings


def _parse_args() -> tuple[str, str, str]:
    """Parses arguments and sets up logging."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--module_name",
        type=str,
        required=True,
        help="Module for which reporting dataset is created.'. Required.")
    parser.add_argument(
        "--config_file",
        type=str,
        required=True,
        help=("Config file containing deployment configurations, "
              "with relative path. Required."))
    parser.add_argument(
        "--reporting_settings_file",
        type=str,
        required=True,
        help="Reporting settings file, with relative path. Required.")
    parser.add_argument(
        "--debug",
        default=False,
        action="store_true",
        help="Flag to set log level to DEBUG. Default is WARNING")

    args = parser.parse_args()

    enable_debug = args.debug
    logging.basicConfig(level=logging.DEBUG if enable_debug else logging.INFO)

    module_name = args.module_name.upper()
    config_file = args.config_file
    reporting_settings_file = args.reporting_settings_file

    logging.info("Arguments:")
    logging.info("  module_name = %s", module_name)
    logging.info("  config_file = %s", config_file)
    logging.info("  reporting_settings_file = %s", reporting_settings_file)
    logging.info("  debug = %s", enable_debug)

    # validate arguments.
    # Module Name
    if module_name not in _CORTEX_MODULES:
        raise ValueError(
            f"Invalid module name '{module_name}'. Supported modules are : "
            f"'{_CORTEX_MODULES}'.")

    # Config file
    if not Path(config_file).is_file():
        raise FileNotFoundError(f"Config file '{config_file}' does not exist.")

    # Settings file
    if not Path(reporting_settings_file).is_file():
        raise FileNotFoundError(
            f"Reporting Settings file '{reporting_settings_file}' does not "
            "exist.")

    return (module_name, config_file, reporting_settings_file)


def main():

    # Parse and validate arguments.
    (module_name, config_file, reporting_settings_file) = _parse_args()

    logging.info("Generating Reporting Build files....")

    # Load and validate configs in config file
    config_dict = configs.load_config_file(config_file)

    # Load and validate reporting settings
    reporting_settings = _get_reporting_settings(reporting_settings_file)

    # Create output directory, but remove first if already exists.
    # This is important, because otherwise, we may keep on appending to files
    # created by an earlier run.
    if _GENERATED_FILES_DIR.exists():
        shutil.rmtree(_GENERATED_FILES_DIR)
    Path(_GENERATED_FILES_DIR).mkdir(exist_ok=True)

    # Get all the useful items from config file.
    reporting_dataset_full_name = (
        config_dict["projectIdTarget"] + "." +
        config_dict[module_name]["datasets"]["reporting"])
    location = config_dict["location"]
    load_test_data = config_dict["testData"]

    # Create jinja template substitution file. This file is needed when running
    # individual BQ table creation build file later in the process.
    _create_jinja_data_file(module_name, config_dict)

    # Create build files.
    _create_build_files(module_name, config_dict, reporting_settings,
                        reporting_dataset_full_name, load_test_data)

    # Create reporting dataset if not present.
    _create_reporting_dataset(reporting_dataset_full_name, location)

    logging.info("Finished generating Cloud Build files for reporting.")


if __name__ == "__main__":
    main()
