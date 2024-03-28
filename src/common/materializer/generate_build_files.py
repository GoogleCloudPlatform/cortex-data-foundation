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
Generates GCP Cloud Build files that will create BQ objects (tables, views etc)
in the given dataset based on settings file.
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
from common.py_libs import jinja
from common.py_libs import k9_deployer

from google.cloud import bigquery

# NOTE: All paths here are relative to the root directory, unless specified
# otherwise.

# Directory this file is executed from.
_CWD = Path.cwd()

# Directory where this file resides.
_THIS_DIR = Path(__file__).resolve().parent

# Directory where all generated files will be created.
_GENERATED_FILES_PARENT_DIR = Path(_CWD, "generated_materializer_build_files")

# Template containing a build step that will create one bq object from a file.
_CLOUDBUILD_TEMPLATE_DIR = Path(_THIS_DIR, "templates")
_CLOUDBUILD_TEMPLATE_FILE = "cloudbuild_materializer.yaml.jinja"

#TODO: We should rename "Module" to "Data Source" for all variables
# (following applicable case convention) to align with of external naming.

# All supported Cortex modules
_CORTEX_MODULES = [
    "SAP", "SFDC", "GoogleAds", "CM360", "TikTok", "Meta", "SFMC", "k9"
]

# All supported Marketing modules
_MARKETING_MODULES = ["GoogleAds", "CM360", "TikTok", "Meta", "SFMC"]

# All supported target dataset types
_CORTEX_DATASET_TYPES_LOWER = ["cdc", "reporting", "processing"]


def _create_tgt_dataset(full_dataset_name: str, location: str) -> None:
    """Creates target (CDC/Reporting etc.) BQ target dataset if needed."""
    logging.info("Creating '%s' dataset if needed...", full_dataset_name)
    client = bigquery.Client()
    ds = bigquery.Dataset(full_dataset_name)
    ds.location = location
    ds = client.create_dataset(ds, exists_ok=True, timeout=30)


def _create_jinja_data_file(config_dict: dict, generated_files_dir) -> None:
    """Generates jinja data file that will be used to create BQ objects."""

    jinja_file_name = "bq_sql_jinja_data" + ".json"
    jinja_data_file = Path(generated_files_dir, jinja_file_name)

    logging.info("Creating jinja data file '%s'...", jinja_data_file)

    with open(jinja_data_file, "w", encoding="utf-8") as f:
        jinja_data_file_dict = jinja.initialize_jinja_from_config(config_dict)
        f.write(json.dumps(jinja_data_file_dict, indent=4))

    logging.info("Jinja template data file '%s' created successfully.",
                 jinja_data_file)


def _process_bq_object_settings(global_settings: dict,
                                bq_objects_settings: list,
                                wait_for_prev_step: bool) -> list:
    """Creates a list containing build file settings from bq_object settings."""
    build_file_settings = []
    for bq_object_setting in bq_objects_settings:
        logging.debug("bq_object_setting = '%s'", bq_object_setting)

        object_type = bq_object_setting.get("type")

        if object_type in ["table", "view", "script"]:
            sql_file = bq_object_setting.get("sql_file")
            build_file_settings.append({
                "sql_file": sql_file,
                "wait_for_prev_step": wait_for_prev_step,
                # NOTE: Using json.dumps to handle double quotes in settings
                "bq_object_setting": json.dumps(bq_object_setting)
            })
        elif object_type == "k9_dawg":
            k9_id = bq_object_setting.get("k9_id")
            manifest = global_settings["k9_manifest"]
            try:
                k9_definition = manifest[k9_id]
            except Exception as e:
                raise ValueError(f"K9 {k9_id} not found in "
                                 "local manifest.") from e

            k9_settings = bq_object_setting.get("parameters", {})

            build_file_settings.append({
                "k9_id": k9_id,
                "wait_for_prev_step": wait_for_prev_step,
                # NOTE: Using json.dumps to handle double quotes in settings
                "k9_definition": json.dumps(k9_definition),
                "k9_settings": json.dumps(k9_settings)
            })
    return build_file_settings

#TODO: Refactor so it takes only global settings and bq_obj_settings dicts,
# not a full list of individual parameters.

def _create_build_files(global_settings: dict, bq_obj_settings: dict,
                        module_name: str, tgt_dataset_name: str,
                        tgt_dataset_type: str,
                        generated_files_dir: Path) -> None:
    """
    Generates cloud build files that will create target artifacts such as BQ
    tables / views, K9 DAGs, etc.
    """

    logging.info("Creating build files that will create bq objects...")

    logging.debug("module_name = '%s'", module_name)

    build_files_master_list = []

    config_dict = global_settings["config_dict"]
    independent_objects_settings = bq_obj_settings.get("bq_independent_objects")
    dependent_objects_settings = bq_obj_settings.get("bq_dependent_objects")

    # Process independent tables first, accounting for Turbo Mode.
    if independent_objects_settings:
        wait_for_prev_step = not config_dict["turboMode"]
        build_files_master_list.extend(
            _process_bq_object_settings(global_settings,
                                        independent_objects_settings,
                                        wait_for_prev_step))

    # Process dependent tables.
    if dependent_objects_settings:
        wait_for_prev_step = True
        build_files_master_list.extend(
            _process_bq_object_settings(global_settings,
                                        dependent_objects_settings,
                                        wait_for_prev_step))

    # Since cloud build limits 100 steps in one build file, let's split
    # our list such that each list contains at the most 95 entries, as we will
    # have some extra steps too.
    # Each of these lists will be used to generate one "big" build
    # file that will create target BQ objects one object at a time.
    # We limit it to a single step per file when turboMode is false.
    # This emulates the original pre-Turbo behavior.
    max_build_steps = 95 if config_dict.get("turboMode", True) else 1
    build_files_lists = [
        build_files_master_list[x:x + max_build_steps]
        for x in range(0, len(build_files_master_list), max_build_steps)
    ]

    # Generate one build file for each list, using Jinja.
    environment = Environment(loader=FileSystemLoader(_CLOUDBUILD_TEMPLATE_DIR))
    build_file_template = environment.get_template(_CLOUDBUILD_TEMPLATE_FILE)
    build_file_counter = 0
    for build_files_list in build_files_lists:
        build_file_counter += 1
        build_file_text = build_file_template.render({
            "module_name": module_name,
            "target_dataset_type": tgt_dataset_type,
            "target_dataset_name": tgt_dataset_name,
            "load_test_data": config_dict["testData"],
            "config_file": global_settings["config_file"],
            "build_files_list": build_files_list
        })

        build_file_num = f"{build_file_counter:03d}"
        build_file_name = f"cloudbuild.materializer.{tgt_dataset_name}.{build_file_num}.yaml"  #pylint: disable=line-too-long
        build_file = Path(generated_files_dir, build_file_name)

        logging.debug("Creating build file : '%s'", build_file)
        logging.debug("Build File Text =  '%s'", build_file_text)

        with open(build_file, "w", encoding="utf-8") as bf:
            bf.write(build_file_text)


def _get_materializer_settings(materializer_settings_file: str) -> dict:
    """Parses settings file and returns settings dict after validations."""
    logging.info("Loading Materializer settings file '%s'...",
                 materializer_settings_file)

    with open(materializer_settings_file,
              encoding="utf-8") as materializer_settings_fp:
        materializer_settings = yaml.safe_load(materializer_settings_fp)

    if materializer_settings is None:
        raise ValueError(f"🛑 '{materializer_settings_file}' file is empty.")

    logging.debug("Materializer settings for this module : \n%s",
                  json.dumps(materializer_settings, indent=4))

    # Validate bq object settings.
    # Since this setting file contains two separate bq table setting sections,
    # we validate both of them.
    independent_tables_settings = materializer_settings.get(
        "bq_independent_objects")
    dependent_tables_settings = materializer_settings.get(
        "bq_dependent_objects")

    # At least one of the two sections needs to be present.
    if (independent_tables_settings is None and
            dependent_tables_settings is None):
        raise ValueError(
            "🛑 'bq_independent_objects' and 'bq_dependent_setting' both "
            "can not be empty.")

    for settings in [independent_tables_settings, dependent_tables_settings]:
        if settings:
            bq_materializer.validate_bq_materializer_settings(settings)

    return materializer_settings


def _parse_args() -> tuple[str, str, str, str, dict]:
    """Parses arguments and sets up logging."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--module_name",
        type=str,
        required=True,
        help="Module for which output dataset is created. Required.")
    parser.add_argument(
        "--target_dataset_type",
        type=str,
        required=False,
        help=("Target dataset type (CDC/Reporting). Must correspond to "
              "what is available under \"dataset\" section in config.json. "
              "Default is \"Reporting\"."),
        default="Reporting")
    parser.add_argument(
        "--config_file",
        type=str,
        required=True,
        help=("Config file containing deployment configurations, "
              "with relative path. Required."))
    parser.add_argument(
        "--materializer_settings_file",
        type=str,
        required=True,
        help="Materializer settings file, with relative path. Required.")
    parser.add_argument(
        "--k9_manifest_file",
        type=str,
        required=False,
        default=None,
        help="K9 Manifest file, with relative path. Required only if "
             "local K9 is being utilized.")
    parser.add_argument(
        "--debug",
        default=False,
        action="store_true",
        help="Flag to set log level to DEBUG. Default is WARNING")

    args = parser.parse_args()

    enable_debug = args.debug
    logging.basicConfig(level=logging.DEBUG if enable_debug else logging.INFO)

    module_name = args.module_name
    config_file = args.config_file
    materializer_settings_file = args.materializer_settings_file
    target_dataset_type = args.target_dataset_type
    k9_manifest_file = args.k9_manifest_file

    logging.info("Arguments:")
    logging.info("  module_name = %s", module_name)
    logging.info("  config_file = %s", config_file)
    logging.info("  target_dataset_type = %s", target_dataset_type)
    logging.info("  materializer_settings_file = %s",
                 materializer_settings_file)
    logging.info("  k9_manifest_file = %s", k9_manifest_file)
    logging.info("  debug = %s", enable_debug)

    # validate arguments.
    # Module Name
    if module_name not in _CORTEX_MODULES:
        raise ValueError(
            f"🛑 Invalid module name '{module_name}'. Supported modules are : "
            f"'{_CORTEX_MODULES}' (case sensitive).")

    # Target Dataset Type
    lower_tgt_dataset_type = target_dataset_type.lower().replace(" ", "_")

    if lower_tgt_dataset_type not in _CORTEX_DATASET_TYPES_LOWER:
        raise ValueError(
            f"🛑 Invalid target dataset type '{target_dataset_type}'. "
            f"Supported types are : '{_CORTEX_DATASET_TYPES_LOWER}' (case "
            "insensitive).")

    if module_name == "k9" and lower_tgt_dataset_type == "cdc":
        raise ValueError(
            "🛑 Module k9 does not support target dataset type 'cdc'.")

    if lower_tgt_dataset_type == "processing" and module_name != "k9":
        raise ValueError(
            f"🛑 Module '{module_name}' does not support target dataset type "
            "'processing'.")

    # Config file
    if not Path(config_file).is_file():
        raise FileNotFoundError(
            f"🛑 Config file '{config_file}' does not exist.")

    # Settings file
    if not Path(materializer_settings_file).is_file():
        raise FileNotFoundError(
            f"🛑 {target_dataset_type} Settings file "
            f"'{materializer_settings_file}' does not exist.")

    # K9 Manifest file
    try:
        k9_manifest = (
            k9_deployer.load_k9s_manifest(k9_manifest_file)
            if k9_manifest_file
            else {})
    except Exception as e:
        raise FileNotFoundError(f"The specified K9 manifest {k9_manifest_file} "
                                "does not exist.") from e

    return (module_name, target_dataset_type, config_file,
            materializer_settings_file, k9_manifest)



def main():

    # Parse and validate arguments.
    (module_name, tgt_dataset_type, config_file,
     materializer_settings_file, k9_manifest) = _parse_args()

    logging.info("Generating %s Build files....", tgt_dataset_type)

    # Load and validate configs in config file
    config_dict = configs.load_config_file(config_file)

    # Load and validate Materializer settings
    materializer_settings = _get_materializer_settings(
        materializer_settings_file)

    # Create output directory.
    generated_files_dir = Path(_GENERATED_FILES_PARENT_DIR, module_name)
    if generated_files_dir.exists():
        logging.debug("Removing existing generated files directory '%s'....",
                      generated_files_dir)
        shutil.rmtree(generated_files_dir)
    logging.debug("Creating directory '%s' to store generated files....",
                  generated_files_dir)
    Path(generated_files_dir).mkdir(parents=True)

    lower_tgt_dataset_type = tgt_dataset_type.lower().replace(" ", "_")

    if module_name in _MARKETING_MODULES:
        # Marketing modules are nested under "marketing".
        tgt_dataset = config_dict["marketing"][module_name]["datasets"][
            lower_tgt_dataset_type]
    else:
        tgt_dataset = config_dict[module_name]["datasets"][
            lower_tgt_dataset_type]

    tgt_dataset_full_name = config_dict["projectIdTarget"] + "." + tgt_dataset

    # Create jinja template substitution file. This file is needed when running
    # individual BQ table creation SQL build file later in the process.
    _create_jinja_data_file(config_dict, generated_files_dir)

    global_settings={
        "config_file": config_file,
        "config_dict": config_dict,
        "k9_manifest": k9_manifest
    }

    # Create build files.
    _create_build_files(global_settings, materializer_settings, module_name,
                        tgt_dataset_full_name, tgt_dataset_type,
                        generated_files_dir)

    # Create target dataset if not present.
    _create_tgt_dataset(tgt_dataset_full_name, config_dict["location"])

    logging.info("Finished generating Cloud Build files for %s for %s.",
                 tgt_dataset_type, module_name)


if __name__ == "__main__":
    main()
