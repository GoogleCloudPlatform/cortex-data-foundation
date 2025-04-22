# Copyright 2025 Google LLC
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
"""Executable that creates task dependent DAGs.

This must be executed after `generate_build_files.py` and the resulting build
files are executed. It relies on files and BQ objects that are created by those
processes.

Usage example:
  $ python generate_dependent_dags.py \
    --module_name "${MODULE_NAME}" \
    --target_dataset_type "reporting" \
    --config_file "config.json" \
    --materializer_settings_file "reporting_settings.yaml"
"""

import argparse
import logging
import pathlib
import sys
from typing import Sequence

from common.materializer import dependent_dags
from common.materializer import generate_assets
from common.py_libs import configs
from common.py_libs import constants


def main(args: Sequence[str]):
    parser = argparse.ArgumentParser(description="Generate task dependent DAG")
    parser.add_argument("--module_name",
                        type=str,
                        required=True,
                        help="Name of module e.g. 'cm360', 'sap'")
    parser.add_argument("--target_dataset_type",
                        type=str,
                        required=True,
                        help="Type of dataset, must be 'reporting'")
    parser.add_argument("--config_file",
                        type=str,
                        required=True,
                        help="Relative path to file containing deployment "
                        "configurations.")
    parser.add_argument("--materializer_settings_file",
                        type=str,
                        required=True,
                        help="File path to materializer settings")
    parser.add_argument("--debug",
                        action="store_true",
                        default=False,
                        required=False)
    params = parser.parse_args(args)

    logging.basicConfig(format="%(asctime)s | %(levelname)s | %(message)s",
                        level=logging.DEBUG if params.debug else logging.INFO)

    config_dict = configs.load_config_file(params.config_file)

    td_settings = generate_assets.get_enabled_task_dep_settings_file(
        pathlib.Path(params.materializer_settings_file), config_dict)
    if td_settings:
        materializer_settings_file = str(td_settings)
        logging.info(
            "Task dependencies are enabled and %s exists. Using "
            "task dependent settings.", td_settings.name)
    else:
        logging.info(
            "Task dependencies aren't enabled or task dependent version of "
            "%s doesn't exist. ", params.materializer_settings_file)
        return

    logging.info("Generating DAG files with task dependencies.")

    materializer_settings = generate_assets.get_materializer_settings(
        materializer_settings_file)
    task_dep_objs = dependent_dags.get_task_deps(materializer_settings)

    if not task_dep_objs:
        logging.info("No task dependent DAGs were found.")
        return

    # Parse configs for relevant options.
    allow_telemetry = config_dict.get("allowTelemetry", True)
    lower_module_name = params.module_name.lower()
    lower_tgt_dataset_type = params.target_dataset_type.lower().replace(
        " ", "_")
    location = config_dict["location"]
    # Note that the generated DAG files are under a lowercase module_name dir
    # whereas the generated build files are not. This is to match the current
    # implementation for non-task dependent DAGs.
    generated_dag_files_dir = (generate_assets.GENERATED_DAG_DIR_NAME /
                               lower_module_name / lower_tgt_dataset_type /
                               "task_dep_dags")
    generated_build_files_dir = (generate_assets.GENERATED_BUILD_DIR_NAME /
                                 params.module_name)
    jinja_data_file = (generate_assets.GENERATED_BUILD_DIR_NAME /
                       params.module_name /
                       generate_assets.JINJA_DATA_FILE_NAME)

    # Marketing modules are nested inconsistently compared to other sources.
    if params.module_name in constants.MARKETING_MODULES:
        tgt_dataset = config_dict["marketing"][
            params.module_name]["datasets"][lower_tgt_dataset_type]
    else:
        tgt_dataset = config_dict[
            params.module_name]["datasets"][lower_tgt_dataset_type]
    tgt_dataset_full_name = config_dict["projectIdTarget"] + "." + tgt_dataset

    if not (generated_build_files_dir.exists() and jinja_data_file.exists()):
        raise RuntimeError(
            "Generated build files and jinja data must already exist.")

    dep_dag_generator = dependent_dags.DependentDagGenerator(
        lower_module_name, tgt_dataset_full_name, lower_tgt_dataset_type,
        allow_telemetry, location, generated_dag_files_dir, jinja_data_file)

    _ = dep_dag_generator.create_dep_dags(task_dep_objs)
    logging.info("Completed generating DAG files with task dependencies.")


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
