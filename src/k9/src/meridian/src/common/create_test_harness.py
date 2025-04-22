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
"""
Test harness creation tool.
"""

import argparse
import logging
import pathlib
import sys
import typing

# Make sure common modules are in Python path
sys.path.append(str(pathlib.Path(__file__).parent.parent))

# pylint:disable=wrong-import-position
from common.py_libs import constants
from common.py_libs import cortex_bq_client
from common.py_libs.bq_helper import get_table_list
from common.py_libs.configs import load_config_file
from common.py_libs.logging import initialize_console_logging
from common.py_libs.test_harness import load_dataset_test_data

_DEFAULT_CONFIG = "config/config.json"  # data foundation config


def main(args: typing.Sequence[str]):
    parser = argparse.ArgumentParser(description="Test Harness Loader")
    parser.add_argument("--config-file",
                        type=str,
                        required=False,
                        default=_DEFAULT_CONFIG)
    parser.add_argument(
        "--debug",
        help="Debugging mode.",
        action="store_true",
        default=False,
        required=False,
    )
    parser.add_argument("--workload", type=str, required=True)
    parser.add_argument("--dataset", type=str, required=True)
    params = parser.parse_args(args)
    initialize_console_logging(params.debug)
    config = load_config_file(params.config_file)

    test_data = config.get("testData", False)
    test_harness_version = config.get("testHarnessVersion",
                                      constants.TEST_HARNESS_VERSION)
    if not test_data:
        logging.error("testData in %s is false. Aborting.", params.config_file)
        return 1
    test_harness_project = config.get("testDataProject", "")
    if test_harness_project == "":
        logging.error("testDataProject in %s is empty. Aborting.",
                      params.config_file)
        return 1
    source_project = config["projectIdSource"]
    target_project = config["projectIdTarget"]
    location = config["location"]
    workload_path = params.workload
    dataset_type = params.dataset

    workload_components = workload_path.split(".")
    current_workload = config[workload_components[0]]
    for component in workload_components[1:]:
        current_workload = current_workload[component]

    if workload_path == "marketing.GA4" and dataset_type == "cdc":
        dataset_name = current_workload["datasets"][dataset_type][0].get("name")
    else:
        dataset_name = current_workload["datasets"][dataset_type]

    project = source_project if dataset_type != "reporting" else target_project

    client = cortex_bq_client.CortexBQClient(
        project=project,
        location=location)

    if get_table_list(client, project, dataset_name):
        logging.warning(
            "Dataset '%s.%s' already contains tables. Skipping test data "
            "deployment.", project, dataset_name)
        return

    logging.info(
        ("Loading test data for workload `%s`, dataset `%s` "
        "(%s.%s)."), workload_path, dataset_type, project, dataset_name)

    load_dataset_test_data(client,
                           test_harness_project,
                           workload_path,
                           dataset_type,
                           dataset_name,
                           project,
                           location,
                           test_harness_version)
    logging.info("Test data for dataset `%s.%s` has been loaded.",
                 project, dataset_name)


################################################################################
if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
