# Copyright 2023 Google LLC
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
"""Localized K9 deployer.

Deploys one specific K9 dawg based on the input parameters.
"""

import argparse
import json
import logging
from pathlib import Path
import os
import sys
import typing

from common.py_libs import k9_deployer

def main(args: typing.Sequence[str]) -> int:
    """main function

    Args:
        args (typing.Sequence[str]): command line arguments.

    Returns:
        int: return code (0 - success).
    """
    parser = argparse.ArgumentParser(description="K9 Deployer")
    parser.add_argument(
        "--config_file",
        type=str,
        required=True,
        help="Path to config.json that correspond to the current deployment.")
    parser.add_argument(
        "--data_source",
        type=str,
        required=True,
        help=("Data Source where the K9 is to be deployed. Required. Example: "
              "'sap', 'marketing.cm360'. Case sensitive."))
    parser.add_argument(
        "--dataset_type",
        type=str,
        required=True,
        help=("Dataset type as defined in config.json. Example: 'cdc', "
              "'reporting'. Case sensitive."))
    parser.add_argument(
        "--k9_definition",
        type=str,
        required=True,
        help="Definitions for the current K9 from Manifest, in JSON.")
    parser.add_argument(
        "--k9_setting",
        type=str,
        required=False,
        help=("K9 Setting dictionary in JSON - containing value corresponding "
              "to the entry in the Materializer settings file for the given "
              "K9."))
    parser.add_argument(
        "--log_bucket",
        type=str,
        required=True,
        help="GCS bucket for any deployment logs.")
    parser.add_argument(
        "--debug",
        help="Debugging mode.",
        action="store_true",
        default=False,
        required=False,
    )
    params = parser.parse_args(args)

    logging.basicConfig(
        format="%(asctime)s | %(levelname)s | %(message)s",
        level=logging.DEBUG if params.debug else logging.INFO,
    )

    logging.info("Arguments:")
    logging.info("  config_file = %s", params.config_file)
    logging.info("  data_source = %s", params.data_source)
    logging.info("  dataset_type = %s", params.dataset_type)
    logging.info("  k9_definition = %s", params.k9_definition)
    logging.info("  k9_setting = %s", params.k9_setting)
    logging.info("  log_bucket = %s", params.log_bucket)
    logging.info("  debug = %s", params.debug)

    k9_root_path = Path(os.getcwd())
    config_file = str(Path(params.config_file).absolute())
    logs_bucket = params.log_bucket
    data_source = params.data_source
    dataset_type = params.dataset_type.lower()

    try:
        k9_definition = json.loads(params.k9_definition)
    except Exception as e:
        raise ValueError("🛑 Failed to read provided K9 Definition. "
                         f"Error: {e}.") from e
    try:
        k9_settings = json.loads(params.k9_setting)
    except Exception as e:
        raise ValueError("🛑 Failed to read provided K9 Setting. "
                         f"Error: {e}.") from e

    k9_deployer.deploy_k9(k9_definition, k9_root_path, config_file,
                          logs_bucket, data_source, dataset_type, k9_settings)

    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
