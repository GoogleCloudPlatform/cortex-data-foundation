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
Processes and validates Oracle EBS config.json.
"""

import logging
from typing import Union

from common.py_libs import resource_validation_helper

def validate(cfg: dict) -> Union[dict, None]:
    """Validates and processes configuration.

    Args:
        cfg (dict): Config dictionary.

    Returns:
        dict: Processed config dictionary in case of successful validation
        None: In case of validation failure
    """

    if not cfg.get("deployOracleEBS"):
        logging.info("OracleEBS is not being deployed. Skipping validation.")
        return cfg

    logging.info("Validating OracleEBS configuration.")

    oracle_ebs = cfg.get("OracleEBS")
    if not oracle_ebs:
        logging.error("🛑 Missing 'OracleEBS' values in the config file. 🛑")
        return None

    if not oracle_ebs.get("itemCategorySetIds"):
        logging.error("🛑 Missing 'OracleEBS/itemCategorySetIds' values "
                      "in the config file. 🛑")
        return None

    if not oracle_ebs.get("currencyConversionType"):
        logging.error("🛑 Missing 'OracleEBS/currencyConversionType' values "
                      "in the config file. 🛑")
        return None

    if not oracle_ebs.get("currencyConversionTargets"):
        logging.error("🛑 Missing 'OracleEBS/currencyConversionTargets' values "
                      "in the config file. 🛑")
        return None

    if not oracle_ebs.get("languages"):
        logging.error("🛑 Missing 'OracleEBS/languages' values "
                      "in the config file. 🛑")
        return None

    datasets = oracle_ebs.get("datasets")
    if not datasets:
        logging.error("🛑 Missing 'OracleEBS/datasets' values "
                      "in the config file. 🛑")
        return None

    cdc = datasets.get("cdc")
    if not cdc:
        logging.error("🛑 Missing 'OracleEBS/datasets/cdc' values "
                      "in the config file. 🛑")
        return None

    reporting = datasets.get("reporting", "REPORTING_OracleEBS")
    # Setting the value back to dict regardless of original presence
    datasets["reporting"] = reporting

    source = cfg["projectIdSource"]
    target = cfg["projectIdTarget"]
    location = cfg["location"]
    datasets = [
        resource_validation_helper.DatasetConstraints(
            f"{source}.{cdc}",
            True, True, location),
        resource_validation_helper.DatasetConstraints(
            f"{target}.{reporting}",
            False, True, location)
        ]
    if not resource_validation_helper.validate_resources([], datasets):
        return None

    logging.info("✅ OracleEBS configuration is good.")

    return cfg
