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
"""
Processes and validates SAP Reporting config.json.
"""

import logging
from typing import Union

from common.py_libs import resource_validation_helper


def validate(cfg: dict) -> Union[dict, None]:
    """Validates and processes configuration.

    Args:
        cfg (dict): Config dictionary.

    Returns:
        dict: Processed config dictionary.
    """

    if not cfg.get("deploySFDC", False):
        logging.info("SFDC is not being deployed. Skipping validation.")
        return cfg

    logging.info("Validating SFDC configuration.")

    sfdc = cfg.get("SFDC", None)
    if not sfdc:
        logging.error("🛑 Missing 'SFDC' values in the config file. 🛑")
        return None

    deploy_cdc = sfdc.get("deployCDC")
    if deploy_cdc is None:
        logging.error("🛑 Missing 'SFDC/deployCDC' values "
                      "in the config file. 🛑")
        return None

    datasets = sfdc.get("datasets")
    if not datasets:
        logging.error("🛑 Missing 'SFDC/datasets' values in the config file. 🛑")
        return None

    cfg["SFDC"]["createMappingViews"] = sfdc.get("createMappingViews", True)
    cfg["SFDC"]["datasets"]["cdc"] = datasets.get("cdc", "")
    if not cfg["SFDC"]["datasets"]["cdc"]:
        logging.error("🛑 Missing 'SFDC/datasets/cdc' values "
                      "in the config file. 🛑")
        return None
    cfg["SFDC"]["datasets"]["raw"] = datasets.get("raw", "")
    if not cfg["SFDC"]["datasets"]["raw"]:
        logging.error("🛑 Missing 'SFDC/datasets/raw' values "
                      "in the config file. 🛑")
        return None
    cfg["SFDC"]["datasets"]["reporting"] = datasets.get("reporting",
                                                        "REPORTING_SFDC")

    datasets = sfdc.get("datasets")
    source = cfg["projectIdSource"]
    target = cfg["projectIdTarget"]
    location = cfg["location"]
    datasets = [
        resource_validation_helper.DatasetConstraints(
            f'{source}.{datasets["raw"]}',
            True, True, location),
        resource_validation_helper.DatasetConstraints(
            f'{source}.{datasets["cdc"]}',
            True, True, location),
        resource_validation_helper.DatasetConstraints(
            f'{target}.{datasets["reporting"]}',
            False, True, location)
        ]
    if not resource_validation_helper.validate_resources([],
                                                            datasets):
        return None

    logging.info("✅ SFDC configuration is good.")

    return cfg
