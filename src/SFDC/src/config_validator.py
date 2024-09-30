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
Processes and validates SFDC reporting config.json.
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
        None: In case of validation failure
    """

    if not cfg.get("deploySFDC"):
        logging.info("SFDC is not being deployed. Skipping validation.")
        return cfg

    logging.info("Validating SFDC configuration.")

    sfdc = cfg.get("SFDC")
    if not sfdc:
        logging.error("🛑 Missing 'SFDC' values in the config file. 🛑")
        return None

    missing_sfdc_attrs = []
    for attr in ("deployCDC", "currencies", "datasets"):
        if sfdc.get(attr) is None or sfdc.get(attr) == "":
            missing_sfdc_attrs.append(attr)

    if missing_sfdc_attrs:
        logging.error("🛑 Missing 'SFDC values: %s' in the config file. 🛑",
                      missing_sfdc_attrs)
        return None

    datasets = sfdc.get("datasets")

    sfdc["createMappingViews"] = sfdc.get("createMappingViews", True)
    cdc = datasets.get("cdc")
    if not cdc:
        logging.error("🛑 Missing 'SFDC/datasets/cdc' values "
                      "in the config file. 🛑")
        return None

    raw = datasets.get("raw")
    if not raw:
        logging.error("🛑 Missing 'SFDC/datasets/raw' values "
                      "in the config file. 🛑")
        return None

    reporting = datasets.get("reporting", "REPORTING_SFDC")
    datasets["reporting"] = reporting

    source = cfg["projectIdSource"]
    target = cfg["projectIdTarget"]
    location = cfg["location"]
    datasets = [
        resource_validation_helper.DatasetConstraints(f"{source}.{raw}", True,
                                                      True, location),
        resource_validation_helper.DatasetConstraints(f"{source}.{cdc}", True,
                                                      True, location),
        resource_validation_helper.DatasetConstraints(f"{target}.{reporting}",
                                                      False, True, location)
    ]
    if not resource_validation_helper.validate_resources([], datasets):
        return None

    logging.info("✅ SFDC configuration is good.")

    return cfg
