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
        None: In case of validation failure
    """

    if not cfg.get("deploySAP", False):
        logging.info("SAP is not being deployed. Skipping validation.")
        return cfg

    logging.info("Validating SAP configuration.")

    sap = cfg.get("SAP")
    if not sap:
        logging.error("🛑 Missing 'SAP' values in the config file. 🛑")
        return None

    missing_sap_attrs = []
    for attr in ("deployCDC", "languages", "currencies", "datasets"):
        if sap.get(attr) is None or sap.get(attr) == "":
            missing_sap_attrs.append(attr)

    if missing_sap_attrs:
        logging.error("🛑 Missing 'SAP values: %s' in the config file. 🛑",
                      missing_sap_attrs)
        return None

    datasets = sap.get("datasets")

    flavor = sap.get("SQLFlavor", "ecc").lower()
    sap["SQLFlavor"] = flavor

    cfg_sap_ds_cdc = datasets.get("cdc")
    cfg_sap_ds_cdc_ecc = datasets.get("cdcECC")
    cfg_sap_ds_cdc_s4 = datasets.get("cdcS4")

    if flavor == "ecc":
        if cfg_sap_ds_cdc and not cfg_sap_ds_cdc_ecc:
            cfg_sap_ds_cdc_ecc = cfg_sap_ds_cdc
        else:
            cfg_sap_ds_cdc = cfg_sap_ds_cdc_ecc
    elif flavor == "s4":
        if cfg_sap_ds_cdc and not cfg_sap_ds_cdc_s4:
            cfg_sap_ds_cdc_s4 = cfg_sap_ds_cdc
        else:
            cfg_sap_ds_cdc = cfg_sap_ds_cdc_s4

    if not cfg_sap_ds_cdc:
        logging.error(("🛑 Cannot resolve SAP/datasets/cdc|cdcECC|cdcS4 values "
                       "in the config file. 🛑"))
        return None

    datasets["cdc"] = cfg_sap_ds_cdc
    datasets["cdcECC"] = cfg_sap_ds_cdc_ecc
    datasets["cdcS4"] = cfg_sap_ds_cdc_s4

    cfg_sap_ds_raw = datasets.get("raw")
    cfg_sap_ds_raw_ecc = datasets.get("rawECC")
    cfg_sap_ds_raw_s4 = datasets.get("rawS4")

    if flavor == "union":
        if not cfg_sap_ds_raw_ecc or not cfg_sap_ds_raw_s4:
            logging.error("🛑 SAP/SQLFlavor=union requires "
                          "all parameters for both ECC and S4. 🛑")
    elif flavor == "ecc":
        if cfg_sap_ds_raw and not cfg_sap_ds_raw_ecc:
            cfg_sap_ds_raw_ecc = cfg_sap_ds_raw
        else:
            cfg_sap_ds_raw = cfg_sap_ds_raw_ecc
    elif flavor == "s4":
        if cfg_sap_ds_raw and not cfg_sap_ds_raw_s4:
            cfg_sap_ds_raw_s4 = cfg_sap_ds_raw
        else:
            cfg_sap_ds_raw = cfg_sap_ds_raw_s4

    if not cfg_sap_ds_raw:
        logging.error(("🛑 Cannot resolve SAP/datasets/raw|rawECC|rawS4 values "
                       "in the config file. 🛑"))
        return None

    datasets["raw"] = cfg_sap_ds_raw
    datasets["rawECC"] = cfg_sap_ds_raw_ecc
    datasets["rawS4"] = cfg_sap_ds_raw_s4

    cfg_sap_ds_reporting = datasets.get("reporting", "REPORTING")
    datasets["reporting"] = cfg_sap_ds_reporting
    datasets["ml"] = datasets.get("ml", "ML_MODELS")

    cfg_sap_mandt = sap.get("mandt")
    cfg_sap_mandt_ecc = sap.get("mandtECC")
    cfg_sap_mandt_s4 = sap.get("mandtS4")

    if flavor == "union":
        if not cfg_sap_mandt_ecc or not cfg_sap_mandt_s4:
            logging.error(("🛑 SAP/SQLFlavor=union requires "
                           "all parameters for both ECC and S4. 🛑"))
            return None
        elif cfg_sap_mandt_ecc == cfg_sap_mandt_s4:
            logging.error(("🛑 Same ECC and S4 MANDT "
                           "is not allowed for UNION workloads. 🛑"))
            return None
    elif flavor == "ecc":
        if cfg_sap_mandt and not cfg_sap_mandt_ecc:
            cfg_sap_mandt_ecc = cfg_sap_mandt
        else:
            cfg_sap_mandt = cfg_sap_mandt_ecc
    elif flavor == "s4":
        if cfg_sap_mandt and not cfg_sap_mandt_s4:
            cfg_sap_mandt_s4 = cfg_sap_mandt
        else:
            cfg_sap_mandt = cfg_sap_mandt_s4

    if not cfg_sap_mandt:
        logging.warning("⚠️ Using default SAP Mandt/client = 100.")
        cfg_sap_mandt = "100"

    sap["mandt"] = cfg_sap_mandt
    sap["mandtECC"] = cfg_sap_mandt_ecc or cfg_sap_mandt
    sap["mandtS4"] = cfg_sap_mandt_s4 or cfg_sap_mandt

    source = cfg["projectIdSource"]
    target = cfg["projectIdTarget"]
    location = cfg["location"]
    datasets = [
        resource_validation_helper.DatasetConstraints(
            f"{source}.{cfg_sap_ds_raw}", True, cfg.get("testData", False),
            location),
        resource_validation_helper.DatasetConstraints(
            f"{source}.{cfg_sap_ds_cdc}", True, True, location),
        resource_validation_helper.DatasetConstraints(
            f"{target}.{cfg_sap_ds_reporting}", False, True, location)
    ]
    if not resource_validation_helper.validate_resources([], datasets):
        return None

    logging.info("✅ SAP configuration is good.")

    return cfg
