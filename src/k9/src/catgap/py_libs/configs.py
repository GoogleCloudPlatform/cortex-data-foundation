# Copyright 2022 Google LLC
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
#
"""Library for Cortex Config related functions."""

import json
import logging
from pathlib import Path
import yaml


def _validate_config(config):
    """Validates configurations.

    Args:
        config: Dictionary of config as read from a config file.

    Raises:
      ValueError: If config is missing a required value.
    """

    # TODO: This only validates for Ads for now. This can and should be
    # expanded to common libs.

    logging.debug("Validating config")

    if config is None:
        raise ValueError("Config is empty.")

    # Make sure all the other needed top level configs are in place.
    missing_attr = []
    for attr in ["SAP", "k9", "projectIdSource", "projectIdTarget", "location"]:
        if config.get(attr) is None or config.get(attr) == "":
            missing_attr.append(attr)

    if missing_attr:
        raise ValueError("Config file is missing needed attributes or "
                         f"has empty value: {missing_attr}")

    sap_attr = config.get("SAP")
    missing_sap_attr = []
    for attr in ["mandt", "datasets"]:
        if sap_attr.get(attr) is None or sap_attr.get(attr) == "":
            missing_sap_attr.append(attr)
    if missing_sap_attr:
        raise ValueError("Config file is missing needed SAP attributes or "
                         f"has empty value: {missing_sap_attr}")
    sap_datasets = sap_attr.get("datasets")
    missing_datasets_attr = []
    for attr in ["cdc", "reporting"]:
        if sap_datasets.get(attr) is None or sap_datasets.get(attr) == "":
            missing_datasets_attr.append(attr)
    if missing_datasets_attr:
        raise ValueError(
            "Config file is missing or has empty value for one or more "
            f"SAP datasets attributes: {missing_datasets_attr} ")

    k9_attr = config.get("k9")
    missing_k9_attr = []
    for attr in ["datasets"]:
        if k9_attr.get(attr) is None or k9_attr.get(attr) == "":
            missing_k9_attr.append(attr)
    if missing_k9_attr:
        raise ValueError("Config file is missing needed k9 attributes or "
                         f"has empty value: {missing_sap_attr}")
    k9_datasets = k9_attr.get("datasets")
    missing_datasets_attr = []
    for attr in ["processing", "reporting"]:
        if k9_datasets.get(attr) is None or k9_datasets.get(attr) == "":
            missing_datasets_attr.append(attr)
    if missing_datasets_attr:
        raise ValueError(
            "Config file is missing or has empty value for one or more "
            f"k9 datasets attributes: {missing_datasets_attr} ")

    logging.debug("Config file validated and is looking good.")


def load_config_file(config_file):
    """Loads a json config file to a dictionary and validates it.

    Args:
        config_file: Path of config json file.

    Returns:
        Config as a dictionary.
    """
    logging.debug("Config file = %s", config_file)

    if not Path(config_file).is_file():
        raise FileNotFoundError(f"Config file '{config_file}' does not exist.")

    with open(config_file, mode="r", encoding="utf-8") as f:
        try:
            config = json.load(f)
        except json.JSONDecodeError:
            e_msg = f"Config file '{config_file}' is malformed or empty."
            raise RuntimeError(e_msg) from None

        logging.debug("config = %s", config)

        _validate_config(config)

    return config

def check_on_setting(setting, setting_name, mandatory_attributes: list[str]):
    missing_attr = []
    for attr in mandatory_attributes:
        if setting.get(attr, None) is None or setting.get(attr) == "":
            missing_attr.append(attr)
    if missing_attr:
        raise ValueError("Config file is missing needed attributes in "
                         f"{setting_name} or they have empty values: "
                         f"{missing_attr}")

def _validate_setting(setting):
    if "catgap" not in setting:
        raise ValueError("Setting file is missing `catgap` section.")
    catgap = setting["catgap"]

    check_on_setting(catgap, "catgap", ["ads", "sap", "dataflow"])
    check_on_setting(catgap["ads"],
                     "catgap/ads", ["dts_dataset", "customer_id"])
    check_on_setting(catgap["sap"],
                     "catgap/sap", ["max_prod_hierarchy_level"])
    check_on_setting(catgap["dataflow"],
                     "catgap/dataflow",
                     ["service_account", "gcs_bucket", "region"])

    max_sap_prod_hier_level = int(catgap["sap"]["max_prod_hierarchy_level"])
    if max_sap_prod_hier_level < 2 or max_sap_prod_hier_level > 9:
        raise ValueError("catgap/sap/max_prod_hierarchy_level "
                         "must be more or equal to 2 and less or equal to 9.")
    logging.debug("Setting file validated and is looking good.")


def load_setting_file(setting_file, section_name = None):
    """Loads a yaml config file to a dictionary and validates it.

    Args:
        setting_file: Path of config yaml file.

    Returns:
        Config as a dictionary.
    """
    logging.debug("Setting file = %s", setting_file)

    if not Path(setting_file).is_file():
        raise FileNotFoundError((f"Setting file '{setting_file}' "
                                "does not exist."))
    with open(setting_file, encoding="utf-8") as settings_file:
        setting = yaml.load(settings_file, Loader=yaml.SafeLoader)

    _validate_setting(setting)

    if section_name:
        return setting[section_name]
    else:
        return setting
