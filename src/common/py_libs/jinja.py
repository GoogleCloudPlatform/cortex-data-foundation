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
#
"""Library for Jinja related functions."""

from jinja2 import Environment, FileSystemLoader
import json
import logging

logger = logging.getLogger(__name__)


def apply_jinja_params_to_file(input_file: str, jinja_data_file: str) -> str:
    """Applies Jinja data file to the given file and returns rendered text.

    Args:
        input_file: File to be resolved with jinja parameters.
        jinja_data_file: File containing jinja parameters.

    Returns:
        Text from the input file after applying jinja parameters from the
        data file.
    """

    logger.debug("Applying jinja data file to '%s' file ...", input_file)

    with open(jinja_data_file, mode="r", encoding="utf-8") as jinja_f:
        jinja_data_dict = json.load(jinja_f)

    logger.debug("jinja_data_dict = %s", json.dumps(jinja_data_dict, indent=4))

    env = Environment(loader=FileSystemLoader("."))
    input_template = env.get_template(input_file)
    output_text = input_template.render(jinja_data_dict)
    logger.debug("Rendered text = \n%s", output_text)

    return output_text


def initialize_jinja_from_config(config_dict: dict) -> dict:
    """Generates jinja parameters dictionary from configuration."""

    jinja_data_file_dict = {
        "project_id_src": config_dict["projectIdSource"],
        "project_id_tgt": config_dict["projectIdTarget"],
        "location": config_dict["location"],
        "k9_datasets_processing": config_dict["k9"]["datasets"]["processing"],
        "k9_datasets_reporting": config_dict["k9"]["datasets"]["reporting"],
    }
    # SFDC
    if config_dict.get("deploySFDC"):
        jinja_data_file_dict.update({
            "sfdc_datasets_raw":
                config_dict["SFDC"]["datasets"]["raw"],
            "sfdc_datasets_cdc":
                config_dict["SFDC"]["datasets"]["cdc"],
            "sfdc_datasets_reporting":
                config_dict["SFDC"]["datasets"]["reporting"],
            "currencies":
                ",".join(
                    f"'{currency}'" for currency in config_dict["currencies"]),
            "languages":
                ",".join(
                    f"'{language}'" for language in config_dict["languages"])
        })
    # SAP
    if config_dict.get("deploySAP"):
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
            # We only use lowercase SQLFlavor in our templates
            "sql_flavor":
                config_dict["SAP"]["SQLFlavor"].lower(),
            "sql_flavour":
                config_dict["SAP"]["SQLFlavor"].lower(),
            # TODO: Update SAP sql to use currency jinja variable in a
            # readable way - e.g. "IN {{ currencies }}"
            "currency":
                "IN (" + ",".join(
                    f"'{currency}'" for currency in config_dict["currencies"]) +
                ")",
            "language":
                "IN (" + ",".join(
                    f"'{language}'" for language in config_dict["languages"]) +
                ")"
        })
    if config_dict.get("deployMarketing"):
        # GoogleAds
        if config_dict["marketing"].get("deployGoogleAds"):
            jinja_data_file_dict.update({
                "marketing_googleads_datasets_raw":
                    config_dict["marketing"]["GoogleAds"]["datasets"]["raw"],
                "marketing_googleads_datasets_cdc":
                    config_dict["marketing"]["GoogleAds"]["datasets"]["cdc"],
                "marketing_googleads_datasets_reporting":
                    config_dict["marketing"]["GoogleAds"]["datasets"]
                    ["reporting"]
            })
        # CM360
        if config_dict["marketing"].get("deployCM360"):
            jinja_data_file_dict.update({
                "marketing_cm360_datasets_raw":
                    config_dict["marketing"]["CM360"]["datasets"]["raw"],
                "marketing_cm360_datasets_cdc":
                    config_dict["marketing"]["CM360"]["datasets"]["cdc"],
                "marketing_cm360_datasets_reporting":
                    config_dict["marketing"]["CM360"]["datasets"]["reporting"]
            })


    return jinja_data_file_dict
