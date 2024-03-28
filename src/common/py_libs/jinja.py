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

import json
import logging
from pathlib import Path
from typing import Any, Dict

from jinja2 import Environment
from jinja2 import FileSystemLoader

from common.py_libs import configs

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


def apply_jinja_params_dict_to_file(input_file: Path,
                                    jinja_data_dict: Dict[str, Any]) -> str:
    """Applies Jinja data dict to the given file and returns rendered text.

    Args:
        input_file (Path): File to be resolved with jinja parameters.
        jinja_data_dict (Dict[str, Any]): Dictionary containing
            jinja parameters.

    Returns:
        Text from the input file after applying jinja parameters from the
        dictionary.
    """

    logger.debug("Rendering Jinja template file: '%s'", input_file)

    env = Environment(
        loader=FileSystemLoader(input_file.parent.absolute().as_posix()))
    input_template = env.get_template(input_file.name)
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
    # Flavor specific fields use `get()` because they do not exist in the raw
    # config and accessing them directly causes the function to fail.
    if config_dict.get("deploySAP"):
        jinja_data_file_dict.update({
            # raw datasets
            "dataset_raw_landing":
                config_dict["SAP"]["datasets"]["raw"],
            "dataset_raw_landing_ecc":
                config_dict["SAP"]["datasets"].get("rawECC"),
            "dataset_raw_landing_s4":
                config_dict["SAP"]["datasets"].get("rawS4"),
            # cdc datasets
            "dataset_cdc_processed":
                config_dict["SAP"]["datasets"]["cdc"],
            "dataset_cdc_processed_ecc":
                config_dict["SAP"]["datasets"].get("cdcECC"),
            "dataset_cdc_processed_s4":
                config_dict["SAP"]["datasets"].get("cdcS4"),
            # reporting datasets
            "dataset_reporting_tgt":
                config_dict["SAP"]["datasets"]["reporting"],
            # mandt
            "mandt":
                config_dict["SAP"]["mandt"],
            "mandt_ecc":
                config_dict["SAP"].get("mandtECC"),
            "mandt_s4":
                config_dict["SAP"].get("mandtS4"),
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
        # TikTok
        if config_dict["marketing"].get("deployTikTok"):
            jinja_data_file_dict.update({
                "marketing_tiktok_datasets_raw":
                    config_dict["marketing"]["TikTok"]["datasets"]["raw"],
                "marketing_tiktok_datasets_cdc":
                    config_dict["marketing"]["TikTok"]["datasets"]["cdc"],
                "marketing_tiktok_datasets_reporting":
                    config_dict["marketing"]["TikTok"]["datasets"]["reporting"]
            })
        # LiveRamp
        if config_dict["marketing"].get("deployLiveRamp"):
            jinja_data_file_dict.update({
                "marketing_liveramp_datasets_cdc":
                    config_dict["marketing"]["LiveRamp"]["datasets"]["cdc"]
            })
        # Meta
        if config_dict["marketing"].get("deployMeta"):
            jinja_data_file_dict.update({
                "marketing_meta_datasets_raw":
                    config_dict["marketing"]["Meta"]["datasets"]["raw"],
                "marketing_meta_datasets_cdc":
                    config_dict["marketing"]["Meta"]["datasets"]["cdc"],
                "marketing_meta_datasets_reporting":
                    config_dict["marketing"]["Meta"]["datasets"]["reporting"]
            })
        # SFMC
        if config_dict["marketing"].get("deploySFMC"):
            jinja_data_file_dict.update({
                "marketing_sfmc_datasets_raw":
                    config_dict["marketing"]["SFMC"]["datasets"]["raw"],
                "marketing_sfmc_datasets_cdc":
                    config_dict["marketing"]["SFMC"]["datasets"]["cdc"],
                "marketing_sfmc_datasets_reporting":
                    config_dict["marketing"]["SFMC"]["datasets"]["reporting"]
            })

    return jinja_data_file_dict


def create_jinja_data_file_from_config_dict(config_dict: dict,
                                            jinja_data_file) -> None:

    jinja_data_file = Path(jinja_data_file)

    logger.info("Creating jinja template data file '%s'...", jinja_data_file)

    with open(jinja_data_file, "w", encoding="utf-8") as f:
        jinja_data_file_dict = initialize_jinja_from_config(config_dict)
        f.write(json.dumps(jinja_data_file_dict, indent=4))

    logger.info("Jinja template data file '%s' created successfully.",
                jinja_data_file)


def create_jinja_data_file_from_config_file(config_file: str,
                                            jinja_data_file: str) -> None:
    """Generates jinja data file from Cortex Config file."""

    config_dict = configs.load_config_file(config_file)
    create_jinja_data_file_from_config_dict(config_dict, jinja_data_file)
