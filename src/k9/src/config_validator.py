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
Processes and validates K9.
"""

import logging
from typing import Union

from common.py_libs import constants


def validate(config: dict) -> Union[dict, None]:
    """Validates and processes configuration.

    It will discover and log all issues before returning.

    Args:
        config (dict): Config dictionary.

    Returns:
        dict: Processed config dictionary.
        None: In case of validation failure
    """

    failed = False

    # Product Dimension config validation
    config["k9"]["deployProductDim"] = config["k9"].get("deployProductDim",
                                                        False)
    if config["k9"]["deployProductDim"] and "ProductDim" not in config["k9"]:
        logging.error(
            "🛑 Product Dimension is enabled, but no options are specified. 🛑")
        failed = True
    if config["k9"]["deployProductDim"]:
        pd_config = config["k9"]["ProductDim"]
        # TODO: Remove this logic when adding additional sources.
        if (pd_config["dataSourceType"] == "SAP" and not config["deploySAP"]):
            logging.error(("🛑 Product Dimension is set to use SAP, "
                           "but SAP deployment is not enabled. 🛑"))
            failed = True
        if not pd_config.get("dataSourceType"):
            logging.error(
                "🛑 Product Dimension data source type value is missing. 🛑")
            failed = True
        if not pd_config.get("textLanguage"):
            logging.error(
                "🛑 Product Dimension text language value is missing. 🛑")
            failed = True
        elif (pd_config["dataSourceType"] == "SAP" and
              pd_config["textLanguage"] not in config["SAP"]["languages"]):
            logging.error(
                "🛑 Product Dimension text language is not deployed in SAP. 🛑")
            failed = True

    # Currency Conversion config validation
    if (config["k9"]["deployCurrencyConversion"] and
            "CurrencyConversion" not in config["k9"]):
        logging.error("🛑 Currency Conversion is enabled, "
                      "but no options are specified. 🛑")
        failed = True
    if config["k9"]["deployCurrencyConversion"]:
        cc_config = config["k9"]["CurrencyConversion"]

        if not cc_config["dataSourceType"]:
            logging.error(("🛑 Data Source Type is empty. It needs to be "
                           "specified when using Currency Conversion. 🛑"))
            failed = True
        elif (cc_config["dataSourceType"] == "SAP" and not config["deploySAP"]):
            logging.error(("🛑 Currency Conversion is set to use SAP, "
                           "but SAP deployment is not enabled. 🛑"))
            failed = True

        if not cc_config["rateType"]:
            logging.error(("🛑 Rate Type is empty. It needs to be specified "
                           "when using Currency Conversion. 🛑"))
            failed = True

    # Cross Media config validation
    config["k9"]["deployCrossMedia"] = config["k9"].get("deployCrossMedia",
                                                        False)
    if config["k9"]["deployCrossMedia"]:
        if "CrossMedia" not in config["k9"]:
            logging.error(
                "🛑 Cross Media is enabled, but no options are specified. 🛑")
            failed = True
        else:
            cm_config = config["k9"]["CrossMedia"]
            if not cm_config.get("lookbackWindowDays"):
                logging.error(("🛑 lookbackWindowDays value is missing "
                               "in Cross Media configuration. 🛑"))
                failed = True
            if not cm_config.get("productHierarchyType"):
                logging.error(("🛑 productHierarchyType value is missing "
                               "in Cross Media configuration. 🛑"))
                failed = True
            if not cm_config.get("maxProductHierarchyMatchLevel"):
                logging.error(
                    ("🛑 maxProductHierarchyMatchLevel value is missing "
                     "in Cross Media configuration. 🛑"))
                failed = True
            if not cm_config.get("targetCurrencies"):
                logging.error(("🛑 Target Currencies value is missing "
                               "in Cross Media configuration. 🛑"))
                failed = True
            # TODO: Remove when multi target currency support is added later.
            if len(cm_config.get("targetCurrencies") or []) > 1:
                logging.error(("🛑 Only 1 target currency should be specified "
                               "in Cross Media configuration. 🛑"))
                failed = True
            if not cm_config.get("lookbackWindowDays"):
                logging.error(("🛑 lookbackWindowDays value is missing "
                               "in Cross Media configuration. 🛑"))
                failed = True
            config["k9"]["CrossMedia"]["textGenerationModel"] = cm_config.get(
                "textGenerationModel",
                constants.K9_CROSS_MEDIA_DEFAULT_TEXT_GENERATION_MODEL)
            config["k9"]["CrossMedia"]["additionalPrompt"] = cm_config.get(
                "additionalPrompt", "")

        if not config["k9"]["deployProductDim"]:
            logging.error(
                "🛑 Cross Media is enabled, but Product Dimension is not. 🛑")
            failed = True

        if not config["k9"]["deployCurrencyConversion"]:
            logging.error(
                "🛑 Cross Media is enabled, but Currency Conversion is not. 🛑")
            failed = True

        if not config["k9"]["deployCountryDim"]:
            logging.error(
                "🛑 Cross Media is enabled, but Country Dimension is not. 🛑")
            failed = True

        if not config["deployMarketing"] or not (
                config["marketing"]["deployGoogleAds"] or
                config["marketing"]["deployDV360"] or
                config["marketing"]["deployMeta"] or
                config["marketing"]["deployTikTok"]):
            logging.error(
                "🛑 Cross Media requires one or more of Google Ads, DV360, "
                "Meta or TikTok to be deployed. 🛑")
            failed = True

    # Meridian config validation
    config["k9"]["deployMeridian"] = config["k9"].get("deployMeridian", False)

    if config["k9"]["deployMeridian"]:
        if "Meridian" not in config["k9"]:
            logging.error(("🛑 Meridian is enabled, "
                "but no options are specified. 🛑"))
            failed = True
        else:
            meridian_config = config["k9"]["Meridian"]

            if not meridian_config.get("deploymentType"):
                logging.error(
                    "🛑 Meridian deploymentType is missing. 🛑")
                failed = True
            elif (not meridian_config["salesDataSourceType"] == "BYOD" and
                  not meridian_config["salesDatasetID"]):
                logging.error(("🛑 A dataset for sales reporting must be"
                               "specified for incremental deployment 🛑"))
                failed = True
            elif (meridian_config["deploymentType"] == "initial"
                and not config["k9"]["deployCrossMedia"]):
                logging.error(
                    "🛑 Meridian requires Cross Media in a fresh deployment 🛑")
                failed = True
            elif (meridian_config["deploymentType"] == "initial" and
                  meridian_config["salesDataSourceType"] == "SAP" and
                  not config["deploySAP"]):
                logging.error(
                    ("🛑 When deploying a new data foundation with "
                     "Meridian and sales data source is SAP, SAP deployment "
                     "needs to be enabled. 🛑"))
                failed = True
            elif (meridian_config["deploymentType"] == "initial" and
                  meridian_config["salesDataSourceType"] == "OracleEBS" and
                  not config["deployOracleEBS"]):
                logging.error(
                    ("🛑 When deploying a new data foundation with "
                     "Meridian and sales data source is OracleEBS, Oracle EBS "
                     "deployment needs to be enabled. 🛑"))
                failed = True

    if failed:
        logging.error("🛑 K9 configuration is invalid. 🛑")
        return None
    else:
        logging.info("✅ K9 configuration is good. ✅")
        return config
