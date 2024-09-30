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
Processes and validates Marketing config.json.
"""

import logging
from typing import Union

from common.py_libs import resource_validation_helper


def _validate_googleads(cfg: dict) -> None:
    """ Validate GoogleAds specific config attributes. """

    logging.info("Validating configuration for GoogleAds...")

    googleads = cfg["marketing"]["GoogleAds"]

    missing_googleads_attrs = []
    for attr in ("deployCDC", "datasets", "lookbackDays"):
        if googleads.get(attr) is None or googleads.get(attr) == "":
            missing_googleads_attrs.append(attr)

    if missing_googleads_attrs:
        raise ValueError(
            "Config file is missing some GoogleAds attributes or has empty "
            f"values: {missing_googleads_attrs}")

    datasets = googleads["datasets"]
    missing_datasets_attrs = []
    for attr in ("cdc", "raw", "reporting"):
        if datasets.get(attr) is None or datasets.get(attr) == "":
            missing_datasets_attrs.append(attr)

    if missing_datasets_attrs:
        raise ValueError(
            "Config file is missing some GoogleAds datasets attributes "
            f"or has empty value: {missing_datasets_attrs} ")

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
        raise ValueError("Resource validation failed.")

    logging.info("✅ Config file validated for GoogleAds and is looking good.")


def _validate_cm360(cfg: dict) -> None:
    """ Validate CM360 specific config attributes. """

    logging.info("Validating Config file for CM360...")

    cm360 = cfg["marketing"]["CM360"]

    missing_cm360_attrs = []
    for attr in ("deployCDC", "dataTransferBucket", "datasets"):
        if cm360.get(attr) is None or cm360.get(attr) == "":
            missing_cm360_attrs.append(attr)

    if missing_cm360_attrs:
        raise ValueError("Config file is missing some CM360 attributes or "
                         f"has empty value: {missing_cm360_attrs}")

    datasets = cm360["datasets"]
    missing_datasets_attrs = []
    for attr in ("cdc", "raw", "reporting"):
        if datasets.get(attr) is None or datasets.get(attr) == "":
            missing_datasets_attrs.append(attr)

    if missing_datasets_attrs:
        raise ValueError(
            "Config file is missing some CM360 datasets attributes "
            f"or has empty value: {missing_datasets_attrs} ")

    source = cfg["projectIdSource"]
    target = cfg["projectIdTarget"]
    location = cfg["location"]
    buckets = [resource_validation_helper.BucketConstraints(
        cm360["dataTransferBucket"], True, location
    )]
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
    if not resource_validation_helper.validate_resources(buckets,
                                                            datasets):
        raise ValueError("Resource validation failed.")

    logging.info("✅ Config file validated for GoogleAds and is looking good.")


def _validate_tiktok(cfg: dict) -> None:
    """ Validate TikTok specific config attributes. """

    logging.info("Validating Config file for TikTok...")

    tiktok = cfg["marketing"]["TikTok"]

    missing_tiktok_attrs = []
    for attr in ("deployCDC", "datasets"):
        if tiktok.get(attr) is None or tiktok.get(attr) == "":
            missing_tiktok_attrs.append(attr)

    if missing_tiktok_attrs:
        raise ValueError(
            "Config file is missing some TikTok attributes or has empty "
            f"values: {missing_tiktok_attrs}")

    datasets = tiktok["datasets"]
    missing_datasets_attrs = []
    for attr in ("cdc", "raw", "reporting"):
        if datasets.get(attr) is None or datasets.get(attr) == "":
            missing_datasets_attrs.append(attr)

    if missing_datasets_attrs:
        raise ValueError(
            "Config file is missing some TikTok datasets attributes "
            f"or has empty value: {missing_datasets_attrs} ")

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
        raise ValueError("Resource validation failed.")

    logging.info("Config file validated for TikTok and is looking good.")


def _validate_liveramp(cfg: dict) -> None:
    """ Validate LiveRamp specific config attributes. """

    logging.info("Validating Config file for LiveRamp...")

    liveramp = cfg["marketing"]["LiveRamp"]

    missing_liveramp_attrs = []
    attr = "datasets"
    if liveramp.get(attr) is None or liveramp.get(attr) == "":
        missing_liveramp_attrs.append(attr)

    if missing_liveramp_attrs:
        raise ValueError(
            "Config file is missing some LiveRamp attributes or has empty "
            f"values: {missing_liveramp_attrs}")

    datasets = liveramp["datasets"]
    missing_datasets_attrs = []
    attr = "cdc"
    if datasets.get(attr) is None or datasets.get(attr) == "":
        missing_datasets_attrs.append(attr)

    if missing_datasets_attrs:
        raise ValueError(
            "Config file is missing some LiveRamp datasets attributes "
            f"or has empty value: {missing_datasets_attrs} ")

    source = cfg["projectIdSource"]
    location = cfg["location"]
    datasets = [
        resource_validation_helper.DatasetConstraints(
            f'{source}.{datasets["cdc"]}',
            True, True, location)
        ]
    if not resource_validation_helper.validate_resources([],
                                                            datasets):
        raise ValueError("Resource validation failed.")

    logging.info("Config file validated for LiveRamp and is looking good.")


def _validate_meta(cfg: dict) -> None:
    """ Validate Meta specific config attributes. """

    logging.info("Validating Config file for Meta...")

    meta = cfg["marketing"]["Meta"]

    missing_meta_attrs = []
    for attr in ("deployCDC", "datasets"):
        if meta.get(attr) is None or meta.get(attr) == "":
            missing_meta_attrs.append(attr)

    if missing_meta_attrs:
        raise ValueError(
            "Config file is missing some Meta attributes or has empty "
            f"values: {missing_meta_attrs}")

    datasets = meta["datasets"]
    missing_datasets_attrs = []
    for attr in ("cdc", "raw", "reporting"):
        if datasets.get(attr) is None or datasets.get(attr) == "":
            missing_datasets_attrs.append(attr)

    if missing_datasets_attrs:
        raise ValueError(
            "Config file is missing some Meta datasets attributes "
            f"or has empty value: {missing_datasets_attrs} ")

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
        raise ValueError("Resource validation failed.")

    logging.info("Config file validated for Meta and is looking good.")


def _validate_sfmc(cfg: dict) -> None:
    """ Validate SFMC specific config attributes. """

    logging.info("Validating Config file for SFMC...")

    sfmc = cfg["marketing"]["SFMC"]

    missing_sfmc_attrs = []
    for attr in ("deployCDC", "fileTransferBucket", "datasets"):
        if sfmc.get(attr) is None or sfmc.get(attr) == "":
            missing_sfmc_attrs.append(attr)

    if missing_sfmc_attrs:
        raise ValueError(
            "Config file is missing some SFMC attributes or has empty "
            f"values: {missing_sfmc_attrs}")

    datasets = sfmc["datasets"]
    missing_datasets_attrs = []
    for attr in ("cdc", "raw", "reporting"):
        if datasets.get(attr) is None or datasets.get(attr) == "":
            missing_datasets_attrs.append(attr)

    if missing_datasets_attrs:
        raise ValueError(
            "Config file is missing some SFMC datasets attributes "
            f"or has empty value: {missing_datasets_attrs} ")

    source = cfg["projectIdSource"]
    target = cfg["projectIdTarget"]
    location = cfg["location"]
    buckets = [resource_validation_helper.BucketConstraints(
        sfmc["fileTransferBucket"], True, location
    )]
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
    if not resource_validation_helper.validate_resources(buckets, datasets):
        raise ValueError("Resource validation failed.")

    logging.info("Config file validated for SFMC and is looking good.")


def validate(cfg: dict) -> Union[dict, None]:
    """Validates and processes configuration.

    Args:
        cfg (dict): Config dictionary.

    Returns:
        dict: Processed config dictionary.
    """

    if not cfg.get("deployMarketing", False):
        logging.info("'marketing' is not being deployed. Skipping validation.")
        return cfg

    logging.info("Validating 'marketing' configuration...")
    marketing = cfg.get("marketing")
    if not marketing:
        logging.error("🛑 Missing 'marketing' values in the config file. 🛑")
        return None

    # Marketing Attributes
    missing_marketing_attr = []
    for attr in ("deployGoogleAds", "deployCM360", "deployTikTok",
                 "deployLiveRamp", "deployMeta", "deploySFMC",
                 "dataflowRegion"):
        if marketing.get(attr) is None or marketing.get(attr) == "":
            missing_marketing_attr.append(attr)

    if missing_marketing_attr:
        logging.error(
            "🛑 Config file is missing some Marketing attributes or "
            "has empty value: %s. 🛑", missing_marketing_attr)
        return None

    # Google Ads
    deploy_googleads = marketing.get("deployGoogleAds")
    if deploy_googleads:
        googleads = marketing.get("GoogleAds")
        if not googleads:
            logging.error(
                "🛑 Missing 'marketing' 'GoogleAds' attribute "
                "in the config file. 🛑")
            return None
        else:
            try:
                _validate_googleads(cfg)
            except ValueError as e:
                logging.error("🛑 GoogleAds config validation failed: %s 🛑",
                              str(e))
                return None
            except Exception as e:  # pylint: disable=broad-except
                logging.error("🛑 GoogleAds config validation failed. 🛑")
                logging.error(e)
                return None

    # CM360
    deploy_cm360 = marketing.get("deployCM360")
    if deploy_cm360:
        cm360 = marketing.get("CM360")
        if not cm360:
            logging.error(
                "🛑 Missing 'marketing' 'CM360' attribute "
                "in the config file. 🛑")
            return None
        else:
            try:
                _validate_cm360(cfg)
            except ValueError as e:
                logging.error("🛑 CM360 config validation failed: %s 🛑",
                              str(e))
                return None
            except Exception as e:  # pylint: disable=broad-except
                logging.error("🛑 CM360 config validation failed. 🛑")
                logging.error(e)
                return None

    # TikTok
    deploy_tiktok = marketing.get("deployTikTok")
    if deploy_tiktok:
        tiktok = marketing.get("TikTok")
        if not tiktok:
            logging.error(
                "🛑 Missing 'marketing' 'TikTok' attribute "
                "in the config file. 🛑")
            return None
        else:
            try:
                _validate_tiktok(cfg)
            except ValueError as e:
                logging.error("🛑 TikTok config validation failed: %s 🛑",
                              str(e))
                return None
            except Exception as e:  # pylint: disable=broad-except
                logging.error("🛑 TikTok config validation failed. 🛑")
                logging.error(e)
                return None

    # LiveRamp
    deploy_liveramp = marketing.get("deployLiveRamp")
    if deploy_liveramp:
        liveramp = marketing.get("LiveRamp")
        if not liveramp:
            logging.error(
                "🛑 Missing 'marketing' 'LiveRamp' attribute "
                "in the config file. 🛑")
            return None
        else:
            try:
                _validate_liveramp(cfg)
            except ValueError as e:
                logging.error("🛑 LiveRamp config validation failed: %s 🛑",
                              str(e))
                return None
            except Exception as e:  # pylint: disable=broad-except
                logging.error("🛑 LiveRamp config validation failed. 🛑")
                logging.error(e)
                return None

    # Meta
    deploy_meta = marketing.get("deployMeta")
    if deploy_meta:
        meta = marketing.get("Meta")
        if not meta:
            logging.error(
                "🛑 Missing 'marketing' 'Meta' attribute "
                "in the config file. 🛑")
            return None
        else:
            try:
                _validate_meta(cfg)
            except ValueError as e:
                logging.error("🛑 Meta config validation failed: %s 🛑",
                              str(e))
                return None
            except Exception as e:  # pylint: disable=broad-except
                logging.error("🛑 Meta config validation failed. 🛑")
                logging.error(e)
                return None

    # SFMC
    deploy_sfmc = marketing.get("deploySFMC")
    if deploy_sfmc:
        sfmc = marketing.get("SFMC")
        if not sfmc:
            logging.error(
                "🛑 Missing 'marketing' 'SFMC' attribute "
                "in the config file. 🛑")
            return None
        else:
            try:
                _validate_sfmc(cfg)
            except ValueError as e:
                logging.error("🛑 SFMC config validation failed: %s 🛑",
                              str(e))
                return None
            except Exception as e:  # pylint: disable=broad-except
                logging.error("🛑 SFMC config validation failed. 🛑")
                logging.error(e)
                return None

    region = marketing["dataflowRegion"].lower()
    location = cfg["location"].lower()
    if region != location and not region.startswith(f"{location}-"):
        logging.error("🛑 Invalid `dataflowRegion`: `%s`. "
                        "It's expected to be in `%s`. 🛑",
                        marketing["dataflowRegion"], cfg["location"])
        return None

    logging.info("✅ 'marketing' config validated successfully. Looks good.")

    return cfg
