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
Processes config.json for resolving default values and validation.
"""

import argparse
from importlib.util import spec_from_file_location, module_from_spec
import json
import logging
import sys
from pathlib import Path
import typing
import uuid

from google.cloud.exceptions import (BadRequest,
                                     Forbidden,
                                     Unauthorized,
                                     ServerError)
from google.cloud import bigquery

# Make sure common modules are in Python path
sys.path.append(str(Path(__file__).parent.parent))

# pylint:disable=wrong-import-position
from common.py_libs import bq_helper, resource_validation_helper
from common.py_libs.test_harness import TEST_HARNESS_VERSION

_DEFAULT_CONFIG_ = "config/config.json"
_VALIDATOR_FILE_NAME_ = "config_validator"
_VALIDATE_FUNC_NAME_ = "validate"
_DEFAULT_TEST_HARNESS_PROJECT = "kittycorn-public"


def _load_config(config_file):
    """Loads config file.

    Args:
        config_file (StrOrPath): config file path

    Raises:
        FileNotFoundError: Config file not found.
        RuntimeError: Invalid file format.

    Returns:
        typing.Dict[str, any]: configuration dictionary
    """
    if not Path(config_file).is_file():
        raise FileNotFoundError(f"🛑 Config file '{config_file}' "
                                "does not exist. 🛑")
    with open(config_file, mode="r", encoding="utf-8") as cf:
        try:
            config = json.load(cf)
        except json.JSONDecodeError:
            e_msg = f"🛑 Config file '{config_file}' is malformed or empty. 🛑"
            raise RuntimeError(e_msg) from None
    return config


def _save_config(config_file, config_dict):
    """Saves config dictionary to a json file.

    Args:
        config_file (str): Config file path.
        config_dict (typing.Dict[str, any]): Config dictionary.
    """
    with open(config_file, mode="w", encoding="utf-8") as cf:
        json.dump(config_dict, cf, indent=4)


def _validate_workload(
    config: typing.Dict[str, any],  # type: ignore
    workload_path: str
) -> typing.Optional[typing.Dict[str, any]]:  # type: ignore
    """Searches for {_VALIDATOR_FILE_NAME_}.py (config_validator.py) file
    in `workload_path` sub-directory of current directory,
    and calls {_VALIDATE_FUNC_NAME_} (validate) function in it.

    Args:
        config (dict): Configuration dictionary to be passed to
                                   the workload's `validate` function
        workload_path (str): Sub-directory path

    Returns:
        dict: Validated and processed configuration dictionary
              as returned from `validate` function.
              Returns None if config_validator.py file doesn't exists or
              of validate function is not defined in it.
    """

    repo_root = Path.cwd().absolute()
    validator_dir = repo_root.joinpath(
        workload_path) if workload_path else repo_root
    full_file_path = repo_root.joinpath(
        validator_dir, f"{_VALIDATOR_FILE_NAME_}.py").absolute()
    if not full_file_path.exists():
        logging.error("🛑 No config validator for `%s`. Missing `%s` 🛑.",
                      workload_path, str(full_file_path))
        return None
    logging.info("Found %s.py in %s. Running 'validate'.",
                 _VALIDATOR_FILE_NAME_, validator_dir)

    spec = spec_from_file_location(_VALIDATOR_FILE_NAME_, full_file_path)
    module = module_from_spec(spec)  # type: ignore
    spec.loader.exec_module(module)  # type: ignore

    if not hasattr(module, _VALIDATE_FUNC_NAME_):
        logging.error("🛑 %s doesn't have %s function. 🛑",
                      str(full_file_path), _VALIDATE_FUNC_NAME_)
        return None

    validate_func = getattr(module, _VALIDATE_FUNC_NAME_)
    return validate_func(config)


def _validate_config_resources(config: typing.Dict[str, typing.Any])-> bool:
    source = config["projectIdSource"]
    target = config["projectIdTarget"]
    location = config["location"]

    # Checking if we can create datasets in source and target projects.
    projects = [source]
    if source != target:
        projects.append(target)
    for project in projects:
        bq_client = bigquery.Client(project=project, location=location)
        temp_dataset_name = f"tmp_cortex_{uuid.uuid4().hex}"
        full_temp_dataset_name = f"{project}.{temp_dataset_name}"
        try:
            bq_helper.create_dataset(bq_client,
                                     full_temp_dataset_name,
                                     location,
                                     True)
            logging.info("✅ BigQuery in project `%s` is available "
                         "for writing.", project)
        except (Forbidden, Unauthorized):
            logging.error("🛑 Insufficient permissions to create datasets "
                          "in project `%s`. 🛑", project)
            return False
        except (BadRequest, ServerError):
            logging.error("🛑 Error when trying to create a BigQuery dataset "
                          "in project `%s`. 🛑", project, exc_info=True)
            return False
        finally:
            try:
                bq_client.delete_dataset(full_temp_dataset_name,
                                         not_found_ok=True)
            except BadRequest:
                logging.warning("⚠️ Couldn't delete temporary dataset `%s`. "
                                "Please delete it manually. ⚠️",
                                full_temp_dataset_name)

    # targetBucket must exist and be writable
    buckets = [resource_validation_helper.BucketConstraints(
                    str(config["targetBucket"]), True, location)]
    # K9 dataset must be writable, if exist.
    # If it doesn't exist, it will be created later.
    datasets = [
            resource_validation_helper.DatasetConstraints(
                f'{source}.{config["k9"]["datasets"]["processing"]}',
                False, True, location),
            resource_validation_helper.DatasetConstraints(
                f'{target}.{config["k9"]["datasets"]["reporting"]}',
                False, True, location)
            ]
    return resource_validation_helper.validate_resources(buckets, datasets)


def validate_config(
    config: typing.Dict[str, any],  # type: ignore
    sub_validators: typing.List[str]  # type: ignore
) -> typing.Optional[typing.Dict[str, typing.Any]]:  # type: ignore
    """Performs common config validation. Discovers and calls
    workload-specific validators.

    Args:
        config (typing.Dict[str, any]): loaded config.json as a dictionary.
        sub_validators (typing.List[str]): sub-directories with
                                                config_validator.py to call.

    Returns:
        typing.Optional[dict]: validated and updated config dictionary
                            or None if invalid config.
    """

    if not config.get("projectIdSource", None):
        logging.error(
            "🛑 Missing 'projectIdSource' configuration value. 🛑")
        return None

    if not config.get("targetBucket", None):
        logging.error(
            "🛑 Missing 'targetBucket' configuration value. 🛑")
        return None

    config["projectIdTarget"] = config.get("projectIdTarget", "")
    if not config["projectIdTarget"]:
        logging.warning(("⚠️ projectIdTarget is not specified."
                         "Using projectIdSource (%s) ⚠️"),
                        config["projectIdSource"])
        config["projectIdTarget"] = config["projectIdSource"]

    config["deploySAP"] = config.get("deploySAP", False)
    config["deploySFDC"] = config.get("deploySFDC", False)
    config["deployMarketing"] = config.get("deployMarketing", False)
    config["deployDataMesh"] = config.get("deployDataMesh", False)
    config["testData"] = config.get("testData", False)
    config["turboMode"] = config.get("turboMode", True)

    config["location"] = config.get("location", None)
    if not config["location"]:
        logging.warning("⚠️ No location specified. Using `US`. ⚠️")
        config["location"] = "us"
    config["currencies"] = config.get("currencies", ["USD"])
    config["languages"] = config.get("languages", ["E", "S"])

    if config.get("testDataProject", "") == "":
        logging.warning("testDataProject is empty. Using default project `%s`.",
                        _DEFAULT_TEST_HARNESS_PROJECT)
        config["testDataProject"] = _DEFAULT_TEST_HARNESS_PROJECT

    if "k9" not in config:
        logging.warning("No K9 configuration. Using defaults.")
        config["k9"] = {}
    if "datasets" not in config["k9"]:
        logging.warning(("⚠️ No K9 datasets configuration. "
                         "Using defaults. ⚠️"))
        config["k9"]["datasets"] = {}
    if not config["k9"]["datasets"].get("processing", None):
        logging.warning(("⚠️ No K9 processing dataset specified. "
                         "Defaulting to K9_PROCESSING. ⚠️"))
        config["k9"]["datasets"]["processing"] = "K9_PROCESSING"
    if not config["k9"]["datasets"].get("reporting", None):
        logging.warning(("⚠️ No K9 reporting dataset specified. "
                         "Defaulting to K9_REPORTING. ⚠️"))
        config["k9"]["datasets"]["reporting"] = "K9_REPORTING"

    if config["deployDataMesh"] and "DataMesh" not in config:
        logging.error(
            "🛑 Data Mesh is enabled but no options are specified. 🛑")
        return None

    logging.info("Fetching test harness version.")
    config["testHarnessVersion"] = config.get("testHarnessVersion",
                                              TEST_HARNESS_VERSION)

    logging.info("Validating common configuration resources.")
    if not _validate_config_resources(config):
        logging.error(
            "🛑 Resource validation failed. 🛑")
        return None

    logging.info("✅ Common configuration is valid.")

    # Go over all sub-validator directories,
    # and call validate() in config_validator.py in every directory.
    for validator in sub_validators:
        validator_text = validator if validator else "current repository"
        logging.info("Running config validator in `%s`.", validator_text)
        config = _validate_workload(config,
                                    validator)  # type: ignore
        if not config:
            logging.error("🛑 Validation in `%s` failed. 🛑",
                          validator_text)
            return None
        logging.info("✅ Validation succeeded in `%s`.", validator_text)

    return config


def main(args: typing.Sequence[str]) -> int:
    logging.basicConfig(
        format="%(asctime)s | %(levelname)s | %(message)s",
        level=logging.INFO,
    )
    parser = argparse.ArgumentParser(description="Cortex Config Validator.")
    parser.add_argument("--config-file",
                        type=str,
                        required=False,
                        default=_DEFAULT_CONFIG_)
    parser.add_argument("--sub-validator",
                        action="append",
                        default=[],
                        required=False)
    params = parser.parse_args(args)

    logging.info("🦄 Running config validation and processing: %s.",
                 params.config_file)

    try:
        config = _load_config(params.config_file)
    except RuntimeError as ex:
        # Invalid json
        logging.exception(ex, exc_info=True)
        return 1

    if config.get("validated", False):
        logging.info("✅ Configuration in `%s` "
                     "was previously validated.",
                     params.config_file)
    else:
        config = validate_config(config,
                                list(params.sub_validator))
        if not config:
            logging.error("🛑🔪 Configuration in `%s` is invalid. 🔪🛑\n\n",
                        params.config_file)
            return 1
        config["validated"] = True
        _save_config(params.config_file, config)

    logging.info(("🦄 Configuration in `%s` was "
                  "successfully validated and processed. Let's roll! 🦄\n\n"),
                 params.config_file)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
