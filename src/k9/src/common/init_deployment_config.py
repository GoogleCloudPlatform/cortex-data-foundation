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
import re
import sys
from pathlib import Path
import typing

_DEFAULT_CONFIG_ = "config/config.json"
_VALIDATOR_FILE_NAME_ = "config_validator"
_VALIDATE_FUNC_NAME_ = "validate"
_DEFAULT_TEST_HARNESS_PROJECT = "kittycorn-public"

def _load_jsonc(file_obj):
    """Load JSONC file by removing comments and calling json.loads.

    Args:
        file_obj (fileobj): opened text file object

    Returns:
        dict: json dictionary
    """
    regex = re.compile(r"(\".*?\"|\'.*?\')|(/\*.*?\*/|//[^\r\n]*$)",
                       re.MULTILINE | re.DOTALL)

    def _re_sub(match):
        if match.group(2) is not None:
            return ""
        else:
            return match.group(1)

    data_raw = file_obj.read()
    data = regex.sub(_re_sub, data_raw)
    stripped = regex.sub(_re_sub, data)
    return json.loads(stripped)


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
        raise FileNotFoundError(f"Config file '{config_file}' does not exist.")
    with open(config_file, mode="r", encoding="utf-8") as cf:
        try:
            config = _load_jsonc(cf)
        except json.JSONDecodeError:
            e_msg = f"🛑🔪 Config file '{config_file}' is malformed or empty. 🛑🔪"
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
        config (dict): Configuration dictionary
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
        logging.error("ERROR: 🛑🔪 No config validator for %s. Missing %s 🔪🛑",
                      workload_path, str(full_file_path))
        return None
    logging.info("Found %s.py in %s. Running 'validate'.",
                 _VALIDATOR_FILE_NAME_, validator_dir)

    spec = spec_from_file_location(_VALIDATOR_FILE_NAME_, full_file_path)
    module = module_from_spec(spec)  # type: ignore
    spec.loader.exec_module(module)  # type: ignore

    if not hasattr(module, _VALIDATE_FUNC_NAME_):
        logging.error("ERROR: 🛑🔪 %s doesn't have %s function. 🔪🛑",
                      str(full_file_path), _VALIDATE_FUNC_NAME_)
        return None

    validate_func = getattr(module, _VALIDATE_FUNC_NAME_)
    return validate_func(config)


def validate_config(
    config: typing.Dict[str, any],  # type: ignore
    sub_validators: typing.List[str]  # type: ignore
) -> typing.Optional[typing.Dict[str, any]]:  # type: ignore
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
            "ERROR: 🛑🔪 Missing 'projectIdSource' configuration value. 🔪🛑")
        return None

    if not config.get("targetBucket", None):
        logging.error(
            "ERROR: 🛑🔪 Missing 'targetBucket' configuration value. 🔪🛑")
        return None

    config["projectIdTarget"] = config.get("projectIdTarget", "")
    if not config["projectIdTarget"]:
        logging.warning(("projectIdTarget is not specified."
                         "Using projectIdSource (%s)"),
                        config["projectIdSource"])
        config["projectIdTarget"] = config["projectIdSource"]

    config["deploySAP"] = config.get("deploySAP", False)
    config["deploySFDC"] = config.get("deploySFDC", False)
    config["deployMarketing"] = config.get("deployMarketing", False)
    config["testData"] = config.get("testData", False)
    config["turboMode"] = config.get("turboMode", True)

    config["location"] = config.get("location", None)
    if not config["location"]:
        config["location"] = "US"
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
        logging.warning(("No K9 datasets configuration. "
                         "Defaulting to K9_PROCESSING and K9_REPORTING."))
        config["k9"]["datasets"] = {}
    config["k9"]["datasets"]["processing"] = config["k9"]["datasets"].get(
        "processing", "K9_PROCESSING")
    config["k9"]["datasets"]["reporting"] = config["k9"]["datasets"].get(
        "reporting", "K9_REPORTING")

    # Go over all sub-validator directories,
    # and call validate() in config_validator.py in every directory.
    for validator in sub_validators:
        validator_text = validator if validator else "current repository"
        logging.info("Running config validator in `%s`.", validator_text)
        config = _validate_workload(config, validator)  # type: ignore
        if not config:
            logging.error("ERROR: 🛑🔪 Validation in `%s` failed. 🔪🛑",
                          validator_text)
            return None
        logging.info("Validation succeeded in `%s`.", validator_text)

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

    config = validate_config(config, list(params.sub_validator))
    if not config:
        logging.error("ERROR: 🛑🔪 Configuration in `%s` is invalid. 🔪🛑",
                      params.config_file)
        return 1

    _save_config(params.config_file, config)
    logging.info(("🦄 Configuration in `%s` has been "
                  "successfully validated and processed. Let's roll! 🦄\n\n"),
                 params.config_file)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
