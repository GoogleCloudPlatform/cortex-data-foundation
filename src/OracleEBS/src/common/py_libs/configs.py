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
#
"""Library for Cortex Config related functions."""

import functools
import json
import logging
from pathlib import Path
from typing import Any
from typing import Dict

from common.py_libs import constants
from common.py_libs import cortex_exceptions as cortex_exc

logger = logging.getLogger(__name__)

def load_config_file(config_file: str,
                     log_config: bool = True) -> Dict[str, Any]:
    """Loads a json config file to a dictionary.

    Args:
        config_file (str): Path of config json file.
        log_config (bool, optional): Option to print config file to log.
            Defaults to True.

    Returns:
        Config as a dictionary.

    Raises:
        FileNotFoundError: If the config file does not exist.
        cortex_exc.CriticalError: If the config file is malformed.
    """
    logger.debug("Input file = %s", config_file)

    if not Path(config_file).is_file():
        raise FileNotFoundError(f"Config file '{config_file}' does not exist.")

    with open(config_file, mode="r", encoding="utf-8") as f:
        try:
            config = json.load(f)
        except json.JSONDecodeError:
            e_msg = f"Config file '{config_file}' is malformed or empty."
            raise cortex_exc.CriticalError(e_msg) from json.JSONDecodeError

        if log_config:
            logger.info("Using the following config:\n %s",
                        json.dumps(config, indent=4))

    return config

# Caches config so we don't load it again if already loaded
@functools.cache
def load_config_file_from_env() -> Dict[str, Any]:
    """
    Loads the config JSON file from the ${_CONFIG_FILE} environment variable.

    Calls load_config_file() to load the config file and caches the result.
    Calls with the log_config False argument so as to not log
    the config file excessively.

    Returns:
        Config as a dictionary from the load_config_file() function.

    Raises:
        FileNotFoundError: If the .env file does not exist.
        cortex_exc.CriticalError: If the .env file is malformed.
    """
    # Get config file path from .env file
    if not Path(constants.CONFIG_FILE_FULL_PATH_ENV).is_file():
        e_msg = (f"The .env file, {constants.CONFIG_FILE_FULL_PATH_ENV}, "
                "does not exist.")
        raise FileNotFoundError(e_msg) from None

    with open(constants.CONFIG_FILE_FULL_PATH_ENV,
              mode="r", encoding="utf-8") as f:
        try:
            config_file = f.read().strip()
        except OSError:
            e_msg = ("The .env file "
                     f"{constants.CONFIG_FILE_FULL_PATH_ENV} is malformed.")
            raise cortex_exc.CriticalError(e_msg) from None

    return load_config_file(config_file, log_config=False)
