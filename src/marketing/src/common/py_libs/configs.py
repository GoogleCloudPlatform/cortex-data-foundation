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
from typing import Any, Dict

logger = logging.getLogger(__name__)


def load_config_file(config_file) -> Dict[str, Any]:
    """Loads a json config file to a dictionary.

    Args:
        config_file: Path of config json file.

    Returns:
        Config as a dictionary.
    """
    logger.debug("Input file = %s", config_file)

    if not Path(config_file).is_file():
        raise FileNotFoundError(f"Config file '{config_file}' does not exist.")

    with open(config_file, mode="r", encoding="utf-8") as f:
        try:
            config = json.load(f)
        except json.JSONDecodeError:
            e_msg = f"Config file '{config_file}' is malformed or empty."
            raise Exception(e_msg) from None

        logger.info("Using the following config:\n %s",
                    json.dumps(config, indent=4))

    return config
