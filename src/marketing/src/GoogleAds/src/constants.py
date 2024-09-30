# Copyright 2023 Google LLC

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""A single place for the values that are used across Adwords related code."""

from pathlib import Path

from common.py_libs.configs import load_config_file
import yaml

_CURRENT_DIR = Path(__file__).resolve().parent
_CONFIG_PATH = Path(_CURRENT_DIR.parent.parent.parent, "config",
                    "config.json")

_RAW_TO_CDC_TABLES_SECTION = "raw_to_cdc_tables"

_SETTINGS_FILE = Path(_CURRENT_DIR.parent, "config", "ingestion_settings.yaml")

with open(_SETTINGS_FILE, encoding="utf-8") as settings_file:
    SETTINGS = yaml.load(settings_file, Loader=yaml.SafeLoader)
if not SETTINGS:
    raise SystemExit(f"ERROR: File '{_SETTINGS_FILE}' is empty. "
                     "The file must be present and filled.")

if not SETTINGS[_RAW_TO_CDC_TABLES_SECTION]:
    raise SystemExit(
        f"ERROR: List of {_RAW_TO_CDC_TABLES_SECTION} is empty. "
        "The list must be present and filled in the ingestion_settings.yaml.")

_PROJECT_CONFIG = load_config_file(_CONFIG_PATH)

_SOURCE_CONFIG = _PROJECT_CONFIG["marketing"]["GoogleAds"]

POPULATE_TEST_DATA = _PROJECT_CONFIG["testData"]

SCHEMA_DIR = Path(_CURRENT_DIR.parent, "config", "table_schema")

RAW_PROJECT = _PROJECT_CONFIG["projectIdSource"]
RAW_DATASET = _SOURCE_CONFIG["datasets"]["raw"]

CDC_PROJECT = _PROJECT_CONFIG["projectIdSource"]
CDC_DATASET = _SOURCE_CONFIG["datasets"]["cdc"]

PROJECT_LOCATION = _PROJECT_CONFIG["location"]
PROJECT_REGION = _PROJECT_CONFIG["marketing"]["dataflowRegion"]

__all__ = [
    "SCHEMA_DIR", "PROJECT_REGION", "SETTINGS", "RAW_PROJECT", "RAW_DATASET",
    "CDC_PROJECT", "CDC_DATASET", "PROJECT_LOCATION", "PROJECT_REGION"
]
