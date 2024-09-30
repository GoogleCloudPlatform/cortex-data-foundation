# Copyright 2024 Google LLC

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Directories, Buckets, Datasets, Path and other constants needed and used to
build generated DAG model structure.
"""

from pathlib import Path

import yaml

from common.py_libs.configs import load_config_file

# Works as reference for other variables to build directory structure.
# It needs to be next to files that are importing this location variables.
_CURRENT_DIR = Path(__file__).resolve().parent

SETTINGS_FILE = Path(_CURRENT_DIR.parent, "config", "ingestion_settings.yaml")

with open(SETTINGS_FILE, encoding="utf-8") as settings_file:
    SETTINGS = yaml.load(settings_file, Loader=yaml.SafeLoader)
if not SETTINGS:
    raise SystemExit(f"ERROR: File '{SETTINGS_FILE}' is empty. "
                     "Make sure the file exists with correct content.")

SCHEMA_DIR = Path(_CURRENT_DIR.parent, "config", "table_schema")

_CONFIG_PATH = Path(_CURRENT_DIR.parent.parent.parent, "config", "config.json")
_PROJECT_CONFIG = load_config_file(_CONFIG_PATH)
_SOURCE_CONFIG = _PROJECT_CONFIG["marketing"]["Meta"]

POPULATE_TEST_DATA = _PROJECT_CONFIG["testData"]
PROJECT_REGION = _PROJECT_CONFIG["marketing"]["dataflowRegion"]
RAW_PROJECT = _PROJECT_CONFIG["projectIdSource"]
RAW_DATASET = _SOURCE_CONFIG["datasets"]["raw"]

CDC_PROJECT = _PROJECT_CONFIG["projectIdSource"]
CDC_DATASET = _SOURCE_CONFIG["datasets"]["cdc"]

SCHEMA_TARGET_FIELD = "TargetField"
SCHEMA_BQ_DATATYPE_FIELD = "DataType"

SYSTEM_FIELDS = {"recordstamp": "TIMESTAMP", "report_date": "DATE"}
