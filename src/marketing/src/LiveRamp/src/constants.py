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
"""Contains methods for working with BQ Tables and template files. """

from pathlib import Path

from common.py_libs.configs import load_config_file

_CURRENT_DIR = Path(__file__).resolve().parent
_CONFIG_PATH = Path(_CURRENT_DIR.parent.parent.parent, "config", "config.json")

_PROJECT_CONFIG = load_config_file(_CONFIG_PATH)
_SOURCE_CONFIG = _PROJECT_CONFIG["marketing"]["LiveRamp"]

POPULATE_TEST_DATA = _PROJECT_CONFIG["testData"]

PROJECT = _PROJECT_CONFIG["projectIdSource"]
DATASET = _SOURCE_CONFIG["datasets"]["cdc"]

PROJECT_LOCATION = _PROJECT_CONFIG["location"]

_DAG_TEMPLATE_DIR = Path(_CURRENT_DIR, "templates")
DAG_TEMPLATE_FILE = Path(_DAG_TEMPLATE_DIR, "source_to_bq_dag_py_template.py")

_DDLS_DIR = Path(_CURRENT_DIR, "ddls")
_SQL_RAMPID_LOOKUP_INPUT = Path(_DDLS_DIR, "rampid_lookup_input.sql")
_SQL_RAMPID_LOOKUP = Path(_DDLS_DIR, "rampid_lookup.sql")
DDL_SQL_FILES = [_SQL_RAMPID_LOOKUP_INPUT, _SQL_RAMPID_LOOKUP]

# Directory that has all the dependencies for python dag code
DEPENDENCIES_INPUT_DIR = Path(_CURRENT_DIR, "pipelines")

# Directories under which all the generated dag files and related files
# will be stored.
OUTPUT_DIR = Path(_CURRENT_DIR.parent, "_generated_dags")

DEPENDENCIES_OUTPUT_DIR = Path(OUTPUT_DIR, "pipelines")

__all__ = [
    "PROJECT", "DATASET", "PROJECT_LOCATION", "DEPENDENCIES_INPUT_DIR",
    "OUTPUT_DIR", "DEPENDENCIES_OUTPUT_DIR", "DDL_SQL_FILES"
]
