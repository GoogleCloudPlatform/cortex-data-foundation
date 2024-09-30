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
"""Directories used to build generated DAG model structure."""

from pathlib import Path

# Works as reference for other variables to build directory structure.
# It needs to be next to files that are importing this location variables.
_CURRENT_DIR = Path(__file__).resolve().parent

# Template files
DAG_TEMPLATE_DIR = Path(_CURRENT_DIR, "templates")
DAG_TEMPLATE_FILE = Path(DAG_TEMPLATE_DIR, "source_to_raw_dag_py_template.py")

REQUESTS_DIR = Path(_CURRENT_DIR.parent.parent, "config", "request_parameters")

# Directories under which all the generated dag files and related files
# will be stored.
_OUTPUT_DIR_FOR_AIRFLOW = Path(_CURRENT_DIR.parent.parent, "_generated_dags")

OUTPUT_DIR_FOR_RAW = Path(_OUTPUT_DIR_FOR_AIRFLOW, "raw")
SCHEMAS_OUTPUT_DIR = Path(OUTPUT_DIR_FOR_RAW, "table_schema")
REQUESTS_OUTPUT_DIR = Path(OUTPUT_DIR_FOR_RAW, "api_requests")
DEPENDENCIES_OUTPUT_DIR = Path(OUTPUT_DIR_FOR_RAW, "pipelines")
DEPENDENCIES_INPUT_DIR = Path(_CURRENT_DIR, "pipelines")
