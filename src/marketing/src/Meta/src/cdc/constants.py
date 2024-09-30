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

_CURRENT_DIR = Path(__file__).resolve().parent

# Template files
_DAG_TEMPLATE_DIR = Path(_CURRENT_DIR, "templates")
DAG_TEMPLATE_PATH = Path(_DAG_TEMPLATE_DIR, "raw_to_cdc_dag_py_template.py")

CDC_SQL_TEMPLATE_PATH = Path(_DAG_TEMPLATE_DIR, "sql",
                             "raw_to_cdc_sql_template.sql")

# Directories under which all the generated dag files and related files
# will be stored.
_AIRFLOW_OUTPUT_DIR = Path(_CURRENT_DIR.parent.parent, "_generated_dags")

# CDC constants.
CDC_OUTPUT_DIR = Path(_AIRFLOW_OUTPUT_DIR, "cdc")
CDC_SQL_OUTPUT_DIR = Path(CDC_OUTPUT_DIR, "cdc_sql_scripts")

# DAG config file.
_DAG_CONFIG_FILE = "config.ini"
DAG_CONFIG_INI_INPUT_PATH = Path(_CURRENT_DIR, _DAG_CONFIG_FILE)
DAG_CONFIG_INI_OUTPUT_PATH = Path(CDC_OUTPUT_DIR, _DAG_CONFIG_FILE)
