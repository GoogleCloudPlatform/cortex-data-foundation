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
"""Shared values for Salesforce Marketing Cloud CDC layer."""

from pathlib import Path

_CURRENT_DIR = Path(__file__).resolve().parent

# Template files.
_DAG_TEMPLATE_DIR = Path(_CURRENT_DIR, "templates")
DAG_TEMPLATE_PATH = Path(_DAG_TEMPLATE_DIR, "raw_to_cdc_dag_py_template.py")
CDC_SQL_TEMPLATE_PATH = Path(_DAG_TEMPLATE_DIR, "sql",
                             "sfmc_raw_to_cdc_template.sql")

# Directory under which all the generated dag files and related files
# will be stored.
_OUTPUT_DIR_FOR_AIRFLOW = Path(_CURRENT_DIR.parent.parent, "_generated_dags")

# CDC constants.
OUTPUT_DIR_FOR_CDC = Path(_OUTPUT_DIR_FOR_AIRFLOW, "cdc")
CDC_SQL_SCRIPTS_OUTPUT_DIR = Path(OUTPUT_DIR_FOR_CDC, "cdc_sql_scripts")

SOURCE_TIMEZONE = "Etc/GMT+6"
