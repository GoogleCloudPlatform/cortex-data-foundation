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
"""Shared values for GoogleAds CDC layer."""

from pathlib import Path

_current_dir = Path(__file__).resolve().parent

# Template files
DAG_TEMPLATE_DIR = Path(_current_dir, "templates")
DAG_TEMPLATE_PATH = Path(DAG_TEMPLATE_DIR, "raw_to_cdc_dag_py_template.py")
CDC_SQL_TEMPLATE_DIR = Path(DAG_TEMPLATE_DIR, "sql")
CDC_SQL_TEMPLATE_PATH = Path(CDC_SQL_TEMPLATE_DIR,
                             "raw_to_cdc_sql_template.sql")

# Directories under which all the generated dag files and related files
# will be stored.
_output_dir_for_airflow = Path(_current_dir.parent.parent, "_generated_dags")

# CDC constants
OUTPUT_DIR_FOR_CDC = Path(_output_dir_for_airflow, "cdc")
CDC_SQL_SCRIPTS_OUTPUT_DIR = Path(OUTPUT_DIR_FOR_CDC, "cdc_sql_scripts")

__all__ = [
    "OUTPUT_DIR_FOR_CDC", "CDC_SQL_SCRIPTS_OUTPUT_DIR", "CDC_SQL_TEMPLATE_PATH",
    "DAG_TEMPLATE_PATH"
]
