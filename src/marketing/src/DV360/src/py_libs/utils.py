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
"""Utility functions for BQ table creation."""

import json
import logging
import os
from pathlib import Path

from google.cloud.bigquery import Client

from common.py_libs.bq_helper import table_exists
from common.py_libs.jinja import apply_jinja_params_dict_to_file


def generate_template_file(template_path: Path, target_path: Path, subs: dict):
    """Renders template from given Jinja .sql template file to a file.

    It combines the given substitutions for Jinja template with the columns
    from mapping configs and system fields.

    Args:
        template_path (Path): Path of the processed Jinja template.
        target_path (Path): Result .sql file location.
        subs (Dict[str, Any]): Template variables.

    """

    logging.debug("Rendering Jinja template file: '%s' ", template_path)

    logging.debug("Jinja variables: %s", json.dumps(subs, indent=4))

    output_sql = apply_jinja_params_dict_to_file(template_path, subs)

    os.makedirs(os.path.dirname(target_path), exist_ok=True)
    with open(target_path, "w", encoding="utf-8") as f:
        f.write(output_sql)

def populate_test_data(client: Client, path_to_script: Path,
                       full_table_name: str) -> None:
    """Loads test data from RAW to CDC."""

    if not table_exists(bq_client=client, full_table_name=full_table_name):
        raise RuntimeError(f"Test table {full_table_name} is not found!")

    with open(path_to_script, mode="r", encoding="utf-8") as query_file:
        sql = query_file.read()
        populate_cdc_table_job = client.query(query=sql)
        populate_cdc_table_job.result()
