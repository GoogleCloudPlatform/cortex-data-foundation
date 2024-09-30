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
from typing import List

from common.py_libs.bq_helper import table_exists
from common.py_libs.schema_reader import read_field_type_mapping
from google.cloud.bigquery import Client
from jinja2 import Environment
from jinja2 import FileSystemLoader

from src.constants import SCHEMA_BQ_DATATYPE_FIELD
from src.constants import SCHEMA_TARGET_FIELD
from src.constants import SYSTEM_FIELDS


def generate_template_file(template_path: Path, mapping_path: Path,
                           target_path: Path, subs: dict):
    """Renders template from given Jinja .sql template file to a file.

    It combines the given substitutions for Jinja template with the columns
    from mapping configs and system fields.

    Args:
        template_path (Path): Path of the processed Jinja template.
        mapping_path (Path): CSV file which contains the columns and data types.
        target_path (Path): Result .sql file location.
        subs (Dict[str, Any]): Template variables.

    """

    logging.debug("Rendering Jinja template file: '%s' ", template_path)

    loader = FileSystemLoader(str(template_path.parent.absolute()))
    env = Environment(loader=loader)
    input_template = env.get_template(template_path.name)
    sfmc_column_type_mapping = read_field_type_mapping(
                                    mapping_file=mapping_path,
                                    schema_target_field=SCHEMA_TARGET_FIELD,
                                    system_fields=SYSTEM_FIELDS,
                                    schema_bq_datatype_field=\
                                        SCHEMA_BQ_DATATYPE_FIELD)

    columns = list(sfmc_column_type_mapping.keys())
    column_subs = {
        "columns": columns,
        "sfmc_column_type_mapping": sfmc_column_type_mapping,
        "system_fields": SYSTEM_FIELDS
    }
    final_subs = {**subs, **column_subs}

    logging.debug("Jinja variables: %s", json.dumps(final_subs, indent=4))

    output_sql = input_template.render(final_subs)

    logging.debug("Generated SQL from template: \n%s", output_sql)

    os.makedirs(os.path.dirname(target_path), exist_ok=True)
    with open(target_path, "w", encoding="utf-8") as f:
        f.write(output_sql)


def populate_test_data(client: Client, path_to_script: Path,
                       full_table_name: str) -> None:
    """Loads test data from RAW to CDC.

    Also checks CDC test table is empty to avoid existing data loss."""

    if not table_exists(bq_client=client, full_table_name=full_table_name):
        raise RuntimeError(f"Test table {full_table_name} is not found!")

    with open(path_to_script, mode="r", encoding="utf-8") as query_file:
        sql = query_file.read()
        populate_cdc_table_job = client.query(query=sql)
        populate_cdc_table_job.result()


def repr_schema(schema: List):
    """Represents schema for debug purposes."""
    "\n".join([repr(field) for field in schema])
