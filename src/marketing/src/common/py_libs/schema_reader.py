# Copyright 2024 Google LLC

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License foSr the specific language governing permissions and
# limitations under the License.
"""Utility functions for working with BQ Tables and mapping files."""

import csv
import logging
from pathlib import Path

from google.cloud.bigquery import SchemaField


def read_field_type_mapping(mapping_file: Path, schema_target_field: str,
                            system_fields: dict[str, str],
                            schema_bq_datatype_field = None) -> dict:
    """Read field name to target data type mappings from file.

    Args:
        mapping_file (Path): Mapping file location.
        schema_target_field (str): Name of the column in mapping file with
            target field names.
        system_fields (dict[str, str]): Dict with system field names and
            datatypes.
        schema_bq_datatype_field: Name of the column in mapping file with
            target datatypes.

    Returns:
        Dict: Mapping dictionary where keys are the target field names
        and the values are the target data types.
    """
    field_mapping = {}
    with open(mapping_file, encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f, delimiter=","):

            # All input values in raw layer schema are string.
            if schema_bq_datatype_field:
                field_type = row[schema_bq_datatype_field]
            else:
                field_type = "STRING"

            field_mapping[row[schema_target_field]] = field_type

    field_mapping = {**field_mapping, **system_fields}

    return field_mapping


def read_bq_schema(mapping_file: Path,
                   schema_target_field: str,
                   system_fields: dict[str, str],
                   schema_bq_datatype_field = None) -> list[SchemaField]:
    """Reads BQ Schema from file and adds additional fields.

    Args:
        mapping_file (Path): Schema mapping file path.
        schema_target_field (str): Name of the column in mapping file with
            target field names.
        system_fields (dict[str, str]): Dict with system field names and
            datatypes.
        schema_bq_datatype_field: Name of the column in mapping file with
            target datatypes.
    Return:
        BQ schema as a list of fields and datatypes.
    """
    bq_schema = []

    field_mapping = read_field_type_mapping(mapping_file, schema_target_field,
                                            system_fields,
                                            schema_bq_datatype_field)

    for column_name, column_type in field_mapping.items():
        bq_schema.append(SchemaField(name=column_name, field_type=column_type))

    logging.debug("\n".join([repr(field) for field in bq_schema]))

    return bq_schema
