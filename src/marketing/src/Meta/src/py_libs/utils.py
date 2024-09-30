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
"""Utility functions for working with BQ Tables and mapping files."""

import csv
import logging
from pathlib import Path

from google.cloud.bigquery import SchemaField

from src.constants import SCHEMA_BQ_DATATYPE_FIELD
from src.constants import SCHEMA_TARGET_FIELD

_SYSTEM_FIELDS = {
    "recordstamp": "TIMESTAMP",
    "report_date": "DATE",
}


def read_raw_bq_schema(mapping_file: Path) -> list[SchemaField]:
    """Reads BQ Schema from file and adds additional fields.

    Args:
        mapping_file (Path): Schema mapping file path.
    Return:
        BQ schema as a list of fields and datatypes.
    """
    bq_schema = []
    # Generates raw layer BigQuery schema from mapping file.
    with open(mapping_file, encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f, delimiter=","):
            # All input values in raw layer schema are string.
            field = SchemaField(name=row[SCHEMA_TARGET_FIELD],
                                field_type="STRING")
            bq_schema.append(field)

    # Adds additional fields to BQ Schema.
    if "report_date" not in {field.name for field in bq_schema}:
        bq_schema.append(SchemaField(name="report_date", field_type="DATE"))

    if "recordstamp" not in {field.name for field in bq_schema}:
        bq_schema.append(SchemaField(name="recordstamp",
                                     field_type="TIMESTAMP"))

    logging.debug("\n".join([repr(field) for field in bq_schema]))

    return bq_schema


def read_cdc_bq_schema(mapping_file: Path) -> list[SchemaField]:
    """Creates BQ Schema from mapping file and adds additional fields.

    Args:
        mapping_file (Path): Schema mapping file path.
    Returns:
        BQ schema as a list of fields and datatypes.
    """
    bq_schema = []
    # Generates CDC layer BigQuery schema from mapping file.
    with open(mapping_file, encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f, delimiter=","):
            field = SchemaField(name=row[SCHEMA_TARGET_FIELD],
                                field_type=row[SCHEMA_BQ_DATATYPE_FIELD])
            bq_schema.append(field)

    field_names = {field.name for field in bq_schema}

    if "report_date" not in field_names:
        bq_schema.append(SchemaField(name="report_date", field_type="DATE"))

    if "recordstamp" not in field_names:
        bq_schema.append(SchemaField(name="recordstamp",
                                     field_type="TIMESTAMP"))

    logging.debug("\n".join([repr(field) for field in bq_schema]))

    return bq_schema


def read_field_type_mapping(mapping_file: Path) -> dict:
    """Read field name to target data type mappings from file.

    Args:
        mapping_file (Path): Mapping file location.

    Returns:
        Dict: Mapping dictionary where keys are the target field names
        and the values are the target data types.
    """
    field_mapping = {}
    with open(mapping_file, encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f, delimiter=","):
            field_mapping[row["TargetField"]] = row["DataType"]

    field_mapping = {**field_mapping, **_SYSTEM_FIELDS}

    return field_mapping
