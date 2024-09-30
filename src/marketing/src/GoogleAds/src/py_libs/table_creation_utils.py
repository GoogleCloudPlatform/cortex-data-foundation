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
""" Contains methods for working with BQ Tables and mapping files. """

import copy
import csv
from pathlib import Path
from typing import List

from google.cloud.bigquery import SchemaField


def _create_bq_schema_from_mapping(mapping_file: Path, target_field: str,
                                   datatype_field: str) -> list[SchemaField]:
    """Generates BigQuery schema from mapping file.

    Args:
        mapping_file (Path): Schema mapping file path.
        target_field (str): Name of column with target field names.
        datatype_field (str): Name of column with target column datatypes.

    Return:
        BQ schema as list of fields and datatypes.
    """
    raw_mapping = []
    with open(mapping_file, encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f, delimiter=","):
            raw_mapping.append(row)

    schema = []
    for row in raw_mapping:
        if len(row[target_field].split(".")) > 1:
            # Nested field are STRING in RAW layer.
            type_ = "STRING"
        else:
            if row[datatype_field] in ["DATETIME", "DATE"]:
                type_ = "STRING"
            else:
                type_ = row[datatype_field]

        field_name = row[target_field].split(".")[0].replace("[]", "")

        field = SchemaField(name=field_name, field_type=type_)

        # TODO: Investigate the removal of this check.
        if field not in schema:
            schema.append(field)

    return schema


def _add_additional_fields(
        input_schema: list[SchemaField]) -> list[SchemaField]:
    """Adds additional fields to BQ Schema."""
    output_schema = copy.deepcopy(input_schema)
    if "recordstamp" not in {field.name for field in input_schema}:
        output_schema.append(
            SchemaField(name="recordstamp", field_type="TIMESTAMP"))
    return output_schema


def create_bq_schema(mapping_file: Path) -> list[SchemaField]:
    """Creates final BQ Schema.
    This function adds additional fields to schema created from mapping.

    Args:
        mapping_file (Path): Schema mapping file path.
    """
    bq_schema = _create_bq_schema_from_mapping(mapping_file,
                                               target_field="TargetField",
                                               datatype_field="DataType")
    return _add_additional_fields(bq_schema)


def repr_schema(schema: List[SchemaField]):
    """Represents schema for debug purposes."""
    "\n".join([repr(field) for field in schema])


class TableNotFoundError(KeyError):
    """Error in case of the table was not found in the actual dataset."""
    pass
