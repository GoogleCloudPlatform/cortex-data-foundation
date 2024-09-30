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
"""Utilities RAW and CDC layer deployment scrips."""

from collections import defaultdict
import copy
import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

from common.py_libs.dag_generator import generate_file_from_template
from google.cloud.bigquery import DatasetReference
from google.cloud.bigquery import SchemaField
from google.cloud.bigquery import Table
from google.cloud.bigquery import TableReference

from src.constants import SYSTEM_FIELDS

_TARGET_FIELD = "TargetField"
_BQ_DATATYPE_FIELD = "DataType"


class TableNotFoundError(KeyError):
    """Error in case of the table was not found in the actual dataset."""
    pass


@dataclass
class FieldMap:
    target_field: str
    data_type: str


def convert_csv_mapping_to_dict(path: str) -> Dict[str, FieldMap]:
    """This method will load CSV file and the convert to a dict so developer
    can easily look for target fields and data types with autocomplete help.

    Args:
        path (str): Path for mapping file

    Returns:
        dict[str, FieldMap]: Mapping could be used like dict['ID'].TargetField
        or dict['ID'].DataType which is going to help mapping.
    """

    map_dict = defaultdict()
    with open(path, "r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            map_dict[row["SourceField"]] = FieldMap(row["TargetField"],
                                                    row["DataType"])
    return map_dict


def _add_system_fields(input_schema: list[SchemaField]) -> list[SchemaField]:
    """Add system fields which are required for CDC logic."""

    output_schema = copy.deepcopy(input_schema)

    set_of_input_fields = {field.name for field in input_schema}

    for system_field, field_type in SYSTEM_FIELDS.items():
        if system_field not in set_of_input_fields:
            output_schema.append(
                SchemaField(name=system_field, field_type=field_type))

    return output_schema


def repr_schema(schema: List[SchemaField]):
    """Represents schema for debug purposes."""
    "\n".join([repr(field) for field in schema])


def create_bq_schema(mapping_filepath: Path) -> list[SchemaField]:
    schema = []
    with open(mapping_filepath, encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f, delimiter=","):
            schema.append(
                SchemaField(name=row[_TARGET_FIELD],
                            field_type=row[_BQ_DATATYPE_FIELD]))

    return _add_system_fields(schema)


def create_table_ref(schema: list[SchemaField], project: str, dataset: str,
                     table_name: str):
    """Creates BigQuery table reference based on given schema."""

    table_ref = TableReference(DatasetReference(project, dataset), table_name)
    table = Table(table_ref, schema=schema)

    return table


def generate_dag_from_template(template_file: Path,
                               generation_target_directory: Path,
                               project_id: str, dataset_id: str,
                               table_name: str, layer: str, subs: dict):
    """Generates DAG code from template file.

    Each DAG is responsible for loading one table.

    Args:
        template_file (Path): Path of the template file.
        generation_target_directory (Path): Directory where files are generated.
        project_id (str): Target project id of the dag.
        dataset_id (str): Target dataset id of the dag.
        table_name (str): The table name which is loaded by this dag.
        layer: (str): 'extract_to_raw' or 'raw_to_cdc'
        subs (dict): template variables
    """
    output_dag_py_file = Path(
        generation_target_directory,
        f"{project_id}_{dataset_id}_{layer}_{table_name.replace('.', '_')}.py")

    generate_file_from_template(template_file, output_dag_py_file, **subs)
