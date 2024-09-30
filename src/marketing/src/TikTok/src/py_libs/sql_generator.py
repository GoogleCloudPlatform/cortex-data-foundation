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
"""Functions for rendering Jinja CDC SQL templates and writing them to target
directory."""

from collections import defaultdict
import csv
from dataclasses import dataclass
import json
import logging
from pathlib import Path
from typing import Any, Dict, List

from google.cloud.bigquery import SchemaField
from jinja2 import Environment
from jinja2 import FileSystemLoader

from src.constants import SYSTEM_FIELDS

logger = logging.getLogger(__name__)


class TableNotFoundError(KeyError):
    """Error in case of the table was not found in the actual dataset."""
    pass


@dataclass
class FieldMap:
    """ Class for keeping track of a datatype for target_field."""
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


def repr_schema(schema: List[SchemaField]) -> str:
    """Represents schema for debug purposes."""
    "\n".join([repr(field) for field in schema])


def generate_casting_statement(schema: dict) -> str:
    """Generates SQL transformations of nested columns from dictionary schema.

    Args:
        schema: Schema in dictionary format.
    """

    fields = []
    for value in schema.values():
        name = value.target_field
        data_type = value.data_type
        if data_type == "DATE":
            select_column = (f"DATE(PARSE_DATETIME("
                             f"'%Y-%m-%d %H:%M:%S', {name})) AS {name}")
        else:
            select_column = f"CAST({name} AS {data_type}) AS {name}"
        fields.append(select_column + "\n      ")
    if "recordstamp" not in fields:
        fields.append("recordstamp" + "\n      ")
    stmt_columns = ""
    for item in fields:
        stmt_columns = stmt_columns + item + ","
    columns = stmt_columns[:-8]
    return columns


def render_template_file(template_path: Path, mapping_path: Path,
                         subs: Dict[str, Any]) -> str:
    """Renders template from given Jinja .sql template file.

    It combines the given substitutions for Jinja template with the columns
    from mapping configs and system fields.

    Args:
        template_path (Path): Path of the processed Jinja template.
        mapping_path (Path): CSV file which contains the columns and data types.
        subs (Dict[str, Any]): Template variables.

    Returns:
        str: Rendered SQL script to write out.
    """

    logger.debug("Rendering Jinja template file: '%s' ", template_path)

    env = Environment(
        loader=FileSystemLoader(str(template_path.parent.absolute())))
    input_template = env.get_template(template_path.name)

    dict_mapping = convert_csv_mapping_to_dict(path=mapping_path)

    field_names_with_datatype = generate_casting_statement(dict_mapping)

    sys_field_names = list(SYSTEM_FIELDS)

    columns = [v.target_field for _, v in dict_mapping.items()
              ] + sys_field_names

    column_subs = {
        "columns": columns,
        "field_names_with_datatype": field_names_with_datatype
    }

    final_subs = {**subs, **column_subs}

    logger.debug("Jinja variables: %s", json.dumps(final_subs, indent=4))

    output_sql = input_template.render(final_subs)

    logger.debug("Generated SQL from template: \n%s", output_sql)

    return output_sql


def write_generated_sql_to_disk(path: Path, generated_sql: Any) -> None:
    """Writes generated SQL object to the given path."""
    with open(path, "w", encoding="utf-8") as f:
        f.write(generated_sql)
