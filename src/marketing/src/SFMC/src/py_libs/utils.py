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

import copy
import csv
import json
import logging
import os
from pathlib import Path
from typing import List

from google.cloud.bigquery import Client
from google.cloud.bigquery import DatasetReference
from google.cloud.bigquery import SchemaField
from google.cloud.bigquery import Table
from google.cloud.bigquery import TableReference
from jinja2 import Environment
from jinja2 import FileSystemLoader

from common.py_libs.bq_helper import table_exists
from common.py_libs.bq_materializer import add_cluster_to_table_def
from common.py_libs.bq_materializer import add_partition_to_table_def

_TECHNICAL_FIELDS = {
    "RecordStamp": "TIMESTAMP",
    "SourceFileName": "STRING",
    "SourceFileLastUpdateTimeStamp": "TIMESTAMP"
}


def _add_technical_fields(input_schema: List[SchemaField]) -> List[SchemaField]:
    """Adds additional fields to BQ Schema."""
    output_schema = copy.deepcopy(input_schema)
    for column_name, column_type in _TECHNICAL_FIELDS.items():
        if column_name not in {field.name for field in input_schema}:
            output_schema.append(
                SchemaField(name=column_name, field_type=column_type))

    return output_schema


def _create_bq_schema_from_mapping(mapping_file: Path, layer: str,
                                   target_field: str,
                                   datatype_field: str) -> List[SchemaField]:
    """Generates BigQuery schema from mapping file.

    Args:
        mapping_file (Path): Schema mapping file path.
        layer (str): Target layer.
        target_field (str): Name of column with target field names.
        datatype_field (str): Name of column with target column datatypes.

    Return:
        BQ schema as list of fields and datatypes.
    """

    logging.info("Processing schema for %s", mapping_file)

    schema = []
    # Raw layer captures data as STRING.
    with open(mapping_file, encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f, delimiter=","):
            bq_type = row[datatype_field]
            if layer == "raw":
                bq_type = "STRING"

            schema.append(
                SchemaField(name=row[target_field], field_type=bq_type))

    return schema


def create_bq_schema(table_mapping_path: Path, layer: str):
    """Creates final BQ Schema.
    This function adds additional fields to schema created from mapping.

    Args:
        mapping_file (Path): Schema mapping file path.
        layer (str): Layer type indicator.
    """
    bq_schema = _create_bq_schema_from_mapping(table_mapping_path,
                                               layer,
                                               target_field="TargetField",
                                               datatype_field="DataType")
    return _add_technical_fields(bq_schema)


def create_table(client: Client, schema: List[SchemaField], project: str,
                 dataset: str, table_name: str, partition_details: dict,
                 cluster_details: dict):
    """Creates a table in BigQuery. Skips creation if it exists.

    Args:
        client (bigquery.Client): BQ client.
        schema (list[bigquery.SchemaField]): BQ schema.
        project (str): BQ project id.
        dataset (str): Destination dataset.
        table_name (str): Destination table name.
        partition_details (dict): Partition details from setting file.
        clustering_details (dict): Clustering details from setting file.
    """

    logging.info("Creating RAW table %s.%s.%s", project, dataset, table_name)

    table_ref = TableReference(DatasetReference(project, dataset), table_name)
    table = Table(table_ref, schema=schema)
    if partition_details:
        table = add_partition_to_table_def(table, partition_details)

    if cluster_details:
        table = add_cluster_to_table_def(table, cluster_details)

    client.create_table(table)


def _read_type_mapping(mapping_file: Path) -> dict:
    """Reads column names mapping for column rename step.

    Args:
        mapping_file (Path): Mapping file location.

    Returns:
        Dict: Mapping dictionary where keys are the target column names
        and the values are the target data types.
    """
    column_mapping = {}
    with open(mapping_file, encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f, delimiter=","):
            column_mapping[row["TargetField"]] = row["DataType"]

    technical_fileds = {col: "SYSTEM_FIELD" for col in _TECHNICAL_FIELDS}
    final_mapping = {**column_mapping, **technical_fileds}

    return final_mapping


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
    sfmc_column_type_mapping = _read_type_mapping(mapping_path)
    columns = list(sfmc_column_type_mapping.keys())
    column_subs = {
        "columns": columns,
        "sfmc_column_type_mapping": sfmc_column_type_mapping
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
