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
""" Library for Schema Generator"""

import csv
from pathlib import Path

from google.cloud.bigquery import SchemaField


def _sanitize_sql_str(sql: str) -> str:
    return sql.replace('"','').replace('\\','').replace(", '",', ').\
        replace("', ",', ').replace('|','"')


def generate_schema(schema_file: Path, bq_type: str) -> dict:
    """Creates dictionary format schema from csv mapping.

    Args:
        schema_file: Full csv mapping file path.
        bq_type: Type of BQ object for which the schema is generated.
    """
    schema = {}
    with open(
            schema_file,
            mode='r',
            encoding='utf-8',
            newline='',
    ) as csv_file:
        for row in csv.DictReader(csv_file, delimiter=','):
            name = row['TargetField']
            if row['DataType'] == 'ARRAY' and bq_type == 'table':
                data_type = 'STRING'
            else:
                data_type = row['DataType']
            name_parts = name.split('.')
            current = schema
            for part in (name_parts[:-1]):
                if part not in current:
                    current[part] = {}
                current = current[part]
            current[name_parts[-1]] = data_type
        if 'recordstamp' not in schema and bq_type == 'table':
            schema['recordstamp'] = 'TIMESTAMP'
        return schema


def convert_to_bq_schema(schema: dict) -> list[SchemaField]:
    """Creates BigQuery schema from dictionary schema.

    Args:
        schema: Schema in dictionary format.
    """
    fields = []
    for name, value in schema.items():
        if isinstance(value, dict):
            if '[]' in name:
                name = name.replace('[]', '')
                fields.append(
                    SchemaField(name,
                                'RECORD',
                                mode='REPEATED',
                                fields=convert_to_bq_schema(value)))
            else:
                fields.append(
                    SchemaField(name,
                                'RECORD',
                                fields=convert_to_bq_schema(value)))
        else:
            if '[]' in name:
                name = name.replace('[]', '')
                fields.append(SchemaField(name, value, mode='REPEATED'))
            else:
                fields.append(SchemaField(name, value))
    return fields


def generate_column_list(schema: dict) -> list[str]:
    """Creates a list of SQL statements from dictionary schema
        which will be used to select the columns from BigQuery

    Args:
        schema: Schema in dictionary format.

    Returns:
        list[str]: list of sql statements for columns
    """
    columns = [_sanitize_sql_str(column) for column in generate_code(schema)]
    columns.append('recordstamp')
    return columns


def generate_code(schema: dict,
                  level=0,
                  root_item='',
                  nested_item='',
                  domains=None) -> list[str]:
    """Generate SQL transformations of nested columns from dictionary schema.

    Args:
        schema: Schema in dictionary format.
        global_level: Level of column in schema.
        root_item: Root item for current item.
        nested_item: Nested item for current item

    Returns:
        list[str]: list of sql statements for columns
    """
    fields = []
    if domains is None:
        domains = []
    for name, value in schema.items():
        if level > 0:
            domains.append(name.replace('[]', ''))
            if level == 1:
                nested_item = ''
            else:
                nested_item = '.' + '.'.join(domains)
        else:
            nested_item = ''
            root_item = name.replace('[]', '')
        if isinstance(value, dict):
            level = level + 1
            if '[]' in name:
                field_name = name.replace('[]', '')
                root_item = root_item.replace('[]', '')
                nested_item = nested_item.replace('[]', '')
                if field_name in nested_item:
                    select_column = f"""ARRAY(SELECT(STRUCT({str(generate_code(
                        value,level,root_item=name, nested_item=nested_item,
                        domains=None))[2:-2]})) FROM UNNEST(JSON_EXTRACT_ARRAY(REPLACE(REPLACE({
                        root_item},"\'True\'","\'true\'"),"\'False\'","\'false\'"),'${
                        nested_item}')) AS {field_name}) AS {field_name}"""
                else:
                    select_column = f"""ARRAY(SELECT(STRUCT({str(generate_code(
                        value,level,root_item=name,nested_item=nested_item,
                        domains=None))[2:-2]})) FROM UNNEST(JSON_EXTRACT_ARRAY(REPLACE(REPLACE({
                        root_item},"\'True\'","\'true\'"),"\'False\'","\'false\'"),'${
                        nested_item}.{field_name}')) AS {
                        field_name}) AS {field_name}"""
            else:
                nested_item = f'{root_item}.{nested_item}'
                select_column = f"""STRUCT ({str(generate_code(
                    value,level,root_item=root_item,
                    nested_item=nested_item, domains=domains)
                    )[2:-2]}) AS {name}"""
            level = level - 1
            fields.append(select_column)
        elif level == 0:
            root_item = root_item.replace('[]', '')
            if '[]' in name:
                name = name.replace('[]', '')
                select_column = f"""JSON_EXTRACT_STRING_ARRAY({name}) AS {
                    name}"""
            elif value == 'STRING':
                select_column = f"""IF({name} ='',NULL,{name}) AS {name}"""
            else:
                select_column = f"""CAST({name} AS {value}) AS {name}"""
            fields.append(select_column)
        elif level == 1:
            root_item = root_item.replace('[]', '')
            if '[]' in name:
                name = name.replace('[]', '')
                select_column = f"""JSON_EXTRACT_STRING_ARRAY(REPLACE(REPLACE({
                    root_item},"\'True\'","\'true\'"),"\'False\'","\'false\'"),'${
                    nested_item}.{name}') AS {name}"""
            else:
                name = name.replace('[]', '')
                root_item = root_item.replace('[]', '')
                if value in [
                        'BOOL', 'INT64', 'DATE', 'FLOAT64', 'DATETIME',
                        'STRING', 'BYTES'
                ]:
                    select_column = f"""CAST(JSON_VALUE(REPLACE(REPLACE({
                        root_item},"\'True\'","\'true\'"),"\'False\'","\'false\'"),'${
                        nested_item}.{name}') AS {value}) AS {name}"""
                else:
                    select_column = f"""JSON_EXTRACT(REPLACE(REPLACE({
                        root_item},"\'True\'","\'true\'"),"\'False\'","\'false\'"),|${
                        nested_item}.{name}|) AS {name}"""
            fields.append(select_column)
        else:
            root_item = root_item.replace('[]', '')
            if '[]' in name:
                name = name.replace('[]', '')
                select_column = f"""JSON_EXTRACT_STRING_ARRAY(REPLACE(REPLACE({
                    root_item},"\'True\'","\'true\'"),"\'False\'","\'false\'"),'${
                        nested_item}') AS {name}"""
            else:
                name = name.replace('[]', '')
                if value in [
                        'BOOL', 'INT64', 'DATE', 'FLOAT64', 'DATETIME', 'BYTES'
                ]:
                    select_column = f"""CAST(JSON_VALUE(REPLACE(REPLACE({
                        root_item},"\'True\'","\'true\'"),"\'False\'","\'false\'"),'${
                            nested_item}') AS {value}) AS {name}"""
                else:
                    select_column = f"""JSON_VALUE(REPLACE(REPLACE({
                        root_item},"\'True\'","\'true\'"),"\'False\'","\'false\'"),|${
                            nested_item}|) AS {name}"""
            fields.append(select_column)

        if level < 2:
            domains.clear()
        if len(domains) > 0:
            domains.pop()
    return fields
