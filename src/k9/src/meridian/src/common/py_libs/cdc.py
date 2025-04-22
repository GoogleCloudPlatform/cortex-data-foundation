# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Library for CDC related functions."""

import logging

from google.cloud.exceptions import NotFound
from google.cloud import bigquery
from common.py_libs.bq_materializer import add_partition_to_table_def
from common.py_libs.bq_materializer import add_cluster_to_table_def

# Columns to be ignored for CDC tables
_CDC_EXCLUDED_COLUMN_LIST = ["_PARTITIONTIME", "OperationalFlag", "IsDeleted"]

logger = logging.getLogger(__name__)


def create_cdc_table(bq_client: bigquery.Client, table_setting: dict,
                     cdc_project: str, cdc_dataset: str,
                     schema: list[tuple[str, str]]):
    """Creates CDC table using the provided schema and table settings.

    Args:
        bq_client: BQ Client.
        table_setting: Table config as defined in the settings file.
        cdc_project: BQ CDC project.
        cdc_dataset: BQ CDC dataset name.
        schema: CDC table schema as a list of tuples (column name, column type)
    """

    base_table: str = table_setting["base_table"].lower()
    cdc_table_name = cdc_project + "." + cdc_dataset + "." + base_table

    try:
        _ = bq_client.get_table(cdc_table_name)
        logger.warning("Table '%s' already exists. Not creating it again.",
                       cdc_table_name)
    except NotFound:
        # Let's create CDC table.
        logger.info("Table '%s' does not exists. Creating it.", cdc_table_name)

        target_schema = [
            bigquery.SchemaField(name=f[0], field_type=f[1]) for f in schema
        ]

        cdc_table = bigquery.Table(cdc_table_name, schema=target_schema)

        # Add clustering and partitioning properties if specified.
        partition_details = table_setting.get("partition_details")
        if partition_details:
            cdc_table = add_partition_to_table_def(cdc_table, partition_details)

        cluster_details = table_setting.get("cluster_details")
        if cluster_details:
            cdc_table = add_cluster_to_table_def(cdc_table, cluster_details)

        bq_client.create_table(cdc_table)

        logger.info("Created table '%s'.", cdc_table_name)


def create_cdc_table_from_raw_table(bq_client, table_setting, raw_project,
                                    raw_dataset, cdc_project, cdc_dataset):
    """Creates CDC table based on source RAW table schema.

    Retrieves schema details from source table in RAW dataset and creates a
    table in CDC dataset based on that schema if it does not exist.

    Args:
        bq_client: BQ Client.
        table_setting: Table config as defined in the settings file.
        raw_project: BQ Raw project.
        raw_dataset: BQ Raw dataset name.
        cdc_project: BQ CDC project.
        cdc_dataset: BQ CDC dataset name.
    """

    base_table = table_setting["base_table"].lower()
    raw_table_name = raw_project + "." + raw_dataset + "." + base_table
    cdc_table_name = cdc_project + "." + cdc_dataset + "." + base_table

    try:
        _ = bq_client.get_table(cdc_table_name)
        logger.warning("Table '%s' already exists. Not creating it again.",
                       cdc_table_name)
    except NotFound:
        # Let's create CDC table.
        logger.info("Table '%s' does not exists. Creating it.", cdc_table_name)
        try:
            raw_table_schema = bq_client.get_table(raw_table_name).schema
        except NotFound:
            e_msg = (f"RAW Table '{raw_table_name}' does not exist.")
            raise Exception(e_msg) from None

        target_schema = [
            field for field in raw_table_schema
            if field.name not in _CDC_EXCLUDED_COLUMN_LIST
        ]

        cdc_table = bigquery.Table(cdc_table_name, schema=target_schema)

        # Add clustering and partitioning properties if specified.
        partition_details = table_setting.get("partition_details")
        if partition_details:
            cdc_table = add_partition_to_table_def(cdc_table, partition_details)

        cluster_details = table_setting.get("cluster_details")
        if cluster_details:
            cdc_table = add_cluster_to_table_def(cdc_table, cluster_details)

        bq_client.create_table(cdc_table)

        logger.info("Created table '%s'.", cdc_table_name)
