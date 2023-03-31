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
"""Library for materializer related functions."""

import logging
from google.cloud import bigquery

logger = logging.getLogger(__name__)

# File types supported by Reporting materializer.
_REPORTING_FILE_TYPES = ["table", "view", "script"]

# Frequency to refresh CDC table. These values corresponds to
# Apache Airflow / Cloud Composer DAG schedule interval values.
_LOAD_FREQUENCIES = [
    "runtime", "None", "@once", "@hourly", "@daily", "@weekly", "@monthly",
    "@yearly"
]

# Supported partition types.
_PARTITION_TYPES = ["time", "integer_range"]

# Supported grains for time based partitioning.
_TIME_PARTITION_GRAIN_LIST = ["hour", "day", "month", "year"]

# Dict to convert string values to correct partitioning type.
_TIME_PARTITION_GRAIN_DICT = {
    "hour": bigquery.TimePartitioningType.HOUR,
    "day": bigquery.TimePartitioningType.DAY,
    "month": bigquery.TimePartitioningType.MONTH,
    "year": bigquery.TimePartitioningType.YEAR
}

# Column data types supported for time based partitioning.
_TIME_PARTITION_DATA_TYPES = ["DATE", "TIMESTAMP", "DATETIME"]


def validate_cluster_columns(cluster_details, target_schema):
    """Checks schema to make sure columns are appropriate for clustering."""
    cluster_columns = cluster_details["columns"]
    for column in cluster_columns:
        cluster_column_details = [
            field for field in target_schema if field.name == column
        ]
        if not cluster_column_details:
            raise ValueError(
                f"Column '{column}' specified for clustering does not exist "
                "in the table.")


def validate_partition_columns(partition_details, target_schema):
    """Checks schema to make sure columns are appropriate for partitioning."""

    column = partition_details["column"]

    partition_column_details = [
        field for field in target_schema if field.name == column
    ]
    if not partition_column_details:
        raise ValueError(
            f"Column '{column}' specified for partitioning does not exist in "
            "the table.")

    # Since there will be only value in the list (a column exists only once
    # in a table), let's just use the first value from the list.
    partition_column_type = partition_column_details[0].field_type

    partition_type = partition_details["partition_type"]

    if (partition_type == "time" and
            partition_column_type not in _TIME_PARTITION_DATA_TYPES):
        raise ValueError(
            "For 'partition_type' = 'time', partitioning column has to be "
            "one of the following data types:"
            f"{_TIME_PARTITION_DATA_TYPES}.\n"
            f"But column '{column}' is of '{partition_column_type}' type.")

    if (partition_type == "integer_range" and
            partition_column_type != "INTEGER"):
        raise ValueError(
            "Error: For 'partition_type' = 'integer_range', partitioning "
            f"column has to be of INTEGER data type. But column '{column}' is "
            f"of '{partition_column_type}'.")


def add_cluster_to_table_def(table_def, cluster_details):
    validate_cluster_columns(cluster_details, table_def.schema)
    # Add clustering clause to BQ table definition.
    table_def.clustering_fields = cluster_details["columns"]
    return table_def


def add_partition_to_table_def(table_def, partition_details):

    validate_partition_columns(partition_details, table_def.schema)

    # Add partitioning clause to BQ table definition.
    if partition_details["partition_type"] == "time":
        time_partition_grain = partition_details["time_grain"]
        table_def.time_partitioning = bigquery.TimePartitioning(
            field=partition_details["column"],
            type_=_TIME_PARTITION_GRAIN_DICT[time_partition_grain])
    else:
        integer_range_bucket = partition_details["integer_range_bucket"]
        bucket_start = integer_range_bucket["start"]
        bucket_end = integer_range_bucket["end"]
        bucket_interval = integer_range_bucket["interval"]
        table_def.range_partitioning = bigquery.RangePartitioning(
            field=partition_details["column"],
            range_=bigquery.PartitionRange(start=bucket_start,
                                           end=bucket_end,
                                           interval=bucket_interval))
    return table_def


def validate_cluster_details(cluster_details):

    cluster_columns = cluster_details.get("columns")

    if not cluster_columns or len(cluster_columns) == 0:
        raise ValueError(
            "'columns' property missing from 'cluster_details' property.")

    if not isinstance(cluster_columns, list):
        raise ValueError(
            "'columns' property in 'cluster_details' has to be a List.")

    if len(cluster_columns) > 4:
        raise ValueError(
            "More than 4 columns specified in 'cluster_details' property. "
            "BigQuery supports maximum of 4 columns for table cluster.")


def validate_partition_details(partition_details):

    partition_column = partition_details.get("column")
    if not partition_column:
        raise ValueError(
            "Partition 'column' property missing from 'partition_details' "
            "property.")

    partition_type = partition_details.get("partition_type")
    if not partition_type:
        raise ValueError(
            "'partition_type' property missing from 'partition_details' "
            "property.")

    if partition_type not in _PARTITION_TYPES:
        raise ValueError("'partition_type' has to be one of the following: "
                         f"{_PARTITION_TYPES}.\n"
                         f"Specified 'partition_type' is '{partition_type}'.")

    if partition_type == "time":
        time_partition_grain = partition_details.get("time_grain")
        if not time_partition_grain:
            raise ValueError(
                "'time_grain' property missing for 'time' based partition.")

        if time_partition_grain not in _TIME_PARTITION_GRAIN_LIST:
            raise ValueError(
                "'time_grain' property has to be one of the following:"
                f"{_TIME_PARTITION_GRAIN_LIST}.\n"
                f"Specified 'time_grain' is '{time_partition_grain}'.")

    if partition_type == "integer_range":
        integer_range_bucket = partition_details.get("integer_range_bucket")
        if not integer_range_bucket:
            raise ValueError(
                "'integer_range_bucket' property missing for 'integer_range' "
                "based partition.")

        bucket_start = integer_range_bucket.get("start")
        bucket_end = integer_range_bucket.get("end")
        bucket_interval = integer_range_bucket.get("interval")

        if (bucket_start is None or bucket_end is None or
                bucket_interval is None):
            raise ValueError(
                "Error: 'start', 'end' or 'interval' property missing for the "
                "'integer_range_bucket' property.")


def validate_materializer_setting(table_setting):
    """Makes sure the materializer setting for a table is valid."""

    logger.debug("table_settings :\n %s", table_setting)

    load_frequency = table_setting.get("load_frequency")
    if not load_frequency:
        raise ValueError("Missing 'load_frequency' property.")

    if load_frequency not in _LOAD_FREQUENCIES:
        raise ValueError(f"'load_frequency' has to be one of the following:"
                         f"{_LOAD_FREQUENCIES}.\n"
                         f"Specified 'load_frequency' is '{load_frequency}'.")

    partition_details = table_setting.get("partition_details")
    cluster_details = table_setting.get("cluster_details")

    # Validate partition details.
    if partition_details:
        validate_partition_details(partition_details)

    if cluster_details:
        validate_cluster_details(cluster_details)


def validate_materializer_settings(materializer_settings):
    """Makes sure all materializer settings are valid."""

    logger.info("Validating materializer settings ...")

    logger.debug("materializer_settings = %s", materializer_settings)

    tables_processed = set()

    for table_setting in materializer_settings:
        base_table = table_setting.get("base_table")
        if not base_table:
            raise ValueError("'base_table' property missing from an entry.")

        logger.debug("  Checking setting for table '%s' ....", base_table)
        validate_materializer_setting(table_setting)

        # Check for duplicate entries.
        if base_table in tables_processed:
            raise ValueError(f"Table '{base_table}' is present multiple times.")
        else:
            tables_processed.add(base_table)

    logger.info("Materializer settings look good...")


def validate_reporting_materializer_setting(file_setting):
    """Validates reporting materializer setting for a table."""

    logger.debug("file_settings :\n %s", file_setting)

    file_type = file_setting.get("type")
    if not file_type:
        raise ValueError("Missing 'type' property.")
    if file_type not in _REPORTING_FILE_TYPES:
        raise ValueError(f"'type' has to be one of the following:"
                         f"{_REPORTING_FILE_TYPES}.\n"
                         f"Specified 'type' is '{file_type}'.")

    if file_type == "table":
        table_setting = file_setting.get("table_setting")
        validate_materializer_setting(table_setting)
        # Above validation allows for "runtime" frequency, which is not allowed
        # for reporting materializer. Let's do one additional check.
        load_frequency = table_setting.get("load_frequency")
        if load_frequency == "runtime":
            raise ValueError("'load_frequency' can not be 'runtime'.")


def validate_reporting_materializer_settings(materializer_settings):
    """Validates all reporting materializer settings."""

    logger.info("Validating reporting materializer settings ...")

    logger.debug("reporting materializer_settings = %s", materializer_settings)

    files_processed = set()

    for file_setting in materializer_settings:
        sql_file = file_setting.get("sql_file")
        if not sql_file:
            raise ValueError("'sql_file' property missing from an entry.")

        logger.debug("  Checking setting for file '%s' ....", sql_file)
        validate_reporting_materializer_setting(file_setting)

        # Check for duplicate entries.
        if sql_file in files_processed:
            raise ValueError(f"File '{sql_file}' is present multiple times.")
        else:
            files_processed.add(sql_file)

    logger.info("Reporting materializer settings look good...")
