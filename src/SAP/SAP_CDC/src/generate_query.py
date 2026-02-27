# Copyright 2024 Google LLC
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
"""Useful functions to carry out neecssary operations."""

#TODO: Remove all pylint disabled flags.
#pylint:disable=missing-type-doc

import datetime
from pathlib import Path
import sys

from string import Template

from google.cloud.exceptions import NotFound
from google.cloud import bigquery
from google.cloud import storage

# Make sure common modules are in Python path
sys.path.append(str(Path(__file__).parent.parent))

# pylint:disable=wrong-import-position
from common.py_libs import constants
from common.py_libs import cortex_bq_client
from common.py_libs import cortex_exceptions as cortex_exc

_SQL_DAG_PYTHON_TEMPLATE = 'src/template_dag/dag_sql.py'
_SQL_DAG_SQL_TEMPLATE = 'src/template_sql/cdc_sql_template.sql'
_VIEW_SQL_TEMPLATE = 'src/template_sql/runtime_query_view.sql'

_GENERATED_DAG_DIR = 'generated_dag'
_GENERATED_SQL_DIR = 'generated_sql'

# Columns to be ignored for CDC tables
_CDC_EXCLUDED_COLUMN_LIST = ['_PARTITIONTIME', 'operation_flag', 'is_deleted']

# Supported partition types.
_PARTITION_TYPES = ['time', 'integer_range']

# Column data types supported for time based partitioning.
_TIME_PARTITION_DATA_TYPES = ['DATE', 'TIMESTAMP', 'DATETIME']

# Supported grains for time based partitioning.
_TIME_PARTITION_GRAIN_LIST = ['hour', 'day', 'month', 'year']

# Dict to convert string values to correct paritioning type.
_TIME_PARTITION_GRAIN_DICT = {
    'hour': bigquery.TimePartitioningType.HOUR,
    'day': bigquery.TimePartitioningType.DAY,
    'month': bigquery.TimePartitioningType.MONTH,
    'year': bigquery.TimePartitioningType.YEAR
}

# Frequency to refresh CDC table.
# "RUNTIME" translates into a CDC view instead of a table.
# Rest of the values corresponds to Apache Airflow / Cloud Composer
# DAG schedule interval values.
_LOAD_FREQUENCIES = [
    'RUNTIME', 'None', '@once', '@hourly', '@daily', '@weekly', '@monthly',
    '@yearly'
]

# Use Cortex wrapper client
client = cortex_bq_client.CortexBQClient()
storage_client = storage.Client()


def generate_dag_py_file(template, file_name, **dag_subs):
    """Generates DAG definition python file from template."""
    with open(template, mode='r', encoding='utf-8') as dag_template_file:
        dag_template = Template(dag_template_file.read())
    generated_dag_code = dag_template.substitute(**dag_subs)

    dag_file = _GENERATED_DAG_DIR + '/' + file_name
    with open(dag_file, mode='w+', encoding='utf-8') as generated_dag_file:
        generated_dag_file.write(generated_dag_code)
        generated_dag_file.close()
        print(f'Created DAG python file {dag_file}')


def generate_runtime_view(raw_table_name, cdc_table_name):
    """Creates runtime CDC view for RAW table."""

    primary_keys = get_primary_keys(raw_table_name)
    if not primary_keys:
        e_msg = f'Primary keys for {raw_table_name} not found in table DD03L'
        raise cortex_exc.KeyCError(e_msg) from None

    primary_keys_not_null_list = []
    for key in primary_keys:
        primary_keys_not_null_list.append((f'`{key}` IS NOT NULL'))
    primary_keys_not_null_clause = ' AND '.join(primary_keys_not_null_list)

    # Generate view sql by using template.
    with open(_VIEW_SQL_TEMPLATE, mode='r',
              encoding='utf-8') as sql_template_file:
        sql_template = Template(sql_template_file.read())

    generated_sql = sql_template.substitute(
        base_table=raw_table_name,
        primary_keys=', '.join(primary_keys),
        primary_keys_not_null_clause=primary_keys_not_null_clause)

    view = bigquery.Table(cdc_table_name)
    view.view_query = generated_sql
    client.create_table(view, exists_ok=True)

    print(f'Created view {cdc_table_name}')


def generate_cdc_dag_files(raw_table_name, cdc_table_name, load_frequency,
                           gen_test, allow_telemetry, bq_location):
    """Generates file containing DAG code to refresh CDC table from RAW table.

    Args:
        raw_table_name: Full table name of raw table (dataset.table_name).
        cdc_table_name: Full table name of cdc table (dataset.table_name).
        load_frequency: DAG run frequency.
        gen_test: If test data is needed.
        allow_telemetry: If telemetry allowed.
        bq_location: BigQuery location for DAG execution.


    Raises:
        cortex_exc.KeyCEError: If primary keys not found in table DD03L.
        Exception: If error in executing DAG to populate CDC tables.

    """

    dag_file_name_part = 'cdc_' + raw_table_name.replace('.', '_')
    dag_py_file_name = dag_file_name_part + '.py'
    dag_sql_file_name = dag_file_name_part + '.sql'

    today = datetime.datetime.now()
    substitutes = {
        'base_table': raw_table_name,
        'year': today.year,
        'month': today.month,
        'day': today.day,
        'query_file': dag_sql_file_name,
        'load_frequency': load_frequency,
        'runtime_labels_dict': '',  # A place holder for label key
        'bq_location': bq_location
    }
    # Add bq_labels to substitutes dict if Telemetry allowed
    # Converts dict to string for substitution purposes
    if allow_telemetry:
        substitutes['runtime_labels_dict'] = str(constants.CORTEX_JOB_LABEL)

    # Create python DAG file.
    generate_dag_py_file(_SQL_DAG_PYTHON_TEMPLATE, dag_py_file_name,
                         **substitutes)

    # Create query for SQL script that will be used in the DAG.
    fields = []
    update_fields = []

    raw_table_schema = client.get_table(raw_table_name).schema
    for field in raw_table_schema:
        if field.name not in _CDC_EXCLUDED_COLUMN_LIST:
            fields.append(f'`{field.name}`')
            update_fields.append((f'T.`{field.name}` = S.`{field.name}`'))

    if not fields:
        print(f'Schema could not be retrieved for {raw_table_name}')

    primary_keys = get_primary_keys(raw_table_name)
    if not primary_keys:
        e_msg = f'Primary keys for {raw_table_name} not found in table DD03L'
        raise cortex_exc.KeyCError(e_msg) from None

    primary_key_join_list = []
    primary_key_not_null_list = []
    for key in primary_keys:
        primary_key_join_list.append(f'T.`{key}` = S.`{key}`')
        primary_key_not_null_list.append(f'`{key}` IS NOT NULL')

    primary_keys_join_clause = ' AND '.join(primary_key_join_list)
    primary_keys_not_null_clause = ' AND '.join(primary_key_not_null_list)

    with open(_SQL_DAG_SQL_TEMPLATE, mode='r',
              encoding='utf-8') as sql_template_file:
        sql_template = Template(sql_template_file.read())

    generated_sql = sql_template.substitute(
        base_table=raw_table_name,
        target_table=cdc_table_name,
        primary_keys_join_clause=primary_keys_join_clause,
        primary_keys_not_null_clause=primary_keys_not_null_clause,
        fields=', '.join(fields),
        update_fields=', '.join(update_fields),
        primary_keys=', '.join(primary_keys))

    # Create sql file containing the query
    cdc_sql_file = _GENERATED_SQL_DIR + '/' + dag_sql_file_name
    with open(cdc_sql_file, mode='w+', encoding='utf-8') as generated_sql_file:
        generated_sql_file.write(generated_sql)
        generated_sql_file.close()
        print(f'Created DAG sql file {cdc_sql_file}')

    # If test data is needed, we want to populate CDC tables as well
    # from data in the RAW tables.
    # Good thing is - we already have the sql query available to do that.
    if gen_test.upper() == 'TRUE':
        query_job = client.query(generated_sql)
        # Let's wait for query to complete.
        try:
            _ = query_job.result()
        except Exception as e:  #pylint:disable=broad-except
            print('Error in running DAG sql')
            print(f'DAG sql: {generated_sql}')
            print(f'Error: {str(e)}')
            raise e
        print(f'{cdc_table_name} table is populated with data '
              f'from {raw_table_name} table')


def validate_partition_details(partition_details, load_frequency):
    if load_frequency == 'RUNTIME':
        e_msg = ('`partition_details` property should NOT be specified '
                 'for runtime views, but it IS specified.')
        return e_msg

    partition_column = partition_details.get('column')
    if not partition_column:
        e_msg = ('Partition `column` property missing from '
                 '`partition_details` property.')
        return e_msg

    partition_type = partition_details.get('partition_type')
    if not partition_type:
        e_msg = ('`partition_type` property missing from '
                 '`partition_details` property.')
        return e_msg

    if partition_type not in _PARTITION_TYPES:
        e_msg = ('`partition_type` has to be one of the following:'
                 f'{_PARTITION_TYPES}.\n'
                 f'Specified `partition_type` is "{partition_type}".')
        return e_msg

    if partition_type == 'time':
        time_partition_grain = partition_details.get('time_grain')
        if not time_partition_grain:
            e_msg = ('`time_grain` property missing for '
                     '`time` based partition.')
            return e_msg

        if time_partition_grain not in _TIME_PARTITION_GRAIN_LIST:
            e_msg = ('`time_grain` property has to be one of the following:'
                     f'{_TIME_PARTITION_GRAIN_LIST}.\n'
                     f'Specified `time_grain` is "{time_partition_grain}".')
            return e_msg

    if partition_type == 'integer_range':
        integer_range_bucket = partition_details.get('integer_range_bucket')
        if not integer_range_bucket:
            e_msg = ('`integer_range_bucket` property missing for '
                     '`integer_range` based partition.')
            return e_msg

        bucket_start = integer_range_bucket.get('start')
        bucket_end = integer_range_bucket.get('end')
        bucket_interval = integer_range_bucket.get('interval')

        if (bucket_start is None or bucket_end is None
                or bucket_interval is None):
            e_msg = (
                'Error: `start`, `end` or `interval` property missing for '
                'the `integer_range_bucket` property.')
            return e_msg

    return None


def validate_cluster_details(cluster_details, load_frequency):

    if load_frequency == 'RUNTIME':
        e_msg = ('`cluster_details` property should NOT be specified '
                 'for runtime views, but it IS specified.')
        return e_msg

    cluster_columns = cluster_details.get('columns')

    if not cluster_columns or len(cluster_columns) == 0:
        e_msg = '`columns` property missing from `cluster_details` property.'
        return e_msg

    if not isinstance(cluster_columns, list):
        e_msg = '`columns` property in `cluster_details` has to be a List.'
        return e_msg

    if len(cluster_columns) > 4:
        e_msg = (
            'More than 4 columns specified in `cluster_details` property. '
            'BigQuery supports maximum of 4 columns for table cluster.')
        return e_msg

    return None


def validate_table_config(table_config):
    """Makes sure the config for a table in settings file is valid."""
    load_frequency = table_config.get('load_frequency')
    if not load_frequency:
        e_msg = 'Missing `load_frequency` property.'
        return e_msg

    # TODO: Add cron validation
    # if load_frequency not in _LOAD_FREQUENCIES:
    #     e_msg = ('`load_frequency` has to be one of the following:'
    #              f'{_LOAD_FREQUENCIES}.\n'
    #              f'Specified `load_frequency` is "{load_frequency}".')
    #     return e_msg

    partition_details = table_config.get('partition_details')
    cluster_details = table_config.get('cluster_details')

    # Validate partition details.
    if partition_details:
        e_msg = validate_partition_details(partition_details, load_frequency)
        if e_msg:
            return e_msg

    if cluster_details:
        e_msg = validate_cluster_details(cluster_details, load_frequency)
        if e_msg:
            return e_msg

    return None


def validate_table_configs(table_configs):
    """Makes sure all the configs provided in settings file is valid."""
    tables_processed = set()
    for config in table_configs:
        table_name = config.get('base_table')
        if not table_name:
            e_msg = '`base_table` property missing from an entry.'
            return e_msg
        error_message = validate_table_config(config)

        print('.... Checking configs for table "%s" ....', table_name)

        if error_message:
            e_msg = (f'Invalid settings for table "{table_name}".\n'
                     f'{error_message}')
            return e_msg

        # Check for duplicate entries.
        if table_name in tables_processed:
            e_msg = f'Table "{table_name}" is present multiple times.'
            return e_msg
        else:
            tables_processed.add(table_name)

        print(f'.... Check for configs for table "{table_name}" is '
              f'successful ....')


def validate_cluster_columns(cluster_details, target_schema):
    """Checks schema to make sure columns are appropriate for clustering."""
    cluster_columns = cluster_details['columns']
    for column in cluster_columns:
        cluster_column_details = [
            field for field in target_schema if field.name == column
        ]
        if not cluster_column_details:
            e_msg = (f'Column "{column}" specified for clustering does '
                     'not exist in the table.')
            raise cortex_exc.CriticalError(e_msg) from None


def validate_partition_columns(partition_details, target_schema):
    """Checks schema to make sure columns are appropriate for partitioning."""

    column = partition_details['column']

    partition_column_details = [
        field for field in target_schema if field.name == column
    ]
    if not partition_column_details:
        e_msg = (f'Column "{column}" specified for partitioning does not '
                 'exist in the table.')
        raise cortex_exc.CriticalError(e_msg) from None

    # Since there will be only value in the list (a column exists only once
    # in a table), let's just use the first value from the list.
    partition_column_type = partition_column_details[0].field_type

    partition_type = partition_details['partition_type']

    if (partition_type == 'time'
            and partition_column_type not in _TIME_PARTITION_DATA_TYPES):
        e_msg = (
            'For `partition_type` = "time", partitioning column has to be '
            'one of the following data types:'
            f'{_TIME_PARTITION_DATA_TYPES}.\n'
            f'But column "{column}" is of "{partition_column_type}" type.')
        raise cortex_exc.TypeCError(e_msg) from None

    if (partition_type == 'integer_range'
            and partition_column_type != 'INTEGER'):
        e_msg = ('Error: For `partition_type` = "integer_range", '
                 'partitioning column has to be of INTEGER data type.\n'
                 f'But column "{column}" is of {partition_column_type}.')
        raise cortex_exc.TypeCError(e_msg) from None


def create_cdc_table(raw_table_name, cdc_table_name, partition_details,
                     cluster_details):
    """Creates CDC table based on source RAW table schema.

    Retrieves schema details from source table in RAW dataset and creates a
    table in CDC dataset based on that schema if it does not exist.

    Args:
        raw_table_name: Full table name of raw table (dataset.table_name).
        cdc_table_name: Full table name of cdc table (dataset.table_name).
        partition_details: Partition details
        cluster_details: Cluster details

    Raises:
        NotFound: Bigquery table not found.

    """

    try:
        client.get_table(cdc_table_name)
        print(f'Table {cdc_table_name} already exists. Not creating it again.')
    except NotFound:
        # Let's create CDC table.
        raw_table_schema = client.get_table(raw_table_name).schema

        target_schema = [
            field for field in raw_table_schema
            if field.name not in _CDC_EXCLUDED_COLUMN_LIST
        ]

        cdc_table = bigquery.Table(cdc_table_name, schema=target_schema)

        # Add clustering and partitioning properties if specified.
        if partition_details:
            validate_partition_columns(partition_details, target_schema)
            # Add relevant partitioning clause
            if partition_details['partition_type'] == 'time':
                time_partition_grain = partition_details['time_grain']
                cdc_table.time_partitioning = bigquery.TimePartitioning(
                    field=partition_details['column'],
                    type_=_TIME_PARTITION_GRAIN_DICT[time_partition_grain])
            else:
                integer_range_bucket = partition_details[
                    'integer_range_bucket']
                bucket_start = integer_range_bucket['start']
                bucket_end = integer_range_bucket['end']
                bucket_interval = integer_range_bucket['interval']
                cdc_table.range_partitioning = bigquery.RangePartitioning(
                    field=partition_details['column'],
                    range_=bigquery.PartitionRange(start=bucket_start,
                                                   end=bucket_end,
                                                   interval=bucket_interval))

        if cluster_details:
            validate_cluster_columns(cluster_details, target_schema)
            cdc_table.clustering_fields = cluster_details['columns']

        client.create_table(cdc_table)

        print(f'Created table {cdc_table_name}.')


def get_primary_keys(full_table_name):
    """Retrieves primary key columns for raw table from metadata table.

    Args:
        full_table_name: Full table name in project.dataset.table_name format.
    """

    _, dataset, table_name = full_table_name.split('.')

    # Custom tables and fields in SAP have the prefix "/NAMESPACE/",
    # which are renamed to "NAMESPACE_" (or other character, per SLT settings)
    # when replicated to BigQuery, but retain their original naming in DD03L
    # table. So we need to mirror the replacement logic in the query to
    # get records for such tables.
    #
    # For example, "/OPT/Z_TABLE" is replicated to "OPT_Z_TABLE" in BigQuery.

    ## CORTEX-CUSTOMER: Replace per BQ SLT settings.
    replace_char = '_'

    # Remove starting "/" and then replace the second "/" with replace_char.
    sap_naming_replace_logic = (
        'REPLACE('
        '  IF(SUBSTR({FIELD}, 1, 1) = "/", SUBSTR({FIELD}, 2), {FIELD}),'
        '  "/",'
        f'  "{replace_char}"'
        ')')
    bq_field_name = sap_naming_replace_logic.format(FIELD='fieldname')
    bq_table_name = sap_naming_replace_logic.format(FIELD='tabname')
    query = (f'SELECT {bq_field_name} as fieldname '
             f'FROM `{dataset}.dd03l` '
             f'WHERE KEYFLAG = "X" AND fieldname != ".INCLUDE" '
             f'AND {bq_table_name} = "{table_name.upper()}"')
    query_job = client.query(query)

    fields = []
    for row in query_job:
        fields.append(row['fieldname'])
    return fields
