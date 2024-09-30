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
"""Cloud Composer DAG to generate currency conversion table.

Executes relevant nodes to create two tables.
"""
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.version import version as AIRFLOW_VERSION

default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
## This DAG creates two table:
## 1-currency_conversion for storing the exchange rate and other columns.
## 2-currency_decimal to fix the decimal place of amounts
## for non-decimal-based currencies.
with DAG(
        'currency_conversion',
        default_args=default_args,
        schedule_interval='@daily',
        start_date=datetime(2022, 8, 11),
        catchup=False,
        max_active_runs=1
) as dag:
    start_task = DummyOperator(task_id='start')

    if AIRFLOW_VERSION.startswith('1.'):
  ## This task creates currency conversion table and loads data into it daily.
        currency_conversion = BigQueryOperator(
            task_id='currency_conversion',
            sql='currency_conversion.sql',
            create_disposition='CREATE_IF_NEEDED',
            write_disposition='WRITE_TRUNCATE',
            bigquery_conn_id='sap_reporting_bq',
            use_legacy_sql=False)
    ## This task loads currency decimal table to fix the decimal
    ## place of amounts for non-decimal-based currencies.
        currency_decimal=BigQueryOperator(
            task_id='currency_decimal',
            sql='currency_decimal.sql',
            create_disposition='CREATE_IF_NEEDED',
            write_disposition='WRITE_TRUNCATE',
            bigquery_conn_id='sap_reporting_bq',
            use_legacy_sql=False)

        stop_task = DummyOperator(task_id='stop')
    else:
        currency_conversion = BigQueryOperator(
            task_id='currency_conversion',
            sql='currency_conversion.sql',
            create_disposition='CREATE_IF_NEEDED',
            write_disposition='WRITE_TRUNCATE',
            gcp_conn_id='sap_reporting_bq',
            use_legacy_sql=False)
    ## This task loads currency decimal table to fix the decimal
    ## place of amounts for non-decimal-based currencies.
        currency_decimal=BigQueryOperator(
            task_id='currency_decimal',
            sql='currency_decimal.sql',
            create_disposition='CREATE_IF_NEEDED',
            write_disposition='WRITE_TRUNCATE',
            gcp_conn_id='sap_reporting_bq',
            use_legacy_sql=False)

        stop_task = DummyOperator(task_id='stop')
  # pylint:disable=pointless-statement
    (start_task >>  currency_conversion >> currency_decimal >> stop_task)
