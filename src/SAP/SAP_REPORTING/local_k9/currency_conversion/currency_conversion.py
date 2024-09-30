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
import ast

from airflow import DAG
from datetime import datetime, timedelta

from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.empty import EmptyOperator

# BigQuery Job Labels - converts generated string to dict
# If string is empty, assigns empty dict
_BQ_LABELS = ast.literal_eval("{{ runtime_labels_dict }}" or "{}")

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# This DAG creates two table:
# 1-currency_conversion for storing the exchange rate and other columns.
# 2-currency_decimal to fix the decimal place of amounts
# for non-decimal-based currencies.
with DAG(dag_id="currency_conversion",
         default_args=default_args,
         schedule_interval="@daily",
         start_date=datetime(2022, 8, 11),
         catchup=False,
         max_active_runs=1) as dag:

    start_task = EmptyOperator(task_id="start")

    currency_conversion = BigQueryInsertJobOperator(
        task_id="currency_conversion",
        gcp_conn_id="sap_reporting_bq",
        configuration={
            "query": {
                "query": "currency_conversion.sql",
                "useLegacySql": False
            },
            "labels": _BQ_LABELS
        })

    # This task loads currency decimal table to fix the decimal
    # place of amounts for non-decimal-based currencies.
    currency_decimal = BigQueryInsertJobOperator(
        task_id="currency_decimal",
        gcp_conn_id="sap_reporting_bq",
        configuration={
            "query": {
                "query": "currency_decimal.sql",
                "useLegacySql": False
            },
            "labels": _BQ_LABELS
        })

    stop_task = EmptyOperator(task_id="stop")

    # pylint:disable=pointless-statement
    (start_task >> currency_conversion >> currency_decimal >> stop_task
    )  # type: ignore
