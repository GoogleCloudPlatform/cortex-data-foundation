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
"""Cloud Composer DAG to generate financial_statement table.
Executes relevant nodes to create the table.
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

# This DAG does the periodical load
# for financial_statement table.
with DAG(dag_id="financial_statement_periodical_load",
         default_args=default_args,
         schedule_interval="@monthly",
         start_date=datetime(2023, 8, 30),
         catchup=False,
         max_active_runs=1) as dag:

    start_task = EmptyOperator(task_id="start")

    # This task inserts the initial load in financial_statement table.
    financial_statement_periodical_load = BigQueryInsertJobOperator(
        task_id="financial_statement_periodical_load",
        gcp_conn_id="sap_reporting_bq",
        configuration={
            "query": {
                "query": "financial_statement_periodical_load.sql",
                "useLegacySql": False
            },
            "labels": _BQ_LABELS
        })

    stop_task = EmptyOperator(task_id="stop")

    # pylint:disable=pointless-statement
    (start_task >> financial_statement_periodical_load >> stop_task
    )  # type:ignore
