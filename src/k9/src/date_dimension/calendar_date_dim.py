# Copyright 2022 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     https://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Cloud Composer DAG to generate calendar_dim_date table.
Executes relevant nodes to create one tables.
"""
import ast

from datetime import datetime
from datetime import timedelta

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.empty import EmptyOperator

# BigQuery Job Labels - converts generated string to dict
# If string is empty, assigns empty dict
_BQ_LABELS = ast.literal_eval("{{ runtime_labels_dict }}" or "{}")
_BQ_LOCATION = "{{ location }}"

default_args = {
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG("calendar_date_dim",
         default_args=default_args,
         schedule_interval="@yearly",
         start_date=datetime(2022, 11, 2),
         catchup=False,
         max_active_runs=1) as dag:
    start_task = EmptyOperator(task_id="start")
    calendar_date_dim = BigQueryInsertJobOperator(
        task_id="calendar_date_dim",
        gcp_conn_id="sap_cdc_bq",
        configuration={
            "query": {
                "query": "calendar_date_dim.sql",
                "useLegacySql": False
            },
            "labels": _BQ_LABELS,
            "location": _BQ_LOCATION
        })
    stop_task = EmptyOperator(task_id="stop")

    # pylint:disable=pointless-statement
    (start_task >> calendar_date_dim >> stop_task)
