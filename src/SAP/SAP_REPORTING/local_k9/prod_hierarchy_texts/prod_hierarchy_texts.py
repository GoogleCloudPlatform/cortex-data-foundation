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
"""Airflow dag for Product Hierarchy Text"""
import ast

from airflow import DAG
from datetime import datetime, timedelta

from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.empty import EmptyOperator

# BigQuery Job Labels - converts generated string to dict
# If string is empty, assigns empty dict
_BQ_LABELS = ast.literal_eval("{{ runtime_labels_dict }}" or "{}")
_BQ_LOCATION = "{{ location }}"

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(dag_id="Product_Hierarchy_Text",
         default_args=default_args,
         schedule_interval="@yearly",
         start_date=datetime(2021, 1, 1),
         catchup=False,
         max_active_runs=1) as dag:
    start_task = EmptyOperator(task_id="start")

    get_prodhier_texts = BigQueryInsertJobOperator(
        task_id="get_prodhier_texts",
        gcp_conn_id="sap_reporting_bq",
        configuration={
            "query": {
                "query": "prod_hierarchy_texts.sql",
                "useLegacySql": False
            },
            "labels": _BQ_LABELS,
            "location": _BQ_LOCATION
        })

    stop_task = EmptyOperator(task_id="stop")

    # pylint:disable=pointless-statement
    start_task >> get_prodhier_texts >> stop_task  # type: ignore
