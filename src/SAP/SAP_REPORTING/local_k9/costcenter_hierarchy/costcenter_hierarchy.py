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
"""DAG to generate flattened cost center hierarchy tables."""
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

# This DAG creates following two tables
# and deletes hierarchy from a specific node(if needed).
# 1-flattened cost center table.
# 2-cost center and node mapping.
with DAG(dag_id="cost_center",
         default_args=default_args,
         schedule_interval="@monthly",
         start_date=datetime(2023, 11, 27),
         catchup=False,
         max_active_runs=1) as dag:

    start_task = EmptyOperator(task_id="start")

    # This task creates the flattened cost center table.
    costcenter_flattened = BigQueryInsertJobOperator(
        task_id="costcenter_flattened",
        gcp_conn_id="sap_reporting_bq",
        configuration={
            "query": {
                "query": "costcenter_hierarchy.sql",
                "useLegacySql": False
            },
            "labels": _BQ_LABELS,
            "location": _BQ_LOCATION
        })
    # This task deletes the hierarchy from a specific node.
    delete_costcenter_node = BigQueryInsertJobOperator(
        task_id="delete_costcenter_node",
        gcp_conn_id="sap_reporting_bq",
        configuration={
            "query": {
                "query": "costcenter_node_deletion.sql",
                "useLegacySql": False
            },
            "labels": _BQ_LABELS,
            "location": _BQ_LOCATION
        })
    # This task creates the cost center mapping table.
    costcenter_mapping = BigQueryInsertJobOperator(
        task_id="costcenter_mapping",
        gcp_conn_id="sap_reporting_bq",
        configuration={
            "query": {
                "query": "costcenter_node_mapping.sql",
                "useLegacySql": False
            },
            "labels": _BQ_LABELS,
            "location": _BQ_LOCATION
        })

    stop_task = EmptyOperator(task_id="stop")

    # pylint:disable=pointless-statement
    (start_task >> costcenter_flattened >> delete_costcenter_node >>
     costcenter_mapping >> stop_task)  # type: ignore
