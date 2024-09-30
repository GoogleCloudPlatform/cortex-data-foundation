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
"""Cloud Composer DAG to generate flattened fsv table.
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

# This DAG creates following two tables
# and deletes hierarchy from a specific node(if needed).
# 1-flattened fsv table.
# 2-glaccount and fsv node mapping.
with DAG(dag_id="financial_statement_version",
         default_args=default_args,
         schedule_interval="@monthly",
         start_date=datetime(2023, 8, 4),
         catchup=False,
         max_active_runs=1) as dag:

    start_task = EmptyOperator(task_id="start")

    # This task creates the flattened fsv table.
    fsv_flattened = BigQueryInsertJobOperator(
        task_id="fsv_flattened",
        gcp_conn_id="sap_reporting_bq",
        configuration={
            "query": {
                "query": "financial_statement_version.sql",
                "useLegacySql": False
            },
            "labels": _BQ_LABELS
        })

    ## This task deletes the hierarchy from a specific node.
    delete_fsv_node = BigQueryInsertJobOperator(
        task_id="delete_fsv_node",
        gcp_conn_id="sap_reporting_bq",
        configuration={
            "query": {
                "query": "fsv_delete_node.sql",
                "useLegacySql": False
            },
            "labels": _BQ_LABELS
        })

    ## This task creates the fsv & glaccounts mapping table.
    fsv_glaccounts_mapping = BigQueryInsertJobOperator(
        task_id="fsv_glaccounts_mapping",
        gcp_conn_id="sap_reporting_bq",
        configuration={
            "query": {
                "query": "fsv_glaccounts_mapping.sql",
                "useLegacySql": False
            },
            "labels": _BQ_LABELS
        })

    stop_task = EmptyOperator(task_id="stop")

    # pylint:disable=pointless-statement
    (start_task >> fsv_flattened >> delete_fsv_node >> fsv_glaccounts_mapping >>
     stop_task)  # type:ignore
