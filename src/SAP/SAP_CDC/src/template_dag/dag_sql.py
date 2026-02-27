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

# pylint: disable-all

from __future__ import print_function

import ast
from datetime import timedelta, datetime
import airflow
from airflow import __version__ as airflow_version
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.empty import EmptyOperator
from packaging.version import Version

# BigQuery Job Labels - converts generated string to dict
# If string is empty, assigns empty dict
_BQ_LABELS = ast.literal_eval("${runtime_labels_dict}" or "{}")

default_dag_args = {
    "depends_on_past": False,
    "start_date": datetime(int("${year}"), int("${month}"), int("${day}")),
    "catchup": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=30),
}

if Version(airflow_version) >= Version("2.4.0"):
   schedule_kwarg = {"schedule": "${load_frequency}"}
else:
   schedule_kwarg = {"schedule_interval": "${load_frequency}"}

with airflow.DAG(dag_id="CDC_BigQuery_${base_table}",
                 template_searchpath=["/home/airflow/gcs/data/bq_data_replication/"],
                 default_args=default_dag_args,
                 catchup=False,
                 max_active_runs=1,
                 **schedule_kwarg) as dag:
    start_task = EmptyOperator(task_id="start")
    copy_records = BigQueryInsertJobOperator(
        task_id="merge_query_records",
        gcp_conn_id="sap_cdc_bq",
        configuration={
            "query": {
                "query": "${query_file}",
                "useLegacySql": False
            },
            "labels": _BQ_LABELS,
            "location": "${bq_location}"
        },
       location="${bq_location}"
    )
    stop_task = EmptyOperator(task_id="stop")
    start_task >> copy_records >> stop_task # type: ignore