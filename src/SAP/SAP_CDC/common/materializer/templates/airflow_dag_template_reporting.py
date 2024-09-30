# Copyright 2024 Google LLC

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Disable pylance warnings
# type: ignore
# Disable all pylint warning
# pylint: skip-file

from __future__ import print_function

import ast
from datetime import datetime
from datetime import timedelta

import airflow
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import \
    BigQueryInsertJobOperator

# BigQuery Job Labels - converts generated string to dict
# If string is empty, assigns empty dict
_BQ_LABELS = ast.literal_eval("${runtime_labels_dict}" or "{}")

default_dag_args = {
   "depends_on_past": False,
   "start_date": datetime(${year}, ${month}, ${day}),
   "catchup": False,
   "retries": 1,
   "retry_delay": timedelta(minutes=30),
}

with airflow.DAG("${dag_full_name}",
                 default_args=default_dag_args,
                 catchup=False,
                 max_active_runs=1,
                 schedule_interval="${load_frequency}",
                 tags=${tags}) as dag:
    start_task = EmptyOperator(task_id="start")
    refresh_table = BigQueryInsertJobOperator(
            task_id="refresh_table",
            configuration={
                "query": {
                    "query": "${query_file}",
                    "useLegacySql": False,
                },
                "labels": _BQ_LABELS
            },
            gcp_conn_id="${lower_module_name}_${lower_tgt_dataset_type}_bq")
    stop_task = EmptyOperator(task_id="stop")

    start_task >> refresh_table >> stop_task
