# Copyright 2025 Google LLC

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
from airflow import __version__ as airflow_version
from packaging.version import Version
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import \
    BigQueryInsertJobOperator

# BigQuery Job Labels - converts generated string to dict
# If string is empty, assigns empty dict
_BQ_LABELS = ast.literal_eval("${runtime_labels_dict}" or "{}")
_BQ_LOCATION = "${bq_location}"
_GCP_CONN_ID = "${module_name}_${tgt_dataset_type}_bq"

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

with airflow.DAG("${dag_full_name}",
                 default_args=default_dag_args,
                 catchup=False,
                 max_active_runs=1,
                 tags=ast.literal_eval("${tags}"),
                 **schedule_kwarg) as dag:

    start_task = EmptyOperator(task_id="start")

    stop_task = EmptyOperator(task_id="stop")
