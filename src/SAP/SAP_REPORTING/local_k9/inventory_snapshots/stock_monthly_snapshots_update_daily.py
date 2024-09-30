# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

## CORTEX-CUSTOMER: These procedures need to execute for inventory views to work
## please check the ERD linked in the README for dependencies. The procedures
## can be scheduled with Cloud Composer with the provided templates or ported
## into the scheduling tool of choice. These DAGs will be executed from a
## different directory structure in future releases.
## PREVIEW
"""Cloud Composer DAG to regenerate stock monthly snapshot daily.
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
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(dag_id="Stock_Monthly_Snapshots_Daily_Update",
         default_args=default_args,
         description="Update monthly snapshot with new data everyday",
         schedule_interval="@daily",
         start_date=datetime(2023, 2, 13),
         catchup=False,
         max_active_runs=1) as dag:

    start_task = EmptyOperator(task_id="start")

    t1 = BigQueryInsertJobOperator(
        task_id="snapshot_creation_daily",
        gcp_conn_id="sap_reporting_bq",
        configuration={
            "query": {
                "query": "stock_monthly_snapshots_update_daily.sql",
                "useLegacySql": False
            },
            "labels": _BQ_LABELS
        })

    stop_task = EmptyOperator(task_id="stop")

    # pylint:disable=pointless-statement
    start_task >> t1 >> stop_task  # type: ignore
