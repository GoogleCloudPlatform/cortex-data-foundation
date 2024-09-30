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
from airflow.operators.dummy_operator import DummyOperator

from datetime import timedelta, datetime
import airflow
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.version import version as AIRFLOW_VERSION


default_dag_args = {
   'depends_on_past': False,
   'start_date': datetime(${year}, ${month}, ${day}),
   'catchup': False,
   'retries': 1,
   'retry_delay': timedelta(minutes=30),
}

with airflow.DAG("CDC_BigQuery_${base_table}",
                 template_searchpath=['/home/airflow/gcs/data/bq_data_replication/'],
                 default_args=default_dag_args,
                 catchup=False,
                 max_active_runs=1,
                 schedule_interval="${load_frequency}") as dag:
    start_task = DummyOperator(task_id="start")
    if AIRFLOW_VERSION.startswith("1."):
        copy_records = BigQueryOperator(
            task_id='merge_query_records',
            sql="${query_file}",
            create_disposition='CREATE_IF_NEEDED',
            bigquery_conn_id="sap_cdc_bq",
            use_legacy_sql=False)
    else:
        copy_records = BigQueryOperator(
            task_id='merge_query_records',
            sql="${query_file}",
            create_disposition='CREATE_IF_NEEDED',
            gcp_conn_id="sap_cdc_bq",
            use_legacy_sql=False)
    stop_task = DummyOperator(task_id="stop")
    start_task >> copy_records >> stop_task
