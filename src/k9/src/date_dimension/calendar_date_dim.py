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
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.version import version as AIRFLOW_VERSION

default_args = {
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
with DAG(
        'calendar_date_dim',
        template_searchpath=['/home/airflow/gcs/dags/date_dimension/'],
        default_args=default_args,
        schedule_interval='@yearly',
        start_date=datetime(2022, 11, 2),
        catchup=False,
        max_active_runs=1
) as dag:
    start_task = DummyOperator(task_id='start')
    if AIRFLOW_VERSION.startswith("1."):
        calendar_date_dim = BigQueryOperator(
            task_id='calendar_date_dim',
            sql='calendar_date_dim.sql',
            create_disposition='WRITE_TRUNCATE',
            bigquery_conn_id='sap_cdc_bq',
            use_legacy_sql=False)
    else:
        calendar_date_dim = BigQueryOperator(
            task_id='calendar_date_dim',
            sql='calendar_date_dim.sql',
            create_disposition='WRITE_TRUNCATE',
            gcp_conn_id='sap_cdc_bq',
            use_legacy_sql=False)
    stop_task = DummyOperator(task_id='stop')
    # pylint:disable=pointless-statement
    (start_task >> calendar_date_dim >> stop_task)
