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

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.version import version as AIRFLOW_VERSION
# from __future__ import print_function

default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'Product_Hierarchy_Text',
        default_args=default_args,
        schedule_interval='@yearly',
        start_date=datetime(2021, 1, 1),
        catchup=False,
        max_active_runs=1
) as dag:
    start_task = DummyOperator(task_id='start')
    if AIRFLOW_VERSION.startswith("1."):
        get_prodhier_texts = BigQueryOperator(
            task_id='get_prodhier_texts',
            sql='prod_hierarchy_texts.sql',
            create_disposition='CREATE_IF_NEEDED',
            bigquery_conn_id='sap_reporting_bq',
            use_legacy_sql=False)
    else:
        get_prodhier_texts = BigQueryOperator(
            task_id='get_prodhier_texts',
            sql='prod_hierarchy_texts.sql',
            create_disposition='CREATE_IF_NEEDED',
            gcp_conn_id='sap_reporting_bq',
            use_legacy_sql=False)

    stop_task = DummyOperator(task_id='stop')

    start_task >> get_prodhier_texts >> stop_task
