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

"""Cloud Composer DAG to generate stock monthly snapshot update.
"""

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.version import version as AIRFLOW_VERSION

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'Stock_Monthly_Snapshots_Periodical_Update',
    template_searchpath=['/home/airflow/gcs/data/bq_data_replication/'],
    default_args=default_args,
    description='Update monthly inventory snapshot every month.',
    schedule_interval='@monthly',
    start_date=datetime(2023, 2, 13),
    catchup=False,
    max_active_runs=1

) as dag:
    start_task = DummyOperator(task_id='start')
    if AIRFLOW_VERSION.startswith('1.'):
        t1 = BigQueryOperator(
            task_id='monthly_snapshot_update',
            sql='stock_monthly_snapshots_periodical_update.sql',
            create_disposition='WRITE_TRUNCATE',
            gcp_conn_id='sap_reporting_bq',
            use_legacy_sql=False
        )
    else:
        t1 = BigQueryOperator(
            task_id='monthly_snapshot_update',
            sql='stock_monthly_snapshots_periodical_update.sql',
            create_disposition='WRITE_TRUNCATE',
            gcp_conn_id='sap_reporting_bq',
            use_legacy_sql=False
        )
    stop_task = DummyOperator(task_id='stop')
# pylint:disable=pointless-statement
start_task >> t1 >> stop_task
