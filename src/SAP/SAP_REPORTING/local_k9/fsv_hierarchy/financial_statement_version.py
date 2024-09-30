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
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.version import version as AIRFLOW_VERSION

default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
## This DAG creates following two tables
## and deletes hierarchy from a specific node(if needed).
## 1-flattened fsv table.
## 2-glaccount and fsv node mapping.
with DAG(
        'financial_statement_version',
        default_args=default_args,
        schedule_interval='@monthly',
        start_date=datetime(2023, 8, 4),
        catchup=False,
        max_active_runs=1
) as dag:
    start_task = DummyOperator(task_id='start')

    if AIRFLOW_VERSION.startswith('1.'):
        ## This task creates the flattened fsv table.
        fsv_flattened = BigQueryOperator(
            task_id='fsv_flattened',
            sql='financial_statement_version.sql',
            create_disposition='CREATE_IF_NEEDED',
            write_disposition='WRITE_TRUNCATE',
            bigquery_conn_id='sap_reporting_bq',
            use_legacy_sql=False)
         ## This task deletes the hierarchy from a specific node.
        delete_fsv_node = BigQueryOperator(
            task_id='delete_fsv_node',
            sql='fsv_delete_node.sql',
            create_disposition='CREATE_IF_NEEDED',
            write_disposition='WRITE_TRUNCATE',
            bigquery_conn_id='sap_reporting_bq',
            use_legacy_sql=False)
        ## This task creates the fsv & glaccounts mapping table.
        fsv_glaccounts_mapping = BigQueryOperator(
            task_id='fsv_glaccounts_mapping',
            sql='fsv_glaccounts_mapping.sql',
            create_disposition='CREATE_IF_NEEDED',
            write_disposition='WRITE_TRUNCATE',
            bigquery_conn_id='sap_reporting_bq',
            use_legacy_sql=False)

        stop_task = DummyOperator(task_id='stop')
    else:
        ## This task creates the flattened fsv table.
        fsv_flattened = BigQueryOperator(
            task_id='fsv_flattened',
            sql='financial_statement_version.sql',
            create_disposition='CREATE_IF_NEEDED',
            write_disposition='WRITE_TRUNCATE',
            gcp_conn_id='sap_reporting_bq',
            use_legacy_sql=False)
         ## This task deletes the hierarchy from a specific node.
        delete_fsv_node = BigQueryOperator(
            task_id='delete_fsv_node',
            sql='fsv_delete_node.sql',
            create_disposition='CREATE_IF_NEEDED',
            write_disposition='WRITE_TRUNCATE',
            gcp_conn_id='sap_reporting_bq',
            use_legacy_sql=False)
        ## This task creates the fsv & glaccounts mapping table.
        fsv_glaccounts_mapping = BigQueryOperator(
            task_id='fsv_glaccounts_mapping',
            sql='fsv_glaccounts_mapping.sql',
            create_disposition='CREATE_IF_NEEDED',
            write_disposition='WRITE_TRUNCATE',
            gcp_conn_id='sap_reporting_bq',
            use_legacy_sql=False)

        stop_task = DummyOperator(task_id='stop')
  # pylint:disable=pointless-statement
    (start_task >> fsv_flattened >> delete_fsv_node
      >> fsv_glaccounts_mapping >> stop_task)
