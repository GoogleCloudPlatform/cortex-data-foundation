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
"""DAG to generate flattened profit center hierarchy tables."""

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
## 1-flattened profit center table.
## 2-profit center and node mapping.
with DAG(
        'profit_center',
        default_args=default_args,
        schedule_interval='@monthly',
        start_date=datetime(2023, 11, 27),
        catchup=False,
        max_active_runs=1
) as dag:
    start_task = DummyOperator(task_id='start')

    if AIRFLOW_VERSION.startswith('1.'):
        ## This task creates the flattened profit center table.
        profitcenter_flattened = BigQueryOperator(
            task_id='profitcenter_flattened',
            sql='profitcenter_hierarchy.sql',
            create_disposition='CREATE_IF_NEEDED',
            write_disposition='WRITE_TRUNCATE',
            bigquery_conn_id='sap_reporting_bq',
            use_legacy_sql=False)
         ## This task deletes the hierarchy from a specific node.
        delete_profitcenter_node = BigQueryOperator(
            task_id='delete_profitcenter_node',
            sql='profitcenter_node_deletion.sql',
            create_disposition='CREATE_IF_NEEDED',
            write_disposition='WRITE_TRUNCATE',
            bigquery_conn_id='sap_reporting_bq',
            use_legacy_sql=False)
        ## This task creates the profit center mapping table.
        profitcenter_mapping = BigQueryOperator(
            task_id='profitcenter_mapping',
            sql='profitcenter_node_mapping.sql',
            create_disposition='CREATE_IF_NEEDED',
            write_disposition='WRITE_TRUNCATE',
            bigquery_conn_id='sap_reporting_bq',
            use_legacy_sql=False)

        stop_task = DummyOperator(task_id='stop')
    else:
        ## This task creates the flattened profit center table.
        profitcenter_flattened = BigQueryOperator(
            task_id='profitcenter_flattened',
            sql='profitcenter_hierarchy.sql',
            create_disposition='CREATE_IF_NEEDED',
            write_disposition='WRITE_TRUNCATE',
            gcp_conn_id='sap_reporting_bq',
            use_legacy_sql=False)
         ## This task deletes the hierarchy from a specific node.
        delete_profitcenter_node = BigQueryOperator(
            task_id='delete_profitcenter_node',
            sql='profitcenter_node_deletion.sql',
            create_disposition='CREATE_IF_NEEDED',
            write_disposition='WRITE_TRUNCATE',
            gcp_conn_id='sap_reporting_bq',
            use_legacy_sql=False)
        ## This task creates the profit center mapping table.
        profitcenter_mapping = BigQueryOperator(
            task_id='profitcenter_mapping',
            sql='profitcenter_node_mapping.sql',
            create_disposition='CREATE_IF_NEEDED',
            write_disposition='WRITE_TRUNCATE',
            gcp_conn_id='sap_reporting_bq',
            use_legacy_sql=False)

        stop_task = DummyOperator(task_id='stop')
  # pylint:disable=pointless-statement
    (start_task >> profitcenter_flattened >> delete_profitcenter_node
      >> profitcenter_mapping >> stop_task)
