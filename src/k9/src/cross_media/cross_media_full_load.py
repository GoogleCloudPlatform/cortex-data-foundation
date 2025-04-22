#-- Copyright 2025 Google LLC
#--
#-- Licensed under the Apache License, Version 2.0 (the "License");
#-- you may not use this file except in compliance with the License.
#-- You may obtain a copy of the License at
#--
#--   https://www.apache.org/licenses/LICENSE-2.0
#--
#-- Unless required by applicable law or agreed to in writing, software
#-- distributed under the License is distributed on an "AS IS" BASIS,
#-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#-- See the License for the specific language governing permissions and
#-- limitations under the License.

"""Cloud Composer DAG to load Cortex Cross Media views for the first time.

- Clear all existing mappings
- Generate product mappings for all campaigns.
- Regenerate all metrics in the final output table.
"""
import ast
import os
import sys

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

sys.path.append(os.path.dirname(os.path.realpath(__file__)))

# pylint: disable=wrong-import-position
from dependencies.cross_media_matching import run_cross_media_matching

# BigQuery Job Labels - converts generated string to dict
# If string is empty, assigns empty dict
_BQ_LABELS = ast.literal_eval("{{ runtime_labels_dict }}" or "{}")
_AIRFLOW_CONNECTION_ID = "k9_reporting"
_BQ_LOCATION = "{{ location }}"

default_args = {
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def _get_bq_operator(dag_name: str) -> BigQueryInsertJobOperator:
    return BigQueryInsertJobOperator(
        task_id=dag_name,
        gcp_conn_id=_AIRFLOW_CONNECTION_ID,
        configuration={
            "query": {
                "query": f"{dag_name}.sql",
                "useLegacySql": False
            },
            "labels": _BQ_LABELS,
            "location": _BQ_LOCATION
        })

def _map_campaigns_to_products():
    run_cross_media_matching(True, _BQ_LABELS, _AIRFLOW_CONNECTION_ID)

with DAG("cross_media_full_load",
         description="Cross-Media Full/Initial Matching DAG",
         default_args=default_args,
         schedule_interval=None,
         start_date=datetime(2022, 11, 2),
         catchup=False,
         max_active_runs=1,
         user_defined_macros={"initial_load": True}) as dag:
    start_task = EmptyOperator(task_id="start")
    generate_campaign_texts = _get_bq_operator("generate_campaign_texts")
    create_mapping_table = _get_bq_operator("create_mapping_table")
    map_campaigns_to_products = PythonOperator(
        task_id="map_campaigns_to_products",
        python_callable=_map_campaigns_to_products,
        op_args=[],
        dag=dag,
        retries=3,
        retry_delay=60)
    regenerate_metrics = _get_bq_operator("regenerate_metrics")
    stop_task = EmptyOperator(task_id="stop")

    # pylint:disable=pointless-statement
    (start_task >> generate_campaign_texts >> create_mapping_table >>
     map_campaigns_to_products >> regenerate_metrics >> stop_task)
