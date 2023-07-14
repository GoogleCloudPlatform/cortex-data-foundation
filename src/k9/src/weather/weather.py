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
"""Cloud Composer DAG to generate weather related data.

Executes relevant nodes to create underlying tables as well as final table.
"""

# TODO: Adjust schedule based on how other related DAGs are scheduled.

from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.version import version as AIRFLOW_VERSION
from datetime import datetime, timedelta

default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'Weather',
        default_args=default_args,
        template_searchpath=['/home/airflow/gcs/dags/weather/'],
        description='Generate weather related data',
        schedule_interval='@daily',
        start_date=datetime(2021, 1, 1),
        catchup=False,
        max_active_runs=1,
) as dag:

    start_task = DummyOperator(task_id='start')

    if AIRFLOW_VERSION.startswith("1."):
        refresh_postcode_table = BigQueryOperator(
            task_id='refresh_postcode_table',
            sql='postcode.sql',
            create_disposition='CREATE_IF_NEEDED',
            bigquery_conn_id='sap_cdc_bq',
            use_legacy_sql=False)

        update_daily_weather_table = BigQueryOperator(
            task_id='update_daily_weather_table',
            sql='weather_daily.sql',
            create_disposition='CREATE_IF_NEEDED',
            bigquery_conn_id='sap_cdc_bq',
            use_legacy_sql=False)

        refresh_weekly_weather_table = BigQueryOperator(
            task_id='refresh_weekly_weather_table',
            sql='weather_weekly.sql',
            create_disposition='CREATE_IF_NEEDED',
            bigquery_conn_id='sap_cdc_bq',
            use_legacy_sql=False)

    else:
        refresh_postcode_table = BigQueryOperator(
            task_id='refresh_postcode_table',
            sql='postcode.sql',
            create_disposition='CREATE_IF_NEEDED',
            gcp_conn_id='sap_cdc_bq',
            use_legacy_sql=False)

        update_daily_weather_table = BigQueryOperator(
            task_id='update_daily_weather_table',
            sql='weather_daily.sql',
            create_disposition='CREATE_IF_NEEDED',
            gcp_conn_id='sap_cdc_bq',
            use_legacy_sql=False)

        refresh_weekly_weather_table = BigQueryOperator(
            task_id='refresh_weekly_weather_table',
            sql='weather_weekly.sql',
            create_disposition='CREATE_IF_NEEDED',
            gcp_conn_id='sap_cdc_bq',
            use_legacy_sql=False)

    stop_task = DummyOperator(task_id='stop')

    (start_task >> refresh_postcode_table >> update_daily_weather_table >>
     refresh_weekly_weather_table >> stop_task)
