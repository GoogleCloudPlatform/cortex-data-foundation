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
import ast

from datetime import datetime
from datetime import timedelta

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.empty import EmptyOperator

# BigQuery Job Labels - converts generated string to dict
# If string is empty, assigns empty dict
_BQ_LABELS = ast.literal_eval("{{ runtime_labels_dict }}" or "{}")

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
        "Weather",
        default_args=default_args,
        description="Generate weather related data",
        schedule_interval="@daily",
        start_date=datetime(2021, 1, 1),
        catchup=False,
        max_active_runs=1,
) as dag:

    start_task = EmptyOperator(task_id="start")

    refresh_postcode_table = BigQueryInsertJobOperator(
        task_id="refresh_postcode_table",
        gcp_conn_id="sap_cdc_bq",
        configuration={
            "query": {
                "query": "postcode.sql",
                "useLegacySql": False
            },
            "labels": _BQ_LABELS
        })

    update_daily_weather_table = BigQueryInsertJobOperator(
        task_id="update_daily_weather_table",
        gcp_conn_id="sap_cdc_bq",
        configuration={
            "query": {
                "query": "weather_daily.sql",
                "useLegacySql": False
            },
            "labels": _BQ_LABELS
        })

    refresh_weekly_weather_table = BigQueryInsertJobOperator(
        task_id="refresh_weekly_weather_table",
        gcp_conn_id="sap_cdc_bq",
        configuration={
            "query": {
                "query": "weather_weekly.sql",
                "useLegacySql": False
            },
            "labels": _BQ_LABELS
        })

    stop_task = EmptyOperator(task_id="stop")

    # pylint: disable=pointless-statement
    (start_task >> refresh_postcode_table >> update_daily_weather_table >>
     refresh_weekly_weather_table >> stop_task)
