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
"""This DAG starts a Beam pipeline for processing Ads CDC layer ingestion."""

from datetime import datetime
from datetime import timedelta

from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.version import version as AIRFLOW_VERSION

_CDC_SQL_PATH = "${cdc_sql_path}"
_DAG_SCHEDULE = "${load_frequency}"
_PROJECT_ID = "${project_id}"
_DATASET_ID = "${cdc_dataset}"
_TABLE_NAME = "${table_name}"

_START_DATE = datetime.fromisoformat("${start_date}")
_IDENTIFIER = f"googleads_{_PROJECT_ID}_{_DATASET_ID}_raw_to_cdc_{_TABLE_NAME}"

_DAG_OPTIONS = {
    "dag_id": _IDENTIFIER,
    "description": f"Extract {_TABLE_NAME} from raw to cdc",
    "start_date": _START_DATE,
    "dagrun_timeout": timedelta(minutes=60),
    "tags": ["GoogleAds", "cdc"],
    "catchup": False,
    "max_active_runs": 1
}

_START_TASK_OPTIONS = {
    "task_id": "start",
    "depends_on_past": True,
    "wait_for_downstream": True
}

_BQ_OPTIONS = {
    "task_id": _IDENTIFIER,
    "sql": _CDC_SQL_PATH,
    "use_legacy_sql": False,
    "retries": 0
}

if AIRFLOW_VERSION.startswith("1."):
    with DAG(**_DAG_OPTIONS, schedule_interval=_DAG_SCHEDULE) as dag:

        from airflow.operators.dummy_operator import DummyOperator

        start_task = DummyOperator(**_START_TASK_OPTIONS)
        copy_raw_to_cdc = BigQueryOperator(**_BQ_OPTIONS,
                                           bigquery_conn_id="googleads_cdc_bq")
        stop_task = DummyOperator(task_id="stop")
else:
    with DAG(**_DAG_OPTIONS, schedule=_DAG_SCHEDULE) as dag:

        from airflow.operators.empty import EmptyOperator

        start_task = EmptyOperator(**_START_TASK_OPTIONS)
        copy_raw_to_cdc = BigQueryOperator(**_BQ_OPTIONS,
                                           gcp_conn_id="googleads_cdc_bq")
        stop_task = EmptyOperator(task_id="stop")

start_task >> copy_raw_to_cdc >> stop_task  # pylint: disable=pointless-statement
