# Copyright 2024 Google LLC

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""This DAG runs a BigQuery Job for Raw to CDC migration."""

import configparser
from datetime import datetime
from datetime import timedelta
from pathlib import Path

from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.version import version as AIRFLOW_VERSION

_PROJECT_ID = "${cdc_project_id}"
_DATASET_ID = "${cdc_dataset}"
_TABLE_NAME = "${table_name}"

# Read settings from ini file.
current_dir = Path(__file__).resolve().parent
config = configparser.ConfigParser()
config.read(Path(current_dir, "config.ini"), encoding="utf-8")

retry_delay_sec = config.getint("sfmc", "retry_delay_sec", fallback=60)
max_retry_delay_sec = config.getint("sfmc",
                                    "max_retry_delay_sec",
                                    fallback=3600)
execution_retry_count = config.getint("sfmc",
                                      "execution_retry_count",
                                      fallback=3)


_IDENTIFIER = f"sfmc_{_PROJECT_ID}_{_DATASET_ID}_raw_to_cdc_{_TABLE_NAME}"
_START_DATE = datetime.fromisoformat("${start_date}")

_DAG_OPTIONS = {
    "dag_id": _IDENTIFIER,
    "description": f"Extract {_TABLE_NAME} from Raw to CDC.",
    "start_date": _START_DATE,
    "dagrun_timeout": timedelta(minutes=60),
    "tags": ["sfmc", "cdc"],
    "catchup": False,
    "max_active_runs": 1
}

_START_TASK_OPTIONS = {
    "task_id": "start",
    "depends_on_past": True,
    "wait_for_downstream": True
}

_BQ_COMMON_OPTIONS = {
    "task_id": _IDENTIFIER,
    "sql": "${cdc_sql_path}",
    "use_legacy_sql": False,
    "max_retry_delay": timedelta(seconds=max_retry_delay_sec),
    "retry_delay": timedelta(seconds=retry_delay_sec),
    "retries": execution_retry_count
}

if AIRFLOW_VERSION.startswith("1."):
    with DAG(**_DAG_OPTIONS, schedule_interval="${load_frequency}") as dag:
        # Import here to avoid slow DAG imports in Airflow.
        from airflow.operators.dummy_operator import DummyOperator

        # Set Airflow version specific BQ options.
        _BQ_OPTIONS = {**_BQ_COMMON_OPTIONS, "bigquery_conn_id": "sfmc_cdc_bq"}

        start_task = DummyOperator(**_START_TASK_OPTIONS)
        raw_to_cdc = BigQueryOperator(**_BQ_OPTIONS)
        stop_task = DummyOperator(task_id="stop")
else:
    with DAG(**_DAG_OPTIONS, schedule="${load_frequency}") as dag:
        # Import here to avoid slow DAG imports in Airflow.
        from airflow.operators.empty import EmptyOperator

        # Set Airflow version specific BQ options.
        _BQ_OPTIONS = {**_BQ_COMMON_OPTIONS, "gcp_conn_id": "sfmc_cdc_bq"}

        start_task = EmptyOperator(**_START_TASK_OPTIONS)
        raw_to_cdc = BigQueryOperator(**_BQ_OPTIONS)
        stop_task = EmptyOperator(task_id="stop")

start_task >> raw_to_cdc >> stop_task  # pylint: disable=pointless-statement
