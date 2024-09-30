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
"""This DAG starts a Beam pipeline for processing Ads CDC layer ingestion."""

import ast
import configparser
from datetime import datetime
from datetime import timedelta
import os
from pathlib import Path

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.empty import EmptyOperator

_CDC_SQL_PATH = "${cdc_sql_path}"
_DAG_SCHEDULE = "${load_frequency}"
_PROJECT_ID = "${project_id}"
_DATASET_ID = "${cdc_dataset}"
_TABLE_NAME = "${table_name}"

# BigQuery Job Labels - converts generated string to dict
# If string is empty, assigns empty dict
_BQ_LABELS = ast.literal_eval("${runtime_labels_dict}" or "{}")

_START_DATE = datetime.fromisoformat("${start_date}")
_THIS_DIR = Path(__file__).resolve().parent
_IDENTIFIER = f"googleads_{_PROJECT_ID}_{_DATASET_ID}_raw_to_cdc_{_TABLE_NAME}"

# Load config values.
dag_config_path = os.path.join(_THIS_DIR, "config.ini")
config = configparser.ConfigParser()
config.read(dag_config_path)

# Check DAG config sections.
retry_delay_sec = config.getint("googleads", "retry_delay_sec", fallback=60)
execution_retry_count = config.getint("googleads",
                                      "execution_retry_count",
                                      fallback=3)

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
    "configuration": {
        "query": {
            "query": _CDC_SQL_PATH,
            "useLegacySql": False,
        },
        "labels": _BQ_LABELS
    },
    "gcp_conn_id": "googleads_cdc_bq",
    "retries": execution_retry_count,
    "retry_delay": retry_delay_sec
}

with DAG(**_DAG_OPTIONS, schedule=_DAG_SCHEDULE) as dag:

    start_task = EmptyOperator(**_START_TASK_OPTIONS)
    copy_raw_to_cdc = BigQueryInsertJobOperator(**_BQ_OPTIONS)
    stop_task = EmptyOperator(task_id="stop")

start_task >> copy_raw_to_cdc >> stop_task  # pylint: disable=pointless-statement
