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

import ast
import configparser
from datetime import datetime
from datetime import timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

_PROJECT_ID = "${cdc_project_id}"
_DATASET_ID = "${cdc_dataset}"
_TABLE_NAME = "${table_name}"

# BigQuery Job Labels - converts generated string to dict
# If string is empty, assigns empty dict
_BQ_LABELS = ast.literal_eval("${runtime_labels_dict}" or "{}")

# Read settings from ini file.
current_dir = Path(__file__).resolve().parent
config = configparser.ConfigParser()
config.read(Path(current_dir, "config.ini"), encoding="utf-8")

retry_delay_sec = config.getint("dv360", "retry_delay_sec", fallback=60)
max_retry_delay_sec = config.getint("dv360",
                                    "max_retry_delay_sec",
                                    fallback=3600)
execution_retry_count = config.getint("dv360",
                                      "execution_retry_count",
                                      fallback=3)

_IDENTIFIER = f"dv360_{_PROJECT_ID}_{_DATASET_ID}_raw_to_cdc_{_TABLE_NAME}"

_DAG_OPTIONS = {
    "dag_id": _IDENTIFIER,
    "description": f"Extract {_TABLE_NAME} from Raw to CDC.",
    "start_date": datetime.fromisoformat("${start_date}"),
    "dagrun_timeout": timedelta(minutes=60),
    "schedule": "${load_frequency}",
    "tags": ["dv360", "cdc"],
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
            "query": "${cdc_sql_path}",
            "useLegacySql": False,
        },
        "labels": _BQ_LABELS
    },
    "max_retry_delay": timedelta(seconds=max_retry_delay_sec),
    "retry_delay": timedelta(seconds=retry_delay_sec),
    "retries": execution_retry_count,
    "gcp_conn_id": "dv360_cdc_bq",
    "location": "${bq_location}"
}

with DAG(**_DAG_OPTIONS) as dag:
    start_task = EmptyOperator(**_START_TASK_OPTIONS)
    raw_to_cdc = BigQueryInsertJobOperator(**_BQ_OPTIONS)
    stop_task = EmptyOperator(task_id="stop")

start_task >> raw_to_cdc >> stop_task  # pylint: disable=pointless-statement
