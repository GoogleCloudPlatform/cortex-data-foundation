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
"""This DAG starts a Python file for processing LiveRamp ingestion."""

import configparser
from datetime import datetime
import importlib
import os

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# Use dynamic import to account for Airflow directory structure limitations.
_THIS_DIR = os.path.dirname(os.path.realpath(__file__))
_DEPENDENCIES_LIB_PATH = (os.path.join(
    _THIS_DIR,
    "pipelines.extract_data_from_liveramp").replace("/home/airflow/gcs/dags/",
                                                    "").replace("/", "."))

_LR_BQ_CONNECTION_ID = "liveramp_cdc_bq"
_SCHEDULE_INTERVAL = "@daily"
_DATASET_ID = "${dataset}"
_PROJECT_ID = "${project_id}"

_IDENTIFIER = f"liveramp_{_PROJECT_ID}_{_DATASET_ID}_extract_to_bq"
_START_DATE = datetime.fromisoformat("${start_date}")

liveramp_to_bq_module = importlib.import_module(_DEPENDENCIES_LIB_PATH)

extract_liveramp_ids = liveramp_to_bq_module.extract_ramp_ids_from_liveramp

dag_config_path = os.path.join(_THIS_DIR, "pipelines/config.ini")
config = configparser.ConfigParser()
config.read(dag_config_path)

# TODO: Investigate to compose a dictionary from config then pass it to the
# PythonOperator.

# Check DAG config sections.
retry_delay_sec = int(config.get("liveramp", "retry_delay_sec", fallback=60))
execution_retry_count = config.get("liveramp",
                                   "execution_retry_count",
                                   fallback=3)

# Check pipeline config sections.
http_timeout = int(config.get("liveramp", "http_timeout_sec", fallback=60))
liveramp_base_url = config.get("liveramp",
                               "liveramp_api_base_url",
                               fallback="https://us.identity.api.liveramp.com")

liveramp_lookup_api_url = f"{liveramp_base_url}/v1/batch/lookup"
liveramp_auth_url = f"{liveramp_base_url}/token"

with DAG(dag_id=_IDENTIFIER,
         description="Extract RampIds from LiveRamp Lookup to BQ",
         schedule=_SCHEDULE_INTERVAL,
         start_date=_START_DATE,
         tags=["liveramp", "cdc"],
         catchup=False,
         max_active_runs=1) as dag:

    start_task = EmptyOperator(task_id="start",
                               depends_on_past=True,
                               wait_for_downstream=True)

    extract_data = PythonOperator(task_id=_IDENTIFIER,
                                  python_callable=extract_liveramp_ids,
                                  op_args=[
                                      "${project_id}", "${dataset}",
                                      http_timeout, liveramp_lookup_api_url,
                                      liveramp_auth_url, _LR_BQ_CONNECTION_ID
                                  ],
                                  dag=dag,
                                  retries=execution_retry_count,
                                  retry_delay=retry_delay_sec)

    stop_task = EmptyOperator(task_id="stop")

start_task >> extract_data >> stop_task  # pylint: disable=pointless-statement
