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
"""This DAG starts a Beam pipeline for processing Ads RAW layer ingestion."""

import configparser
from datetime import datetime
from datetime import timedelta
from pathlib import Path

from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowConfiguration

_API_VERSION = "v17"
_TABLE_NAME = "${table_name}"
_DATASET_ID = "${raw_dataset}"
_PROJECT_ID = "${project_id}"
_GCP_CONN_ID = "googleads_raw_dataflow"
_THIS_DIR = Path(__file__).resolve().parent

_IDENTIFIER = f"googleads_{_PROJECT_ID}_{_DATASET_ID}_extract_to_raw_{_TABLE_NAME}"
_START_DATE = datetime.fromisoformat("${start_date}")

# Read settings from ini file.
config = configparser.ConfigParser()
config.read(Path(_THIS_DIR, "pipelines/config.ini"), encoding="utf-8")

retry_delay_sec = config.getint("googleads", "retry_delay_sec", fallback=60)
max_retry_delay_sec = config.getint("googleads",
                                    "max_retry_delay_sec",
                                    fallback=3600)
execution_retry_count = config.getint("googleads",
                                      "execution_retry_count",
                                      fallback=3)
lookback_days = config.getint("googleads",
                              "lookback_days",
                              fallback=180)
pipeline_logging_level = config.get("googleads",
                                    "pipeline_logging_level",
                                    fallback="INFO")

_DAG_OPTIONS = {
    "dag_id": _IDENTIFIER,
    "description": f"Extract from source to raw {_TABLE_NAME} entities",
    "start_date": _START_DATE,
    "dagrun_timeout": timedelta(minutes=60),
    "tags": ["GoogleAds", "raw"],
    "catchup": False,
    "max_active_runs": 1
}

_START_TASK_OPTIONS = {
    "task_id": "start",
    "depends_on_past": True,
    "wait_for_downstream": True
}

beam_pipeline_params = {
    "api_version": _API_VERSION,
    "api_name": "${api_name}",
    "tgt_project": _PROJECT_ID,
    "tgt_dataset": _DATASET_ID,
    "tgt_table": _TABLE_NAME,
    "mapping_file": str(Path(_THIS_DIR, "${schemas_dir}", "${schema_file}")),
    "lookback_days": lookback_days,
    "resource_type": "${resource_type}",
    "setup_file": str(Path(_THIS_DIR, "${pipeline_setup}")),
    "tempLocation": "${pipeline_temp_location}",
    "stagingLocation": "${pipeline_staging_location}",
    "retry_delay": timedelta(seconds=retry_delay_sec),
    "max_retry_delay": timedelta(seconds=max_retry_delay_sec),
    "retries": execution_retry_count,
    "pipeline_logging_level": pipeline_logging_level
}

_DATAFLOW_CONFIG = {
    "project_id": "${project_id}",
    "location": "${project_region}",
    "gcp_conn_id": _GCP_CONN_ID,
    "wait_until_finished": True,
    "job_name": _IDENTIFIER.lower()
}

_BEAM_OPERATOR_CONFIG = {
    "task_id": _IDENTIFIER,
    "runner": "DataflowRunner",
    "py_file": str(Path(_THIS_DIR, "${pipeline_file}")),
    "pipeline_options": beam_pipeline_params,
    "py_system_site_packages": False,
    "dataflow_config": DataflowConfiguration(**_DATAFLOW_CONFIG),
    "py_requirements": [
        "apache-beam[gcp]==2.53.0", "google-cloud-secret-manager==2.16.3",
        "google-ads==25.0.0"
    ],
    "gcp_conn_id": _GCP_CONN_ID,
    "retry_exponential_backoff": True
}

with DAG(**_DAG_OPTIONS, schedule="${load_frequency}") as dag:

    start_task = EmptyOperator(**_START_TASK_OPTIONS)
    extract_data = BeamRunPythonPipelineOperator(**_BEAM_OPERATOR_CONFIG)
    stop_task = EmptyOperator(task_id="stop")

start_task >> extract_data >> stop_task  # pylint: disable=pointless-statement
