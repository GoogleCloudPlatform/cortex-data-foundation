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
"""This DAG populates data to RAW layer from Meta Marketing API."""

import configparser
from datetime import datetime
from datetime import timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowConfiguration

_TABLE_NAME = "${table_name}"
_DATASET_ID = "${dataset}"
_PROJECT_ID = "${project_id}"
_GCP_CONN_ID = "meta_raw_dataflow"

_CURRENT_DIR = Path(__file__).resolve().parent

# Read settings from ini file.
config = configparser.ConfigParser()
config.read(Path(_CURRENT_DIR, "pipelines/config.ini"), encoding="utf-8")

retry_delay_sec = config.getint("meta", "retry_delay_sec", fallback=60)
max_retry_delay_sec = config.getint("meta",
                                    "max_retry_delay_sec",
                                    fallback=3600)
execution_retry_count = config.getint("meta",
                                      "execution_retry_count",
                                      fallback=3)
batch_size_days = config.getint("meta", "batch_size_days", fallback=7)
http_timeout = config.getint("meta", "http_timeout_sec", fallback=60)
next_request_delay_sec = config.getfloat("meta",
                                         "next_request_delay_sec",
                                         fallback=1.0)
api_version = config.get("meta", "api_version", fallback="v19.0")

max_load_lookback_days = config.get("meta",
                                    "max_load_lookback_days",
                                    fallback=366)

pipeline_logging_level = config.get("meta",
                                    "pipeline_logging_level",
                                    fallback="INFO")


_IDENTIFIER = ("meta_"
               f"{_PROJECT_ID}_{_DATASET_ID}_extract_to_raw_{_TABLE_NAME}")
_START_DATE = datetime.fromisoformat("${start_date}")

_DAG_OPTIONS = {
    "dag_id": _IDENTIFIER,
    "description": f"Extract from source to raw {_TABLE_NAME} entities",
    "start_date": _START_DATE,
    "dagrun_timeout": timedelta(minutes=60),
    "tags": ["meta", "raw"],
    "catchup": False,
    "max_active_runs": 1
}

_START_TASK_OPTIONS = {
    "task_id": "start",
    "depends_on_past": True,
    "wait_for_downstream": True
}

beam_pipeline_params = {
    "setup_file":
        str(Path(_CURRENT_DIR, "${pipeline_setup}")),
    "tempLocation":
        "${pipeline_temp_bucket}",
    "stagingLocation":
        "${pipeline_staging_bucket}",
    "tgt_project":
        "${project_id}",
    "tgt_dataset":
        "${dataset}",
    "tgt_table":
        _TABLE_NAME,
    "mapping_file":
        str(Path(_CURRENT_DIR, "${schemas_dir}", f"{_TABLE_NAME}.csv")),
    "request_file":
        str(Path(_CURRENT_DIR, "${requests_dir}", f"{_TABLE_NAME}.yaml")),
    "entity_type":
        "${entity_type}",
    "object_endpoint":
        "${object_endpoint}",
    "object_id_column":
        "${object_id_column}",
    "breakdowns":
        "${breakdowns}",
    "action_breakdowns":
        "${action_breakdowns}",
    "batch_size":
        batch_size_days,
    "http_timeout":
        http_timeout,
    "next_request_delay_sec":
        next_request_delay_sec,
    "api_version":
        api_version,
    "max_load_lookback_days":
        max_load_lookback_days,
    "pipeline_logging_level":
        pipeline_logging_level
}

_DATAFLOW_CONFIG = {
    "project_id": "${project_id}",
    "location": "${project_region}",
    "wait_until_finished": True,
    "poll_sleep": 60,
    "gcp_conn_id": _GCP_CONN_ID,
    # from DataflowHook: name must consist of only the
    # characters [-a-z0-9]
    "job_name": _IDENTIFIER.lower()
}

_BEAM_OPERATOR_CONFIG = {
    "task_id":
        _IDENTIFIER,
    "runner":
        "DataflowRunner",
    "py_file":
        str(Path(_CURRENT_DIR, "pipelines", "meta_source_to_raw_pipeline.py")),
    "pipeline_options":
        beam_pipeline_params,
    "py_system_site_packages":
        False,
    "dataflow_config":
        DataflowConfiguration(**_DATAFLOW_CONFIG),
    "gcp_conn_id":
        _GCP_CONN_ID,
    "py_requirements": [
        "apache-beam[gcp]==2.53.0", "google-cloud-secret-manager", "requests",
        "pyyaml"
    ],
    "retry_exponential_backoff":
        True,
    "retry_delay":
        timedelta(seconds=retry_delay_sec),
    "max_retry_delay":
        timedelta(seconds=max_retry_delay_sec),
    "retries":
        execution_retry_count,
}

with DAG(**_DAG_OPTIONS, schedule="${load_frequency}") as dag:

    start_task = EmptyOperator(**_START_TASK_OPTIONS)
    extract_data = BeamRunPythonPipelineOperator(**_BEAM_OPERATOR_CONFIG)
    stop_task = EmptyOperator(task_id="stop")

start_task >> extract_data >> stop_task  # pylint: disable=pointless-statement
