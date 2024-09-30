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
"""
This DAG starts a Beam pipeline for processing Salesforce Marketing Cloud
RAW layer ingestion.
"""

import configparser
from datetime import datetime
from datetime import timedelta
import os
from pathlib import Path

from airflow import DAG
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowConfiguration
from airflow.version import version as AIRFLOW_VERSION

_TABLE_NAME = "${table_name}"
_DATASET_ID = "${raw_dataset}"
_PROJECT_ID = "${project_id}"
_GCP_CONN_ID = "sfmc_raw_dataflow"

_CURRENT_DIR = Path(__file__).resolve().parent

# Read settings from ini file.
config = configparser.ConfigParser()
config.read(Path(_CURRENT_DIR, "pipelines/config.ini"), encoding="utf-8")

retry_delay_sec = config.getint("sfmc", "retry_delay_sec", fallback=60)
max_retry_delay_sec = config.getint("sfmc",
                                    "max_retry_delay_sec",
                                    fallback=3600)
execution_retry_count = config.getint("sfmc",
                                      "execution_retry_count",
                                      fallback=3)

_IDENTIFIER = ("sfmc_"
               f"{_PROJECT_ID}_{_DATASET_ID}_extract_to_raw_{_TABLE_NAME}")
_START_DATE = datetime.fromisoformat("${start_date}")

_DAG_OPTIONS = {
    "dag_id": _IDENTIFIER,
    "description": f"Extract from source to raw {_TABLE_NAME} entities",
    "start_date": _START_DATE,
    "dagrun_timeout": timedelta(minutes=60),
    "tags": ["sfmc", "raw"],
    "catchup": False,
    "max_active_runs": 1
}

_START_TASK_OPTIONS = {
    "task_id": "start",
    "depends_on_past": True,
    "wait_for_downstream": True
}

_input_file_path_pattern = os.path.join("gs://${data_transfer_bucket}",
                                        "${file_pattern}")

beam_pipeline_params = {
    "setup_file": str(Path(_CURRENT_DIR, "${pipeline_setup}")),
    "tempLocation": "${pipeline_temp_bucket}",
    "stagingLocation": "${pipeline_staging_bucket}",
    "input_file_path_pattern": _input_file_path_pattern,
    "mapping_file": str(Path(_CURRENT_DIR, "${schemas_dir}", "${schema_file}")),
    "tgt_project": "${project_id}",
    "tgt_dataset": "${raw_dataset}",
    "tgt_table": _TABLE_NAME
}

_DATAFLOW_CONFIG = {
    "project_id": "${project_id}",
    "location": "${project_region}",
    "wait_until_finished": True,
    "poll_sleep": 60,
    "gcp_conn_id": _GCP_CONN_ID,
    "job_name": _IDENTIFIER.lower()
}

_BEAM_OPERATOR_CONFIG = {
    # from DataflowHook: name must consist of only the
    # characters [-a-z0-9]
    "task_id": _IDENTIFIER,
    "runner": "DataflowRunner",
    "py_file": str(Path(_CURRENT_DIR, "${pipeline_file}")),
    "pipeline_options": beam_pipeline_params,
    "py_system_site_packages": False,
    "dataflow_config": DataflowConfiguration(**_DATAFLOW_CONFIG),
    "gcp_conn_id": _GCP_CONN_ID,
    "py_requirements": ["apache-beam[gcp]==2.53.0"],
    "retry_delay": timedelta(seconds=retry_delay_sec),
    "max_retry_delay": timedelta(seconds=max_retry_delay_sec),
    "retries": execution_retry_count
}

if AIRFLOW_VERSION.startswith("1."):
    with DAG(**_DAG_OPTIONS, schedule_interval="${load_frequency}") as dag:
        # Import here to avoid slow DAG imports in Airflow.
        from airflow.operators.dummy_operator import DummyOperator

        start_task = DummyOperator(**_START_TASK_OPTIONS)
        extract_data = BeamRunPythonPipelineOperator(**_BEAM_OPERATOR_CONFIG)
        stop_task = DummyOperator(task_id="stop")
else:
    with DAG(**_DAG_OPTIONS, schedule="${load_frequency}") as dag:
        # Import here to avoid slow DAG imports in Airflow.
        from airflow.operators.empty import EmptyOperator

        start_task = EmptyOperator(**_START_TASK_OPTIONS)
        extract_data = BeamRunPythonPipelineOperator(**_BEAM_OPERATOR_CONFIG)
        stop_task = EmptyOperator(task_id="stop")

start_task >> extract_data >> stop_task  # pylint: disable=pointless-statement
