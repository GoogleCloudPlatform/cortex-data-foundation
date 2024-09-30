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

# Disable pylance / pylint as errors
# type: ignore
"""Airlow DAG file for SFDC RAW to CDC layer."""

import ast
from datetime import timedelta
import logging

try:
    from pendulum import DateTime as Pendulum
except ImportError:
    from pendulum import Pendulum
from pendulum import UTC

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.exceptions import AirflowRescheduleException
from airflow.models.dagrun import DagRun
from airflow.models.dagbag import DagBag
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.state import State
from airflow.utils.db import provide_session
from airflow.utils import timezone

try:
    from airflow.api.common.trigger_dag import trigger_dag
except ImportError:
    from airflow.api.common.experimental.trigger_dag import trigger_dag


_RAW_WAITING_TIMEOUT_MINUTES = 10
_RAW_AGE_HOURS_MAX = 12

default_args = {
    "depends_on_past": False,
    "start_date": Pendulum(int("${year}"), int("${month}"), int("${day}")),
    "catchup": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

_CDC_SQL_PATH = "sql_scripts/${base_table}.sql"
_IDENTIFIER = "SFDC_${project_id}_${cdc_dataset}_raw_to_cdc_${base_table}"
_RAW_DAG_ID = "SFDC_${project_id}_${raw_dataset}_extract_to_raw_${base_table}"

# BigQuery Job Labels - converts generated string to dict
# If string is empty, assigns empty dict
_BQ_LABELS = ast.literal_eval("${runtime_labels_dict}" or "{}")

# pylint: disable=unused-argument
@provide_session
def check_raw_if_deployed(session=None, **kwargs):
    del kwargs
    now = Pendulum.now(UTC)

    active_runs = DagRun.find(dag_id=_RAW_DAG_ID, state=State.RUNNING)
    if active_runs and len(active_runs) > 0:
        logging.info("Rescheduling to wait for an active run of the Raw DAG.")
        raise AirflowRescheduleException(now + timedelta(
            minutes=_RAW_WAITING_TIMEOUT_MINUTES))

    complete_runs: list[DagRun] = DagRun.find(dag_id=_RAW_DAG_ID,
                                              state=State.SUCCESS)
    run_raw_now = True
    if complete_runs and len(complete_runs) > 0:
        if (now - complete_runs[-1].execution_date
           ).total_hours() < _RAW_AGE_HOURS_MAX:
            run_raw_now = False
            logging.info("Found a recent run of the Raw DAG.")

    if run_raw_now:
        bag = DagBag()
        raw_dag: DAG = bag.get_dag(_RAW_DAG_ID)
        if not raw_dag:
            logging.info("No Raw DAG %s found.", _RAW_DAG_ID)
            return
        logging.info("Starting a new run of the Raw DAG")
        trigger_dag(
                dag_id=_RAW_DAG_ID,
                run_id=f"forced__{now.isoformat()}",
                conf=None,
                execution_date=timezone.utcnow(),
                replace_microseconds=False,
            )
        logging.info("Rescheduling to wait for a new run of the Raw DAG.")
        raise AirflowRescheduleException(now + timedelta(
            minutes=_RAW_WAITING_TIMEOUT_MINUTES))

with DAG(dag_id=_IDENTIFIER,
         description=(
             "Merge from Salesforce RAW BQ dataset to CDC BQ dataset for "
             "'${project_id}.${cdc_dataset}.${base_table}' table"),
         default_args=default_args,
         schedule_interval="${load_frequency}",
         catchup=False,
         tags=["sfdc","cdc"],
         max_active_runs=1) as dag:
    check_raw = PythonOperator(task_id="check_" + _RAW_DAG_ID,
                               python_callable=check_raw_if_deployed,
                               dag=dag)

    copy_raw_to_cdc = BigQueryInsertJobOperator(
        task_id=_IDENTIFIER,
        gcp_conn_id="sfdc_cdc_bq",
        configuration={
            "query": {
                "query": _CDC_SQL_PATH,
                "useLegacySql": False
            },
            "labels": _BQ_LABELS
        }
    )

    stop_task = EmptyOperator(task_id="stop")

check_raw >> copy_raw_to_cdc >> stop_task  # pylint: disable=pointless-statement
