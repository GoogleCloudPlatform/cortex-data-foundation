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
"""This is an Airflow DAG template for exrtaction pipeline to RAW layer."""

from datetime import datetime, timedelta
import importlib
import os

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# Use dynamic import to account for Airflow directory structure limitations.
_THIS_DIR = os.path.dirname(os.path.realpath(__file__))
_DEPENDENCIES_LIB_PATH = (
    os.path.join(_THIS_DIR, "dag_dependencies.airflow_sfdc2bq")
    .replace("/home/airflow/gcs/dags/", "")
    .replace("/", ".")
)

sfdc_to_bigquery_module = importlib.import_module(_DEPENDENCIES_LIB_PATH)

_IDENTIFIER = "SFDC_${project_id}_${raw_dataset}_extract_to_raw_${base_table}"

default_args = {
   "depends_on_past": False,
   "start_date": datetime(int("${year}"), int("${month}"), int("${day}")),
   "catchup": False,
   "retries": 1,
   "retry_delay": timedelta(minutes=10),
}

with DAG(dag_id=_IDENTIFIER,
         description=(
            "Data extraction from Salesforce system to BQ RAW dataset "
            "for '${base_table}' object"),
         default_args=default_args,
         schedule_interval="${load_frequency}",
         tags=["sfdc", "raw"],
         catchup = False,
         max_active_runs=1) as dag:
    start_task = EmptyOperator(task_id="start")

    extract_data = PythonOperator(
        task_id=_IDENTIFIER,
        python_callable=sfdc_to_bigquery_module.extract_data_from_sfdc,
        op_args = [
            # TODO: Load this Salesforce connection name from some config.
            "salesforce-conn",
            "${api_name}",
            # TODO: Load this BigQuery connection name from some config.
            "sfdc_cdc_bq",
            "${project_id}",
            "${raw_dataset}",
            "${base_table}"],
        dag=dag)

    stop_task = EmptyOperator(task_id="stop")

start_task >> extract_data >> stop_task # pylint: disable=pointless-statement
