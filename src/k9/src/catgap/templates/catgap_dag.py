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
"""
Cortex CATGAP - Airflow DAG.
"""
# pylint: disable=C0415

from datetime import timedelta
import uuid

from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import (
    DataflowTemplatedJobStartOperator)

try:
    from pendulum import DateTime as Pendulum
except ImportError:
    from pendulum import Pendulum
from pendulum import UTC

default_args = {
    "depends_on_past": False,
    "start_date": Pendulum.now(UTC) - timedelta(days=1, minutes=10),
    "catchup": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(dag_id="CATGAP_PIPELINE",
         description=("Runs Dataflow Pipeline for CATGAP"),
         default_args=default_args,
         schedule_interval="@daily",
         catchup=False,
         max_active_runs=1) as dag:
    _ = DataflowTemplatedJobStartOperator(
        task_id="CATGAP_PIPELINE_EXECUTE",
        job_name=f"catgap-{uuid.uuid4().hex}",
        template=("gs://{{ dataflow_bucket }}/CATGAP/dataflow/"
                  "{{ deployment_id }}/template.json"),
        project_id="{{ project_id_src }}",
        location="{{ dataflow_region }}",
        options={
            "tempLocation": ("gs://{{ dataflow_bucket }}/CATGAP/dataflow/"
                             "{{ deployment_id }}/temp")
        },
        wait_until_finished=True,
        dag=dag)
