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

import sys
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

sys.path.append("/home/airflow/gcs/dags/sap/reporting/hier_reader")
import dag_hierarchies_module

default_dag_args = {
   "depends_on_past": False,
   "start_date": datetime(${year}, ${month}, ${day}),
   "retries": 1,
   "retry_delay": timedelta(minutes=30),
}

with DAG(dag_id="CDC_Hierarchy_${setname}",
         description="Hierarchy Resolution for ${setname}",
         schedule_interval="${load_frequency}",
         default_args=default_dag_args,
         catchup=False,
         max_active_runs=1) as dag:

    start_task = EmptyOperator(task_id="start")

    python_task	= PythonOperator(
        task_id="python_task",
        python_callable=dag_hierarchies_module.generate_hier,
        op_kwargs={
            "src_project": "${src_project}",
            "src_dataset": "${src_dataset}",
            "setname": "${setname}",
            "setclass":"${setclass}",
            "orgunit":"${orgunit}",
            "mandt": "${mandt}",
            "table": "${table}",
            "select_key":  "${select_key}",
            "where_clause": ${where_clause},
            "full_table" : "${full_table}"
        })

    stop_task = EmptyOperator(task_id="stop")

    start_task >> python_task >> stop_task

 # type: ignore