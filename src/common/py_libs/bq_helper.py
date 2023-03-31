# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Library for BigQuery related functions."""

# TODO: Reconcile SFDC common_lib code with this.

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


def execute_sql_file(bq_client: bigquery.Client, sql_file: str) -> None:
    """Executes a Bigquery sql file."""
    with open(sql_file, mode="r", encoding="utf-8") as sqlf:
        query_job = bq_client.query(sqlf.read())
        # Let's wait for query to complete.
        _ = query_job.result()


def table_exists(bq_client: bigquery.Client, full_table_name: str) -> bool:
    """Checks if a given table exists in BigQuery."""
    try:
        bq_client.get_table(full_table_name)
        return True
    except NotFound:
        return False
