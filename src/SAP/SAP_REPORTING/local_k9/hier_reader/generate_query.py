# Copyright 2023 Google LLC
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
"""Useful functions to carry out necessary operations."""

#TODO: Remove all pylint disabled flags.
#TODO: Arrange functions in more logical order.

from string import Template

from google.cloud.exceptions import NotFound
from google.cloud import bigquery
from google.cloud import storage

from common.py_libs import cortex_bq_client

_HIER_DAG_PYTHON_TEMPLATE = ('local_k9/hier_reader/template_dag'
                             '/dag_sql_hierarchies.py')

_GENERATED_DAG_DIR = 'generated_dag'

client = cortex_bq_client.CortexBQClient()
storage_client = storage.Client()


def generate_dag_py_file(template, file_name, **dag_subs):
    """Generates DAG definition python file from template."""
    with open(template, mode='r', encoding='utf-8') as dag_template_file:
        dag_template = Template(dag_template_file.read())
    generated_dag_code = dag_template.substitute(**dag_subs)

    dag_file = f'{_GENERATED_DAG_DIR}/{file_name}'
    with open(dag_file, mode='w+', encoding='utf-8') as generated_dag_file:
        generated_dag_file.write(generated_dag_code)
        generated_dag_file.close()
        print(f'Created DAG python file {dag_file}')


def generate_hier_dag_files(file_name, **dag_subs):
    generate_dag_py_file(_HIER_DAG_PYTHON_TEMPLATE, file_name, **dag_subs)


def check_create_hiertable(full_table, field):
    try:
        client.get_table(full_table)
    except NotFound:
        schema = [
            bigquery.SchemaField('mandt', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('parent', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('parent_org', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('child', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('child_org', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField(field, 'STRING', mode='NULLABLE')
        ]

        table = bigquery.Table(full_table, schema=schema)
        table = client.create_table(table)
        print(f'Created {full_table}')

def copy_to_storage(gcs_bucket, prefix, directory, filename):
    try:
        bucket = storage_client.get_bucket(gcs_bucket)
    except Exception as e:  #pylint:disable=broad-except
        print(f'Error when accessing GCS bucket: {gcs_bucket}')
        print(f'Error : {str(e)}')
    blob = bucket.blob(f'{prefix}/{filename}')
    blob.upload_from_filename(f'{directory}/{filename}')
