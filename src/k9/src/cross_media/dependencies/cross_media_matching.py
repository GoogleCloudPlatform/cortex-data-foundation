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
Cross-Media Campaign to Product Category matching pipeline runner.
"""

import copy
import datetime
from pathlib import Path
import time
import typing

from google.cloud import bigquery
import vertexai
from vertexai.batch_prediction import BatchPredictionJob

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook


def _copy_table(bq_client: bigquery.Client,
                source_table: str,
                destination_table: str,
                job_config: bigquery.CopyJobConfig):
    src_table = bq_client.get_table(source_table)
    while src_table.streaming_buffer:
        # Waiting for the streaming buffer to close.
        # It is necessary for data consistency before copying the table.
        time.sleep(30)
        src_table = bq_client.get_table(source_table)
    copy_job = bq_client.copy_table(src_table,
                                    destination_table,
                                    project=src_table.project,
                                    location=src_table.location, # type: ignore
                                    job_config=job_config)
    copy_job.result()


def _run_matching(source_project: str,
                  target_project: str,
                  bq_location: str,
                  vertexai_region: str,
                  k9_processing_dataset: str,
                  k9_reporting_dataset: str,
                  vertexai_processing_dataset: str,
                  prepare_data_query: str,
                  postprocess_results_query: str,
                  gemini_model_name: str,
                  is_full_refresh: bool,
                  connection_id: str,
                  job_labels: typing.Optional[
                                typing.Dict[str, typing.Any]] = None):
    """Runs Cross-Media matching data prep, job, and post-processing.

    Args:
        source_project (str): Cortex Data Foundation source project.
        target_project (str): Cortex Data Foundation target project.
        bq_location (str): Cortex Data Foundation BigQuery location.
        vertexai_region (str): Vertex AI region.
        k9_processing_dataset (str): Cortex Data Foundation K9 processing
                                     dataset name.
        k9_reporting_dataset (str): Cortex Data Foundation K9 reporting
                                    dataset name.
        vertexai_processing_dataset (str): Cortex Data Foundation Vertex AI
                                           processing dataset name.
        prepare_data_query (str): SQL code for preparing input data.
        postprocess_results_query (str): SQL code for post-processing
                                         Gemini batch text generation results.
        gemini_model_name (str): Gemini model name for batch text generation.
        is_full_refresh (bool): True if it's a full re-match run.
        connection_id (str): Airflow Connection Id for BigQuery.
        job_labels (typing.Optional[ typing.Dict[str, typing.Any]], optional):
                    BigQuery job labels. Defaults to None.

    Raises:
        RuntimeError: raised if Vertex AI batch text generation job fails.
    """

    print(("🦄 Starting Cross-Media Campaign to "
          "Product Category matching pipeline. 🦄"))
    bq_client = BigQueryHook(connection_id).get_client(
                                                    project_id=source_project,
                                                    location=bq_location)

    execution_timestamp = datetime.datetime.now(
                    datetime.timezone.utc).strftime("%Y-%m-%d-%H-%M-%S-%f")
    temp_input_table_name = f"TMP_XMedia_input_{execution_timestamp}"
    temp_output_table_name = f"TMP_XMedia_output_{execution_timestamp}"
    temp_input_table_fullname = (f"{source_project}.{k9_processing_dataset}."
                            f"{temp_input_table_name}")
    batch_input_table_fullname = (f"{source_project}."
                                  f"{vertexai_processing_dataset}."
                                  f"{temp_input_table_name}")
    temp_output_table_fullname = (f"{source_project}.{k9_processing_dataset}."
                             f"{temp_output_table_name}")

    # Create temporary input table
    print("Creating campaign matching temporary input table: "
          f"`{temp_input_table_fullname}`.")
    input_table = bigquery.Table(temp_input_table_fullname, schema=[
        bigquery.SchemaField(name="batch_campaigns", field_type="STRING"),
        bigquery.SchemaField(name="request", field_type="JSON")
    ])
    input_table = bq_client.create_table(input_table)

    # Prepare job config
    default_query_job_config = (copy.deepcopy(
                                    bq_client.default_query_job_config)
                                or bigquery.QueryJobConfig())
    if job_labels:
        default_query_job_config.labels = job_labels
    default_query_job_config.priority = "BATCH"
    copy_job_config = bigquery.CopyJobConfig()
    if job_labels:
        copy_job_config.labels = job_labels

    # Run input query
    print("Preparing input data.")
    prepare_job_config = copy.deepcopy(default_query_job_config)
    prepare_job_config.query_parameters = [
        bigquery.ScalarQueryParameter(
                                      "batch_input_table",
                                      "STRING",
                                      temp_input_table_fullname),
        bigquery.ScalarQueryParameter(
                                      "is_full_refresh",
                                      "BOOL",
                                      is_full_refresh)
    ]
    bq_client.query(prepare_data_query, job_config=prepare_job_config).result()
    input_table = bq_client.get_table(input_table)
    if input_table.num_rows == 0:
        print("No new campaigns to match! Deleting the temporary input table  "
              "and exiting.")
        bq_client.delete_table(input_table)
        return

    # Copy input data to Vertex AI dataset
    print("Copying input data to Vertex AI Processing dataset, table "
          f"`{batch_input_table_fullname}`.")
    _copy_table(bq_client,
                temp_input_table_fullname,
                batch_input_table_fullname,
                job_config=copy_job_config)
    # Delete input table
    bq_client.delete_table(temp_input_table_fullname)

    # Run Vertex AI batch text generation using Gemini
    print("Running Vertex AI batch text generation job using "
          f"model `{gemini_model_name}`.")
    vertexai.init(project=source_project, location=vertexai_region)
    job = BatchPredictionJob.submit(
        source_model=gemini_model_name,
        input_dataset=f"bq://{batch_input_table_fullname}",
        output_uri_prefix=f"bq://{source_project}.{vertexai_processing_dataset}"
    )

    while not job.has_ended:
        time.sleep(30)
        job.refresh()

    if job.has_succeeded:
        job_output_table = job.output_location.replace("bq://", "")
        print(f"Job succeeded! Data in {job_output_table}")
        # Copy batch job output to K9 processing dataset
        print(f"Copying job output to `{temp_output_table_fullname}`.")
        _copy_table(bq_client,
                    job_output_table,
                    temp_output_table_fullname,
                    job_config=copy_job_config)
        # Uncomment next 2 lines for cleaning up job and its direct output.
        # job.delete()
        # bq_client.delete_table(job_output_table)

        # Run postprocessing query
        postprocess_job_config = copy.deepcopy(default_query_job_config)
        postprocess_job_config.query_parameters = [
            bigquery.ScalarQueryParameter(
                                        "batch_output_table",
                                        "STRING",
                                        temp_output_table_fullname),
            bigquery.ScalarQueryParameter(
                                        "is_full_refresh",
                                        "BOOL",
                                        is_full_refresh),
        ]
        print("Post-processing data and writing results to "
            f"{target_project}.{k9_reporting_dataset}.campaign_product_mapping")
        bq_client.query(postprocess_results_query,
                        job_config=postprocess_job_config).result()
        bq_client.delete_table(temp_output_table_fullname)
        print(("✅ Cross-Media Campaign to Product Category matching pipeline "
              "completed successfully."))
    else:
        error_message = ("🛑 Vertex AI batch text generation job failed! "
                        f"\n{job.error}")
        raise RuntimeError(error_message)



def run_cross_media_matching(is_full_refresh: bool,
                             job_labels: typing.Dict[str, typing.Any],
                             connection_id: str):
    """Executes Cross-Media Campaign to Product Category matching pipeline.

    Args:
        is_full_refresh (bool): True if it's a full refresh run.
        job_labels (typing.Dict[str, typing.Any]): BigQuery job labels.
        connection_id (str): Airflow connection id.
    """

    gemini_model_name = "{{ k9_cross_media_text_generation_model }}"
    bq_location = "{{ location }}"
    vertexai_region = "{{ vertexai_region }}"
    source_project = "{{ project_id_src }}"
    target_project = "{{ project_id_tgt }}"
    k9_processing = "{{ k9_datasets_processing }}"
    k9_reporting = "{{ k9_datasets_reporting }}"
    vertexai_processing = "{{ vertexai_datasets_processing }}"

    prepare_sql_path = Path(__file__).parent / \
                    "cross_media_prepare_gemini_requests.sql"
    postprocess_sql_path = Path(__file__).parent / \
                    "cross_media_postprocess_results.sql"
    prepare_query = prepare_sql_path.read_text(encoding="utf-8")
    postprocess_query = postprocess_sql_path.read_text(encoding="utf-8")
    _run_matching(
        source_project,
        target_project,
        bq_location,
        vertexai_region,
        k9_processing,
        k9_reporting,
        vertexai_processing,
        prepare_query,
        postprocess_query,
        gemini_model_name,
        is_full_refresh,
        connection_id,
        job_labels)

