# Copyright 2023 Google LLC

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Beam Pipeline to fetch data from TikTok Business API to Cortex RAW layer.

This dataflow pipeline loads data for Cortex Marketing workload from Tiktok.
It calls TikTok reporting API.
Data time range is based on current day and sampling period (days).
Result is loaded in the provided Bigquery table.
"""

from datetime import date
from datetime import datetime
from datetime import timezone
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import bigquery
from helpers.pipeline_steps import process_api_data
from helpers.pipeline_utils import calculate_query_time_range
from helpers.pipeline_utils import create_bq_schema
from helpers.pipeline_utils import create_column_mapping
from helpers.pipeline_utils import get_latest_load_date
from helpers.pipeline_utils import get_secret
from helpers.pipeline_utils import REPORT_QUERIES
from helpers.tiktok_api import TikTokClient


class TikTokRawLayerOptions(PipelineOptions):
    """TikTok pipeline arguments."""

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument("--mapping_file",
                            required=True,
                            type=str,
                            help="Mapping CSV file absolute local path.")
        parser.add_argument("--tgt_project",
                            required=True,
                            type=str,
                            help="Project ID of target BQ dataset.")
        parser.add_argument("--tgt_dataset",
                            required=True,
                            type=str,
                            help="Target BQ dataset name.")
        parser.add_argument("--tgt_table",
                            required=True,
                            type=str,
                            help="Target BQ table name.")
        parser.add_argument("--pipeline_logging_level",
                            required=True,
                            type=str,
                            help="Logging level of pipeline.")


def run():
    args = PipelineOptions().view_as(TikTokRawLayerOptions)

    target_project = args.tgt_project
    target_dataset = args.tgt_dataset
    target_table = args.tgt_table

    mapping_file = args.mapping_file

    logger = logging.getLogger(__name__)
    level = getattr(logging, args.pipeline_logging_level)
    logger.setLevel(level)

    now = datetime.now(tz=timezone.utc).timestamp()
    today = date.today()

    # Get secrets for the pipeline.
    app_id = get_secret(project_id=target_project,
                        secret_name="cortex_tiktok_app_id")
    access_token = get_secret(project_id=target_project,
                              secret_name="cortex_tiktok_access_token")
    secret = get_secret(project_id=target_project,
                        secret_name="cortex_tiktok_app_secret")

    tiktok_client = TikTokClient(access_token=access_token,
                                 app_id=app_id,
                                 secret_key=secret)

    bq_client = bigquery.Client(project=target_project)

    # Setup report time period.
    latest_load_date = get_latest_load_date(client=bq_client,
                                            project=target_project,
                                            dataset=target_dataset,
                                            table=target_table)

    report_time_range = calculate_query_time_range(
        latest_load_date=latest_load_date, today=today)

    report_query = {**REPORT_QUERIES[target_table], **report_time_range}

    logger.info("Report query: %s", report_query)

    # BigQuery schema creation.
    bq_schema = create_bq_schema(mapping_file=mapping_file)

    logger.debug("BigQuery schema: %s", bq_schema)

    column_mapping = create_column_mapping(path_to_mapping=mapping_file)

    # Get authorized advertisers.
    advertisers = tiktok_client.get_advertisers()

    # yapf: disable
    pipeline = beam.Pipeline(options=args)

    advertisers_info = (
                        pipeline
                        | beam.Create(
                            tiktok_client.get_advertisers_info(
                                advertisers=advertisers))
    )

    # Process report results.
    raw_data = advertisers_info | process_api_data(
                                        getter=tiktok_client.get_report,
                                        query=report_query,
                                        timestamp=now,
                                        column_mapping=column_mapping)

    # Output
    _ = (
        raw_data
        | beam.io.WriteToBigQuery(
                project=target_project,
                dataset=target_dataset,
                table=target_table,
                schema=bq_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER)
    )

    pipeline.run() # Asynchronous run enabled.

# yapf: enable
if __name__ == "__main__":
    run()
