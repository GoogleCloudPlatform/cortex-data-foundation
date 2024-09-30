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
"""Beam Pipeline to load data from source bucket to raw layer.

Data loading pipeline for GoogleAds source data. It uses GoogleAds API to
read the data. Result is landed in the BigQuery table.
"""

from datetime import datetime
from datetime import timedelta
from datetime import timezone
import logging
from typing import Optional

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import bigquery
from helpers.pipeline_utils import change_dict_key_names
from helpers.pipeline_utils import create_bigquery_schema_from_mapping
from helpers.pipeline_utils import create_column_mapping
from helpers.pipeline_utils import generate_query
from helpers.pipeline_utils import get_available_client_ids
from helpers.pipeline_utils import get_credentials_from_secret_manager
from helpers.pipeline_utils import get_data_for_customer
from helpers.pipeline_utils import get_max_recordstamp
from helpers.pipeline_utils import get_select_columns


class GoogleAdsRawLayerOptions(PipelineOptions):
    """Arguments for raw extraction pipeline."""

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument("--api_version",
                            required=True,
                            help="Google Ads API version")
        parser.add_argument("--api_name",
                            required=True,
                            help="API resource endpoint name ")
        parser.add_argument("--tgt_project",
                            required=True,
                            help="Google Cloud Project ID")
        parser.add_argument("--tgt_dataset",
                            required=True,
                            help="Target BQ dataset to create a table")
        parser.add_argument("--tgt_table",
                            required=True,
                            help="Target BQ table to write data")
        parser.add_argument("--mapping_file",
                            required=True,
                            help="Mapping CSV file absolute local path")
        parser.add_argument(
            "--lookback_days",
            required=True,
            type=int,
            help="Number of past days to fetch data for Metrics table")
        parser.add_argument("--resource_type",
                            required=True,
                            help="Resource type")
        parser.add_argument("--pipeline_logging_level",
                            required=True,
                            type=str,
                            help="Logging level of pipeline.")


args = PipelineOptions().view_as(GoogleAdsRawLayerOptions)

logger = logging.getLogger(__name__)
level = getattr(logging, args.pipeline_logging_level)
logger.setLevel(level)

# Reading column mappings for data load
columns_mapping = create_column_mapping(args.mapping_file)

# Creating BQ schema based on mappings
table_schema = create_bigquery_schema_from_mapping(columns_mapping)

bq_client = bigquery.Client()
target_project = args.tgt_project
target_dataset = args.tgt_dataset
target_table = args.tgt_table
resource_type = args.resource_type
current_datetime = datetime.now(tz=timezone.utc)
max_recordstamp: Optional[float] = get_max_recordstamp(bq_client,
                                                       target_project,
                                                       target_dataset,
                                                       target_table)

# Reading Google Ads API credentials from Google Secret Manager
credentials: dict = get_credentials_from_secret_manager(args.tgt_project)

# Metric table flag. Depends on it we can change the behaviour of processing.
is_metric_table = resource_type == "metric"

# Getting list of columns for the API query
select_columns = get_select_columns(args.mapping_file)

# Generating query
# For the metric table we have to define date filter and apply it to the query
if is_metric_table:
    if max_recordstamp:  # if table is not empty
        start_window = current_datetime - timedelta(days=args.lookback_days)
    else:
        start_window = datetime(2000, 10, 23)  # oldest possible start point
    end_window = current_datetime
else:
    start_window = None
    end_window = None
query = generate_query(select_columns,
                       args.api_name,
                       start_window=start_window,
                       end_window=end_window)

# yapf: disable
with beam.Pipeline(options=args) as pipeline:
    clients_to_process = get_available_client_ids(credentials, args.api_version,
                                                  is_metric_table)

    if not clients_to_process:
        raise ValueError("No accessible customer ids found with the current "
                         "credentials. Aborting.")

    # Avoiding load constant data several times.
    if resource_type == "constant":
        clients_to_process = [clients_to_process[0]]

    raw_rows = (
        pipeline
        | beam.Create(clients_to_process)
        | "Process customer" >> beam.FlatMap(
            get_data_for_customer,
            credentials=credentials,
            api_version=args.api_version,
            query=query,
            timestamp=current_datetime.timestamp(),
            api_name=args.api_name,
            is_metric_table=is_metric_table)
        | "Convert column names" >> beam.Map(
            change_dict_key_names, mapping=columns_mapping))

    output = (
        raw_rows
        | beam.io.WriteToBigQuery(
                project=target_project,
                dataset=target_dataset,
                table=target_table,
                schema=table_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER))
