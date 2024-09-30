# Copyright 2023 Google LLC
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
"""Beam Pipeline to load data from source bucket to raw layer.

Data loading pipeline for Marketing data. It processes DataTransfer export CSVs.
Filtering based on latest load timestamp provides incremental load.
Result is landing in the defined BigQuery table.
"""

from datetime import datetime
from datetime import timezone
import logging
from typing import Optional

import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import bigquery
from helpers.pipeline_utils import change_dict_key_names
from helpers.pipeline_utils import create_bigquery_schema_from_mapping
from helpers.pipeline_utils import create_column_mapping
from helpers.pipeline_utils import get_max_recordstamp
from helpers.pipeline_utils import yield_dict_rows_from_compressed_csv

# is used as the default value in case empty table
_EPOCH_BEGINNING_TIMESTAMP: float = 0


class CM360RawLayerOptions(PipelineOptions):
    """Arguments for raw extraction pipeline."""

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument("--input_file_pattern",
                            required=True,
                            type=str,
                            help="Input file pattern to process.")
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


args = PipelineOptions().view_as(CM360RawLayerOptions)

logger = logging.getLogger(__name__)
level = getattr(logging, args.pipeline_logging_level)
logger.setLevel(level)

mapping = create_column_mapping(args.mapping_file)
table_schema = create_bigquery_schema_from_mapping(mapping)

bq_client = bigquery.Client()
target_project = args.tgt_project
target_dataset = args.tgt_dataset
target_table = args.tgt_table
current_timestamp = datetime.now(tz=timezone.utc).timestamp()
max_recordstamp: Optional[float] = get_max_recordstamp(bq_client,
                                                       target_project,
                                                       target_dataset,
                                                       target_table)
latest_recordstamp = max_recordstamp or _EPOCH_BEGINNING_TIMESTAMP

logger.info("The latest recordstamp %s for %s.%s", latest_recordstamp,
             target_dataset, target_table)

# yapf: disable
# pylint: disable = no-value-for-parameter, unsupported-binary-operation
with beam.Pipeline(options=args) as pipeline:
    raw_files = (
        pipeline
        | fileio.MatchFiles(args.input_file_pattern)
        | "Latest files" >>
            beam.Filter(
                lambda object, timestamp:
                    object.last_updated_in_seconds > timestamp,
                timestamp=latest_recordstamp)
        | beam.Reshuffle())

    raw_data = (
        raw_files
        | "Read by line" >> beam.FlatMap(
            yield_dict_rows_from_compressed_csv, timestamp=current_timestamp)
        | "Convert column names" >> beam.Map(
            change_dict_key_names, mapping=mapping))

    output = (
        raw_data
        | beam.io.WriteToBigQuery(
                project=target_project,
                dataset=target_dataset,
                table=target_table,
                schema=table_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER))
