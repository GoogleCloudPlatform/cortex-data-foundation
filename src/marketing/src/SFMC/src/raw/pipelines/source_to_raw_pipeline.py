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
"""Beam Pipeline to fetch data from GCS bucket to Cortex RAW layer.

This Beam pipeline loads data for Cortex Marketing workload from GCS bucket.
The result is loaded to the target BigQuery table.
"""

from datetime import datetime
from datetime import timezone
import logging
from typing import Optional

import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import bigquery
from helpers.pipeline_utils import create_mapping_by_schema_definition
from helpers.pipeline_utils import get_max_recordstamp
from helpers.pipeline_utils import is_file_schema_valid
from helpers.pipeline_utils import read_csv_by_rows
from helpers.pipeline_utils import transform_source_data

# Is used as the default value in case of empty table.
_EPOCH_BEGINNING_TIMESTAMP: float = 0.0


class SFMCRawLayerOptions(PipelineOptions):
    """Pipeline argument parser."""

    @classmethod
    def _add_argparse_args(cls, parser) -> None:
        parser.add_argument("--input_file_path_pattern",
                            required=True,
                            type=str,
                            help="Path pattern of the input file to process.")
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


def run_pipeline():
    args = PipelineOptions().view_as(SFMCRawLayerOptions)

    logger = logging.getLogger(__name__)
    level = getattr(logging, args.pipeline_logging_level)
    logger.setLevel(level)

    logger.info("Pipeline started...")
    logger.info("Args: %s", args)

    # Gathering arguments.
    target_project = args.tgt_project
    target_dataset = args.tgt_dataset
    target_table = args.tgt_table
    mapping_file = args.mapping_file

    # Setting correct mappings according to schema file.
    renamed_mapping = create_mapping_by_schema_definition(
        mapping_file=mapping_file)

    # Getting current timestamp.
    current_timestamp = datetime.now(tz=timezone.utc).timestamp()

    bq_client = bigquery.Client(project=target_project)
    max_recordstamp: Optional[float] = get_max_recordstamp(
        bq_client, target_project, target_dataset, target_table)

    latest_recordstamp: float = max_recordstamp or _EPOCH_BEGINNING_TIMESTAMP

    # yapf: disable
    # pylint: disable = no-value-for-parameter, unsupported-binary-operation
    # Beam pipeline.
    with beam.Pipeline(options=args) as pipeline:
        raw_files = (
            pipeline
            | fileio.MatchFiles(args.input_file_path_pattern)
            | "Latest files" >>
                beam.Filter(
                    lambda file, timestamp:
                        file.last_updated_in_seconds > timestamp,
                    timestamp=latest_recordstamp)
            | "Filter valid files" >>
                beam.Filter(is_file_schema_valid, mapping=renamed_mapping)
            | beam.Reshuffle())

        processed_raw_data = (
            raw_files
            | "Read files by line" >> beam.FlatMap(read_csv_by_rows)
            | "Transform fields" >> beam.Map(transform_source_data,
                                             mappings=renamed_mapping,
                                             timestamp=current_timestamp))

        _ = (
            processed_raw_data
            | beam.io.WriteToBigQuery(
                project=target_project,
                dataset=target_dataset,
                table=target_table,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER))


if "__main__" == __name__:
    run_pipeline()
