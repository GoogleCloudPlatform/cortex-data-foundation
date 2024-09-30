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
"""Beam Pipeline to fetch data from Meta Business API to Cortex RAW layer.
Result is loaded in the provided BigQuery table.
"""

from datetime import datetime
import logging
from typing import Dict, List

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from helpers.meta_client import MetaClient
from helpers.pipeline_utils import add_service_columns
from helpers.pipeline_utils import get_access_token
from helpers.pipeline_utils import get_first_load_date
from helpers.pipeline_utils import process_output_objects
from helpers.pipeline_utils import read_request_fields
from helpers.pipeline_utils import read_field_mapping


class MetaRawLayerOptions(PipelineOptions):
    """Facebook pipeline arguments."""

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument("--request_file",
                            required=True,
                            type=str,
                            help="Request parameters file absolute local path.")
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
        parser.add_argument("--entity_type",
                            required=True,
                            type=str,
                            help="Extracting object type.")
        parser.add_argument("--object_endpoint",
                            required=True,
                            type=str,
                            help="Meta API endpoint.")
        parser.add_argument("--object_id_column",
                            required=False,
                            type=str,
                            help="Id column name of parent object.")
        parser.add_argument("--breakdowns",
                            required=False,
                            type=str,
                            help="Breakdown dimensions overall.")
        parser.add_argument("--action_breakdowns",
                            required=False,
                            type=str,
                            help="Breakdown dimensions for action objects.")
        parser.add_argument("--http_timeout",
                            required=True,
                            type=int,
                            help="HTTP request timeout.")
        parser.add_argument("--batch_size",
                            required=False,
                            type=int,
                            default=7,
                            help="Days per request.")
        parser.add_argument(
            "--next_request_delay_sec",
            required=False,
            type=float,
            default=1.0,
            help="The delay between two Meta API calls in seconds.")
        parser.add_argument("--api_version",
                            required=True,
                            type=str,
                            help="Meta Marketing API version.")
        parser.add_argument(
            "--max_load_lookback_days",
            required=False,
            type=int,
            help="Maximum number of look back days for data loading.")
        parser.add_argument("--pipeline_logging_level",
                            required=True,
                            type=str,
                            help="Logging level of pipeline.")


def run_pipeline():
    args = PipelineOptions().view_as(MetaRawLayerOptions)

    logger = logging.getLogger(__name__)
    level = getattr(logging, args.pipeline_logging_level)
    logger.setLevel(level)

    # Getting API request fields from settings file.
    request_fields = read_request_fields(args.request_file)
    logger.debug("Request fields: %s", request_fields)

    # Creating response and target field pairs.
    field_mapping = read_field_mapping(path_to_mapping=args.mapping_file)
    logger.debug("Field mapping: %s", field_mapping)

    # Getting Meta access token for the pipeline.
    access_token = get_access_token(args.tgt_project,
                                    "cortex_meta_access_token")

    # yapf: disable
    with beam.Pipeline(options=args) as pipeline:

        meta_client = MetaClient(access_token,
                                args.http_timeout,
                                args.next_request_delay_sec,
                                args.api_version)

        if args.entity_type == "adaccount":
            _run_adaccounts_pipeline(pipeline, meta_client, args.tgt_project,
                                     args.tgt_dataset, args.tgt_table,
                                     request_fields, field_mapping)
        elif args.entity_type == "dimension":
            if not args.object_endpoint:
                raise ValueError("--object_endpoint is required "
                                 "for dimension entity type.")

            _run_dimension_pipeline(pipeline, meta_client, args.tgt_project,
                                     args.tgt_dataset, args.tgt_table,
                                     args.object_endpoint, request_fields,
                                     field_mapping)
        elif args.entity_type == "fact":
            if not args.object_endpoint:
                raise ValueError("--object_endpoint is required "
                                 "for fact entity type.")
            if not args.object_id_column:
                raise ValueError("--object_id_column is required "
                                 "for fact entity type.")

            _run_fact_pipeline(pipeline, meta_client, args.tgt_project,
                                args.tgt_dataset, args.tgt_table,
                                args.object_endpoint, args.object_id_column,
                                args.breakdowns, args.action_breakdowns,
                                args.batch_size,
                                args.max_load_lookback_days,
                                request_fields, field_mapping)
        else:
            raise ValueError(f"Unknown entity type: {args.entity_type}")


def _run_adaccounts_pipeline(pipeline: beam.Pipeline, meta_client: MetaClient,
                             project: str, dataset: str, table: str,
                             request_fields: List[str],
                             field_mapping: Dict[str, str]):
    """This dataflow pipeline loads data for Cortex Marketing workload
    from Meta "me/accounts" endpoint. The response already contains
    all target fields specified in the .request file
    """
    return (
        pipeline
        # "me" is special id for the current user node.
        # "adaccounts" is endpoint for requesting account list.
        | "Request all accounts from Meta API" >> beam.Create(list(
            meta_client.request_dim_data("me", "adaccounts", request_fields)))

        | "Rename fields and serialize values" >>
            beam.Map(process_output_objects, field_mapping)

        | "Add service columns" >>
            beam.Map(add_service_columns, datetime.utcnow())

        | beam.io.WriteToBigQuery(
            project=project,
            dataset=dataset,
            table=table,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER)
    )

def _run_dimension_pipeline(pipeline: beam.Pipeline, meta_client: MetaClient,
                             project: str, dataset: str, table: str,
                             endpoint: str, request_fields: List[str],
                             field_mapping: Dict[str, str]):
    """This dataflow pipeline loads data for Cortex Marketing workload
    from dimension endpoints.
    """
    return (
        pipeline
        # "me" is special id for the current user node.
        # "adaccounts" is endpoint for requesting account list.
        | "Request all accounts from Meta API" >>
            beam.Create([ad_account["id"] for ad_account in
                meta_client.request_dim_data("me", "adaccounts", ["id"])])

        | "Request dimension data" >>
            beam.FlatMap(meta_client.request_dim_data, endpoint, request_fields)

        | "Rename fields and serialize values" >>
            beam.Map(process_output_objects, field_mapping)

        | "Add service columns" >>
            beam.Map(add_service_columns, datetime.utcnow())

        | beam.io.WriteToBigQuery(
            project=project,
            dataset=dataset,
            table=table,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER)
    )

def _run_fact_pipeline(pipeline: beam.Pipeline, meta_client: MetaClient,
                        project: str, dataset: str, table: str,
                        endpoint: str, id_column: str, breakdowns: str,
                        action_breakdowns: str, batch_size: int,
                        max_load_lookback_days: int,
                        request_fields: List[str],
                        field_mapping: Dict[str, str]):
    """This dataflow pipeline loads data for Cortex Marketing workload
    from fact endpoints.
    """
    return (
        pipeline
        # "me" is special id for the current user node.
        # "adaccounts" is endpoint for requesting account list.
        | "Request all accounts from Meta API" >>
            beam.Create([ad_account["id"] for ad_account in
                meta_client.request_dim_data("me", "adaccounts", ["id"])])

        | "Request all ad objects for account" >>
            beam.FlatMap(meta_client.request_dim_data, endpoint, ["id"])

        | "Convert ad objects to list of ad object ids" >>
            beam.Map(lambda ad_object: ad_object["id"])

        | "Calculate first load date for each ad object id" >>
            beam.Map(get_first_load_date, project, dataset, table, id_column,
                    max_load_lookback_days)

        | "Request insights for each ad object id" >>
            beam.FlatMap(meta_client.request_fact_data, request_fields,
                        breakdowns, action_breakdowns, batch_size)

        | "Rename fields and serialize values" >>
            beam.Map(process_output_objects, field_mapping)

        | "Add service columns" >>
            beam.Map(add_service_columns, datetime.utcnow())

        | beam.io.WriteToBigQuery(
            project=project,
            dataset=dataset,
            table=table,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER)
        )


if __name__ == "__main__":
    run_pipeline()
