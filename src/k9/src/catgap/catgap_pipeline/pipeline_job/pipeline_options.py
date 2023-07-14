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
"""CATGAP Pipeline Options"""

from apache_beam.options.pipeline_options import PipelineOptions


class CatgapOptions(PipelineOptions):
    """CATGAP-specific Beam pipeline options"""

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
            "--source_project",
            help="Data Foundation GCP Project",
            type=str,
            required=True,
        )
        parser.add_value_provider_argument(
            "--ads_project",
            help="GCP Project with Ads Data",
            type=str,
            required=True,
        )
        parser.add_value_provider_argument(
            "--bigquery_location",
            help="BigQuery location.",
            type=str,
            required=True,
        )
        parser.add_value_provider_argument(
            "--staging_dataset",
            help=(
                "BigQuery dataset for storing intermediate temporary tables."),
            type=str,
            required=True,
        )
        parser.add_value_provider_argument(
            "--k9_processing_dataset",
            help="K9 Processing BQ dataset in the source project",
            type=str,
            required=True,
        )
        parser.add_value_provider_argument(
            "--ads_dataset",
            help="Google Ads BQ dataset in the Ads project",
            type=str,
            required=True,
        )
        parser.add_value_provider_argument(
            "--ads_customer_id",
            help="Google Ads Customer Id",
            type=str,
            required=True,
        )
        parser.add_value_provider_argument(
            "--mapping_spreadsheet",
            help="URL of Ads-to-SAP mapping spreadsheet",
            type=str,
            required=True,
        )
