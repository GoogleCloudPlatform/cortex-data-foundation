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
""" Salesforce to BigQuery module init.  """

import os
import sys
import typing

from google.cloud import bigquery
from simple_salesforce import Salesforce

sys.path.append(os.path.dirname(os.path.realpath(__file__)))

from salesforce_to_bigquery import SalesforceToBigquery  # pylint:disable=wrong-import-position


def sfdc2bq_replicate(
        simple_sf_connection: Salesforce,
        api_name: str,
        bq_client: bigquery.Client,
        project_id: str,
        dataset_name: str,
        output_table_name: str,
        text_encoding: str,
        include_non_standard_fields: typing.Union[bool,
                                                  typing.Iterable[str]] = False,
        exclude_standard_fields: typing.Iterable[str] = None) -> None:
    """Method to extract data from Salesforce to BigQuery

    Args:
        sfdc_connection_id (str): Airflow Salesforce connection id
        api_name (str): Salesforce object name to replicate
        bq_connection_id (str): Airflow BigQuery connection name
        project_id (str): destination GCP project id
        dataset_name (str): destination dataset name
        output_table_name (str): destination table
        include_non_standard_fields (bool, Iterable[str]|): whether to replicate
            non-standard fields, True/False or a list of names
        exclude_standard_fields (Iterable[str]): list of standard fields
            to exclude from replication
    """

    SalesforceToBigquery.replicate(
        simple_sf_connection=simple_sf_connection,
        api_name=api_name,
        bq_client=bq_client,
        project_id=project_id,
        dataset_name=dataset_name,
        output_table_name=output_table_name,
        text_encoding=text_encoding,
        include_non_standard_fields=include_non_standard_fields,
        exclude_standard_fields=exclude_standard_fields)
