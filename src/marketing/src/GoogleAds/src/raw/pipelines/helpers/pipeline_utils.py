# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Utils functions for pipeline for GoogleAds."""

import ast
import csv
from dataclasses import dataclass
from datetime import date
from datetime import datetime
import json
import logging
from typing import Any, Callable, Dict, Iterable, List, NamedTuple, Optional

from dateutil import parser
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from google.cloud import secretmanager
from google.cloud.bigquery import Client
from google.protobuf import json_format

_RECORDSTAMP_COLUMN = "recordstamp"
_SOURCE_FIELD = "SourceField"
_TARGET_FIELD = "TargetField"
_RESPONSE_FIELD = "ResponseField"
_BQ_DATATYPE_FIELD = "DataType"

_SERVICE_COLUMNS: Dict = {
    _RESPONSE_FIELD: _RECORDSTAMP_COLUMN,
    _TARGET_FIELD: _RECORDSTAMP_COLUMN,
    _BQ_DATATYPE_FIELD: "TIMESTAMP"
}


class _Customer(NamedTuple):
    customer_id: str
    login_customer_id: str


def _cast_to_date(date_string: str) -> date:
    return parser.parse(date_string).date()


# Type conversion definition between python nad BQ
_BQ_PYTHON_TYPES_MAPPING: Dict[str, Callable] = {
    "INT64": int,
    "FLOAT64": float,
    "STRING": str,
    "BOOL": bool,
    "DATE": _cast_to_date,
    "TIMESTAMP": float
}


class LackBigQueryToPythonTypeMappingError(Exception):
    """Column mapping class.
    Used when there is no connection between BQ and Python type.
    """


@dataclass
class _ColumnMapping:
    """Used as container for mapping"""
    original_name: str
    target_name: str
    bq_type: str
    python_type: type

    def cast_according_to_bq_type(self, value: Any) -> Any:
        if value == "":
            return None
        try:
            return self.python_type(value)
        except (TypeError, ValueError):
            logging.warning(
                "A value '%s' of the type %s "
                "cannot be cast to %s. Replaced with None", value, type(value),
                self.python_type)
            return None


def _list_accessible_client_ids(credentials: Dict,
                                api_version: str) -> Iterable[str]:
    """List all top level customer ids that are accessible
    to the current credentials.

    Args:
        credentials (Dict): Contains client_token, client_secret, refresh_token,
                      client_id.
        api_version (str): Google Ads API version (for example: 'v15').
    Yield:
        List of accessible customer_ids.
    """

    googleads_client = GoogleAdsClient.load_from_dict(credentials,
                                                      version=api_version)

    customer_service = googleads_client.get_service("CustomerService")
    customer_resource_names = (
        customer_service.list_accessible_customers().resource_names)

    for customer_resource_name in customer_resource_names:
        customer_id = customer_service.parse_customer_path(
            customer_resource_name)["customer_id"]
        yield customer_id


def _get_child_customer_ids(credentials: Dict, api_version: str,
                            login_customer_id: str,
                            is_metric_table: bool) -> Iterable[str]:
    """Get details for the current customer and any managed customers
    (if current account is a Manager account).

    Args:
        credentials (Dict): Contains client_token, client_secret, refresh_token,
                      client_id.
        api_version (str): Google Ads API version (for example: 'v15').
        login_customer_id (str): Id of the processing Google Ads account.
        is_metric_table (bool): Type of data which be retrieved.
    Yields:
        customer_ids for auto discovered accounts.
    """

    credentials["login_customer_id"] = login_customer_id

    googleads_client = GoogleAdsClient.load_from_dict(credentials,
                                                      version=api_version)
    customer_service = googleads_client.get_service("GoogleAdsService")
    query = ("SELECT "
             "    customer_client.id, "
             "    customer_client.status, "
             "    customer_client.manager "
             " FROM customer_client "
             " WHERE customer_client.status IN ('ENABLED')"
             "   AND customer_client.test_account = FALSE")

    try:
        stream = customer_service.search_stream(customer_id=login_customer_id,
                                                query=query)

    # The above call fails if the customer is Cancelled, even when the filter
    # is applied. So we need to catch this error explicitly.
    except GoogleAdsException as ex:
        customer_not_enabled = googleads_client.get_type(
            "AuthorizationErrorEnum").AuthorizationError.CUSTOMER_NOT_ENABLED

        for googleads_error in ex.failure.errors:
            auth_error_code = googleads_error.error_code.authorization_error
            if auth_error_code == customer_not_enabled:
                logging.warning(
                    "Customer %s is not enabled or has been deactivated. "
                    "Skipping it.", login_customer_id)
                return []

        raise ex

    for batch in stream:
        for result_row in batch.results:
            customer = result_row.customer_client
            # Manager accounts can not get metrics data. Excluding them.
            if customer.manager and is_metric_table:
                continue
            yield str(customer.id)


def get_available_client_ids(credentials: Dict, api_version: str,
                             is_metric_table: bool) -> List[_Customer]:
    """Getting list of active accounts for the given client.

    Args:
        credentials (Dict): Contains client_token, client_secret, refresh_token,
                      client_id.
        api_version (str): Google Ads API version (for example: 'v13').
        is_metric_table (bool): Type of data which be retrieved.
    Returns:
        List of pairs of customer_ids and login_customer_ids.
    """

    accessible_customer_ids = []

    # Set for the deduplication.
    discovered_customer_ids = set()

    login_customer_ids = _list_accessible_client_ids(credentials, api_version)
    for login_customer_id in login_customer_ids:
        customer_ids = _get_child_customer_ids(credentials, api_version,
                                               login_customer_id,
                                               is_metric_table)
        for customer_id in customer_ids:
            if customer_id not in discovered_customer_ids:
                discovered_customer_ids.add(customer_id)
                accessible_customer_ids.append(
                    _Customer(customer_id, login_customer_id))

    return accessible_customer_ids


def generate_query(column_names: List[str], api_name: str,
                   start_window: Optional[datetime],
                   end_window: Optional[datetime]) -> str:
    """Generating query for API request.

    For the metrics we have to define filter (WHERE clause)
    We define date window period to filter out the data.
    """
    query = f"SELECT {', '.join(column_names)} FROM {api_name} "
    if start_window and end_window:
        start_day = start_window.strftime("%Y-%m-%d")
        end_day = end_window.strftime("%Y-%m-%d")
        query += ("WHERE segments.date "
                  f"BETWEEN '{start_day}' AND '{end_day}'")
    return query


def _add_service_columns(row: Dict, timestamp: float) -> Dict[str, Any]:
    """Additional service columns for BQ."""
    service_columns = {
        _RECORDSTAMP_COLUMN: timestamp,
    }
    return {**row, **service_columns}


def change_dict_key_names(row: Dict, mapping: Dict):
    """Takes a row as dict and creates a new dict with the key names
    according to the mapping provided.

    Args:
        row (Dict): data row from the API response.
        mapping (Dict): column mapings with original and target names.
    """
    dict_with_renamed_keys = {}
    for original_key, value in row.items():
        column_mapping = mapping.get(original_key)
        if not column_mapping:
            continue
        dict_with_renamed_keys[
            column_mapping.
            target_name] = column_mapping.cast_according_to_bq_type(value)
    return dict_with_renamed_keys


def create_bigquery_schema_from_mapping(
        mapping: Dict) -> Dict[str, List[Dict[str, str]]]:
    """Generate BQ schema from mapping object.

    Returns:
        BigQuery schema.
    """
    column_mode = "NULLABLE"

    fields = [{
        "name": column_mapping.target_name,
        "type": column_mapping.bq_type,
        "mode": column_mode
    } for column_mapping in mapping.values()]
    return {"fields": fields}


def get_select_columns(path_to_mapping: str) -> List:
    """Getting list of columns for the API query from csv file.

    Args:
        path_to_mapping (str): Path to mapping file

    Returns:
        Unique list of columns.
    """
    fields = []
    with open(path_to_mapping, encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f, delimiter=","):
            fields.append(row[_SOURCE_FIELD])

    # For some fields the requested column is the same,
    # we only need unique list of columns
    unique_fields = (list(set(fields)))
    return unique_fields


def _extend_mapping_with_service_columns(mapping: Dict) -> Dict:
    """Extends the current column mapping with service columns."""
    row = _SERVICE_COLUMNS
    new_mapping = dict(mapping)
    new_mapping[row[_TARGET_FIELD]] = _create_column_mapping(row)
    return new_mapping


def create_column_mapping(path_to_mapping: str) -> Dict:
    """Loads CSV mapping file and converts it to a dictionary."""
    raw_mapping = []
    with open(path_to_mapping, encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f, delimiter=","):
            raw_mapping.append(row)

    map_dict = {}
    for row in raw_mapping:
        target_field = row[_RESPONSE_FIELD]
        if len(row[_TARGET_FIELD].split(".")) > 1:
            # nested field are STRING in RAW layer
            row[_BQ_DATATYPE_FIELD] = "STRING"
        map_dict[target_field] = _create_column_mapping(row)

    return _extend_mapping_with_service_columns(map_dict)


def _create_column_mapping(row: Dict[str, str]) -> _ColumnMapping:
    """Reorganizing column mapping to ColumnMapping class."""

    # Ads API does not return DATETIME values. All dates stored as STRING.
    # All DATETIME mapping columns converted to STRING for raw layer.
    if row[_BQ_DATATYPE_FIELD] in ["DATETIME", "DATE"]:
        bq_type = "STRING"
    else:
        bq_type = row[_BQ_DATATYPE_FIELD]
    return _ColumnMapping(
        original_name=row[_RESPONSE_FIELD],
        target_name=row[_TARGET_FIELD].split(".")[0].replace("[]", ""),
        bq_type=bq_type,
        python_type=_transform_bq_to_python_type(bq_type),
    )


def _transform_bq_to_python_type(bq_type: str) -> Any:
    """Returns a python type according to the input string.

    Raises:
      LackBigQueryToPythonTypeMappingError: If there is no such pair for
      the provided input string
    """
    try:
        return _BQ_PYTHON_TYPES_MAPPING[bq_type]
    except KeyError as err:
        raise LackBigQueryToPythonTypeMappingError(
            f"There is no mapping for {bq_type}. "
            "Update of BQ_PYTHON_TYPES_MAPPING const is needed.") from err


def get_max_recordstamp(client: Client, project: str, dataset: str,
                        table: str) -> Optional[float]:
    """Gets the highest timestamp value for the given table."""

    query_job = client.query(
        "SELECT UNIX_SECONDS(MAX(recordstamp)) AS max_recordstamp "
        f"FROM `{project}.{dataset}.{table}`")
    results = query_job.result()
    if not results.total_rows:
        return None
    return next(results).max_recordstamp


def get_credentials_from_secret_manager(
        project_id: str,
        secret_name: str = "cortex-framework-google-ads-yaml") -> Dict:
    """Getting credentials from Google Secret Manager."""
    name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    secret_client = secretmanager.SecretManagerServiceClient()
    response = secret_client.access_secret_version(name=name)
    payload = response.payload.data.decode()
    credentials = ast.literal_eval(payload)
    return credentials


def get_data_for_customer(customer: _Customer, credentials: Dict,
                          api_version: str, query: str, timestamp: float,
                          api_name: str, is_metric_table: bool):
    """Send request and gets data from the Google Ads API.

    Args:
        customer (NamedTuple): For which customers we getting data. Consist of
                     customer id and corresponding login_customer_id.
        credentials (Dict): access token and secret for Google Ads API.
        api_version (str): API version (e.g. 'v13').
        query (str): query to request the data.
        timestamp (float): Load date and time.
        api_name (str): api endpoint name.
        is_metric_table (bool): Is requested data is metrics or not.
        Depends on it we read the response in different way.

    Yields:
        Customers requested data formatted as dictionary.
    """

    credentials["login_customer_id"] = customer.login_customer_id

    ads_client = GoogleAdsClient.load_from_dict(credentials,
                                                version=api_version)
    ads_service = ads_client.get_service("GoogleAdsService")
    stream = ads_service.search_stream(customer_id=customer.customer_id,
                                       query=query)
    for batch in stream:
        for row in batch.results:
            json_str = json_format.MessageToJson(
                message=row, preserving_proto_field_name=True)
            if is_metric_table:
                row_data = json.loads(json_str)
            else:
                row_data = json.loads(json_str)[api_name]
            extended_row_data = _add_service_columns(row_data, timestamp)
            yield extended_row_data
