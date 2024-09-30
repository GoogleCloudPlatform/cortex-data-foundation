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
"""Utility functions for Meta pipeline."""

import csv
import yaml
from datetime import date
from datetime import datetime
from datetime import timedelta
import logging
import json
from typing import Any, Dict, List, Union

from google.cloud import secretmanager
from google.cloud.bigquery import Client as BQClient

_RESPONSE_FIELD = "ResponseField"
_TARGET_FIELD = "TargetField"

# Service column names.
_RECORDSTAMP_COLUMN = "recordstamp"
_REPORT_DATE_COLUMN = "report_date"
# As insights are requested with one-day interval,
# date_stop column can be taken as report date.
_DATE_STOP_COLUMN = "date_stop"

_LATEST_LOAD_DATE = date.today() - timedelta(days=1)
_META_UPDATE_WINDOW_DAYS = 28  # Period when insights can be updated by Meta.


def get_access_token(project_id: str, secret_name: str) -> str:
    """Gets credentials from Google Secret Manager."""
    name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    secret_client = secretmanager.SecretManagerServiceClient()
    response = secret_client.access_secret_version(name=name)
    payload = response.payload.data.decode("UTF-8")
    return payload


def process_output_objects(ad_object: Dict[str, Any],
                           field_mapping: Dict[str, str]) -> Dict[str, str]:
    """Renames fields and serializes the field values."""
    result_object = {}
    for field_name, v in ad_object.items():
        # Field name after renaming
        target_field_name = field_mapping[field_name]
        result_object[target_field_name] = json.dumps(v, ensure_ascii=False)
    return result_object


def _parse_dict_to_meta_fields(config_object:Dict[str, Any]) -> List[str]:
    """Converts Python string representation to Meta field representation."""
    fields_list = []
    for key, value in config_object.items():
        if isinstance(value, dict):
            nested_keys = ",".join(_parse_dict_to_meta_fields(value))
            fields_list.append(f"{key}{{{nested_keys}}}")
        else:
            fields_list.append(key)

    return fields_list


def read_request_fields(path_to_settings: str) -> List[str]:
    """Gets list of required fields from request file."""
    with open(path_to_settings, mode="r", encoding="utf-8") as request_file:
        request_config = yaml.load(request_file, Loader=yaml.SafeLoader)
        if request_config:
            request_fields = _parse_dict_to_meta_fields(request_config)
        else:
            raise RuntimeError(f"ERROR: File '{request_file}' is empty. "
                     "Make sure the file exists with correct content.")
        return request_fields


def read_field_mapping(path_to_mapping: str) -> Dict[str, str]:
    """Reads CSV mapping file and returns mapping for renaming fields."""
    # Reading CSV file with table schema.
    with open(path_to_mapping, mode="r", encoding="utf-8", newline="") as f:
        fields_mapping = list(csv.DictReader(f, delimiter=","))

    # Generating field mapping.
    field_mapping = {}
    for mapping_row in fields_mapping:
        response_field = mapping_row[_RESPONSE_FIELD]
        target_field = mapping_row[_TARGET_FIELD]
        field_mapping[response_field] = target_field
    return field_mapping


def add_service_columns(rows: Dict, timestamp) -> Dict[str, Any]:
    """Appends additional service columns for BQ.

    recordstamp - timestamp of record insert.
    report_date - date of requested data.
    """

    # Setting up report_date field based on date_stop field.
    date_stop = rows.get(_DATE_STOP_COLUMN)

    if date_stop:
        date_stop = json.loads(date_stop)
        report_date = datetime.strptime(date_stop, "%Y-%m-%d").date()
    else:
        report_date = timestamp.date()

    service_columns = {
        _RECORDSTAMP_COLUMN: timestamp,
        _REPORT_DATE_COLUMN: report_date,
    }
    return {**rows, **service_columns}


def _get_max_date_for_object(column_name: str, object_id: str, project: str,
                             dataset: str, table: str,
                             object_id_column: str) -> Union[date, None]:
    """Returns the object with the maximum value for the given date column.

    Args:
        column_name(str): Name of the metric field (eg: recordstamp).
        object_id(str): Target object id.
        project(str): Project name.
        dataset(str): Dataset hosting object insights table.
        table(str): Table name with object insights data.
        object_id_column(str): Column containing object id. Ex.: "campaign_id".

    Returns:
        The maximum date value of the given object.
    """
    with BQClient(project=project) as bq_client:
        # Serialize object id as it's stored as JSON serialized string in BQ.
        # Ex.: foo ==> "\"foo\"".
        sql_object_id = '"\\"' + object_id + '\\""'

        sql = (f"SELECT MAX(CAST({column_name} AS DATE)) AS max_value "
               f"FROM `{project}.{dataset}.{table}` "
               f"WHERE {object_id_column} = {sql_object_id}")
        logging.debug("SQL querying maximum %s for object id: %s,\n%s",
                      column_name, object_id, sql)

        query_job = bq_client.query(sql)
        r = query_job.result()

    return None if r.total_rows == 0 else next(r).max_value


def get_first_load_date(object_id: str, project: str, dataset: str, table: str,
                        object_id_column: str, max_load_lookback_days: int):
    """Calculates first load date based on the last load date
    requested from the given table for the particular object.

    Args:
        object_id(str): Target object id.
        project(str): Project name.
        dataset(str): Dataset hosting object insights table.
        table(str): Table name with object insights data.
        object_id_column(str): column containing object id. Ex.: "campaign_id".

    Returns:
        Tuple[str, date, date]: object id processed and date range to load.
    """

    latest_report_date = _get_max_date_for_object(_REPORT_DATE_COLUMN,
                                                  object_id, project, dataset,
                                                  table, object_id_column)
    latest_insert_date = _get_max_date_for_object(_RECORDSTAMP_COLUMN,
                                                  object_id, project, dataset,
                                                  table, object_id_column)

    earliest_load_date = _LATEST_LOAD_DATE - timedelta(
        days=max_load_lookback_days)

    if not latest_report_date:
        # No loads yet, request last year.
        load_start_date = earliest_load_date
    else:

        if not latest_insert_date:
            meta_update_window_days = _META_UPDATE_WINDOW_DAYS
        else:
            # We need to update only 28 days before the date of the latest run.
            meta_update_window_days = min(
                max((latest_report_date - latest_insert_date +
                     timedelta(days=_META_UPDATE_WINDOW_DAYS)).days, -1),
                _META_UPDATE_WINDOW_DAYS)

        # Load start date is 28 days before the last successful load date.
        # Meta may update its insight stats up to 28 days prior.
        # Also make sure it doesn't go past the earliest load date.
        load_start_date = max(
            latest_report_date - timedelta(days=meta_update_window_days),
            earliest_load_date)

    logging.info("%s %s: Extracting period: from %s until %s.",
                 object_id_column, object_id, load_start_date.isoformat(),
                 _LATEST_LOAD_DATE.isoformat())

    # Returning object id, first and last date to extracting.
    return (object_id, load_start_date, _LATEST_LOAD_DATE)
