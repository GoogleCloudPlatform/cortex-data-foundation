# Copyright 2023 Google LLC
#
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
"""Utilities for mapping and schema processing from CSV file"""

import csv
from dataclasses import dataclass
from datetime import date
import gzip
import logging
from typing import Any, Callable, Dict, List, Optional

from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.filesystems import FileSystems
from dateutil import parser

_RECORDSTAMP_COLUMN = "recordstamp"
_SOURCE_FILE_NAME_COLUMN = "source_file_name"
_SOURCE_FILE_LAST_UPDATE_TIMESTAMP_COLUMN = "source_file_last_update_timestamp"

_SOURCE_FIELD = "SourceField"
_TARGET_FIELD = "TargetField"
_BQ_DATATYPE_FIELD = "DataType"


def _cast_to_bool(boolean_string: str) -> bool:
    val = boolean_string.strip().lower()
    if val == "true":
        return True
    elif val == "false":
        return False
    else:
        raise TypeError("This column cannot be casted to bool.")


def _cast_to_date(date_string: str) -> date:
    return parser.parse(date_string).date()


_BQ_PYTHON_TYPES_MAPPING: Dict[str, Callable] = {
    "INT64": int,
    "FLOAT64": float,
    "STRING": str,
    "BOOL": _cast_to_bool,
    "DATE": _cast_to_date,
    "TIMESTAMP": float
}


class LackBigQueryToPythonTypeMappingError(Exception):
    """Used when there is no connection between BQ and Python type."""


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


def yield_dict_rows_from_compressed_csv(file, timestamp: float):
    """Uncompresses CSV file from Storage bucket, reads it line by line
    and add service columns."""
    with FileSystems.open(file.path,
                          compression_type=CompressionTypes.UNCOMPRESSED) as f:
        # Open uncompressed.
        # Then process explicitly because Beam uncompress caused issues.
        with gzip.open(f, "rt") as gzip_text_io_wrapper:
            for row in csv.DictReader(gzip_text_io_wrapper):
                yield _add_service_columns(row, timestamp, file.path,
                                           file.last_updated_in_seconds)


def _add_service_columns(row: dict, timestamp: float, object_path: str,
                         updated_timestamp: float) -> Dict[str, Any]:
    service_columns = {
        _RECORDSTAMP_COLUMN: timestamp,
        _SOURCE_FILE_NAME_COLUMN: object_path,
        _SOURCE_FILE_LAST_UPDATE_TIMESTAMP_COLUMN: updated_timestamp,
    }
    return {**row, **service_columns}


def change_dict_key_names(row, mapping):
    """Takes a row as dict and creates a new dict with the key names
    according to the mapping provided.
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
        mapping: dict) -> Dict[str, List[Dict[str, str]]]:
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


def create_column_mapping(path_to_mapping: str) -> dict:
    """Loads CSV mapping file and converts it to a dictionary
    that has connects column names and types.
    """
    map_dict = {}
    with open(path_to_mapping, "r", encoding="utf-8") as file:
        for row in csv.DictReader(file):
            map_dict[row[_SOURCE_FIELD]] = _create_column_mapping(row)

    return _inject_service_columns(map_dict)


def _create_column_mapping(row: Dict[str, str]) -> _ColumnMapping:
    bq_type = row[_BQ_DATATYPE_FIELD]
    return _ColumnMapping(
        original_name=row[_SOURCE_FIELD],
        target_name=row[_TARGET_FIELD],
        bq_type=bq_type,
        python_type=_transform_bq_to_python_type(bq_type),
    )


def _inject_service_columns(mapping: dict) -> dict:
    service_rows = [
        {
            _SOURCE_FIELD: _RECORDSTAMP_COLUMN,
            _TARGET_FIELD: _RECORDSTAMP_COLUMN,
            _BQ_DATATYPE_FIELD: "TIMESTAMP"
        },
        {
            _SOURCE_FIELD: _SOURCE_FILE_NAME_COLUMN,
            _TARGET_FIELD: _SOURCE_FILE_NAME_COLUMN,
            _BQ_DATATYPE_FIELD: "STRING"
        },
        {
            _SOURCE_FIELD: _SOURCE_FILE_LAST_UPDATE_TIMESTAMP_COLUMN,
            _TARGET_FIELD: _SOURCE_FILE_LAST_UPDATE_TIMESTAMP_COLUMN,
            _BQ_DATATYPE_FIELD: "TIMESTAMP"
        },
    ]
    new_mapping = dict(mapping)
    for row in service_rows:
        new_mapping[row[_SOURCE_FIELD]] = _create_column_mapping(row)
    return new_mapping


def _transform_bq_to_python_type(bq_type: str) -> type:
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


def get_max_recordstamp(client, project, dataset, table) -> Optional[float]:
    query_job = client.query(
        "SELECT UNIX_SECONDS(MAX(recordstamp)) AS max_recordstamp "
        f"FROM `{project}.{dataset}.{table}`")
    results = query_job.result()
    if not results.total_rows:
        return None
    return next(results).max_recordstamp
