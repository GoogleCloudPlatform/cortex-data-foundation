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
"""Utility functions for Salesforce Marketing Cloud pipeline."""

from csv import DictReader
from datetime import datetime
import io
import logging
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.filesystem import FileMetadata
from apache_beam.io.filesystems import FileSystems
from google.cloud import bigquery

_TECHNICAL_COLUMNS = {
    "SourceFileLastUpdateTimeStamp": "TIMESTAMP",
    "SourceFileName": "STRING",
    "RecordStamp": "TIMESTAMP"
}


def get_max_recordstamp(client: bigquery.Client, project: str, dataset: str,
                        table: str) -> Optional[float]:
    """Gets the latest RecordStamp from the given table.

    Args:
        client (bigquery.Client): BigQuery Client object.
        project (str): Name of the GCP project.
        dataset (str): Name of the project.
        table (str): Name of the table.

    Returns:
        float: Latest RecordStamp in the table.
    """
    query_job = client.query(
        "SELECT UNIX_SECONDS(MAX(RecordStamp)) AS max_recordstamp "
        f"FROM `{project}.{dataset}.{table}`")
    results = query_job.result()
    if not results.total_rows:
        return None
    return next(results).max_recordstamp


def read_csv_by_rows(input_file: Any) -> Iterable[Dict[str, str]]:
    """Reads csv data row by row into a dictionary.

    Args:
        input_file (FileMetadata): Input file.

    Yields:
        Dict: Current row of the file.
    """
    file_name: str = input_file.path
    file_updated_timestamp: float = FileSystems.last_updated(file_name)
    file_updated_timestamp_iso: str = datetime.fromtimestamp(
        file_updated_timestamp).isoformat()

    with FileSystems.open(file_name,
                          compression_type=CompressionTypes.UNCOMPRESSED) as f:
        for row in DictReader(io.TextIOWrapper(f, encoding="utf-8-sig")):
            updated_row = row.copy()
            updated_row["SourceFileName"] = file_name
            updated_row[
                "SourceFileLastUpdateTimeStamp"] = file_updated_timestamp_iso
            yield updated_row


def create_mapping_by_schema_definition(mapping_file: Path) -> Dict[str, Any]:
    """Creating the mapping defined by schema file.
    SourceField column values renamed to TargetField column values.

    Args:
        mapping_file (Path): Mapping file location.

    Returns:
        Dict: Mapping dictionary where keys are the original column names and
        values are the target column names.
    """
    column_mapping = {}
    with open(mapping_file, encoding="utf-8", newline="") as f:
        for row in DictReader(f, delimiter=","):
            column_mapping[row["SourceField"]] = row["TargetField"]

    return column_mapping


def transform_source_data(row: Dict[str, Any], mappings: Dict[str, Any],
                          timestamp: float) -> Dict[str, Any]:
    """Transforming the current row of the input data.
    Renaming columns, filling the RecordStamp, setting defaults.

    Args:
        row (Dict): Row data.
        mappings (Dict): Mapping for column rename steps.
        timestamp (float): Load timestamp.

    Returns:
        Dict: Final transformed row data.
    """
    renamed_column_data = {}
    for key in row:
        # Skip extra columns if present in the data file.
        possible_keys = list(_TECHNICAL_COLUMNS.keys()) + list(mappings.keys())
        if key not in possible_keys:
            continue

        # Renaming columns.
        mapping_key = key
        if key not in _TECHNICAL_COLUMNS:
            mapping_key = mappings[key]

        # Setting default value to None.
        renamed_column_data[mapping_key] = row[key] if row[key] != "" else None

    # Filling RecordStamp column with timestamp.
    renamed_column_data["RecordStamp"] = timestamp

    return renamed_column_data

def is_file_schema_valid(file: FileMetadata, mapping: Dict[str, str]) -> bool:
    """Filter files based on the requested schema.

    Args:
        file (FileMetadata): FileMetadata object.
        mapping (Dict): Mapping for column rename steps.

    Returns:
        bool: Check if the file contains the necessary column names.
    """

    # Create a DictReader object and extract header names.
    file_path = file.path
    with FileSystems.open(file_path,
                          compression_type=CompressionTypes.UNCOMPRESSED) as f:
        dict_reader = DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
        csv_colnames = dict_reader.fieldnames

    # Check column name lists.
    csv_colnames_set = set(csv_colnames)
    mapping_colnames_set = set(mapping.keys())
    missing_columns = mapping_colnames_set - csv_colnames_set

    if not missing_columns:
        # Early exit on matching headers.
        return True

    error_message = ("Missing column names from the file: "
                     + ", ".join(missing_columns))

    logging.error(error_message)

    raise RuntimeError(f"{file_path} has missing_columns.")
