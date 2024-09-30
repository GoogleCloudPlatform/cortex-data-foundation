# Copyright 2023 Google LLC

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Utility functions for TikTok pipeline."""

import copy
import csv
from datetime import date
from datetime import timedelta
import json
from pathlib import Path
from typing import Dict, List, Optional

from google.cloud import secretmanager
from google.cloud.bigquery import Client
from google.cloud.bigquery import SchemaField

_SOURCE_FIELD = "SourceField"
_TARGET_FIELD = "TargetField"
_RECORDSTAMP_COLUMN = "recordstamp"
_MAX_AVAILABLE_DAYS = 30

# Queries for TikTok report.
# Ref.: https://ads.tiktok.com/marketing_api/docs?id=1740302848100353
REPORT_QUERIES = {
    "auction_ad_performance": {
        "report_type":
            "BASIC",
        "service_type":
            "AUCTION",
        "data_level":
            "AUCTION_AD",
        "page_size":
            1000,
        "filter":
            json.dumps([{
                "field_name": "ad_status",
                "filter_type": "IN",
                "filter_value": "[\"STATUS_ALL\"]"
            }]),
        "dimensions":
            json.dumps(["ad_id", "country_code", "stat_time_day"]),
        "metrics":
            json.dumps([
                "campaign_name", "campaign_id", "adgroup_name", "adgroup_id",
                "ad_name", "aeo_type", "ad_text", "image_mode",
                "gross_impressions", "cost_per_1000_reached",
                "real_time_conversion", "real_time_cost_per_conversion",
                "real_time_conversion_rate", "result", "cost_per_result",
                "result_rate", "real_time_result", "real_time_cost_per_result",
                "real_time_result_rate", "is_smart_creative",
                "video_watched_2s", "video_watched_6s", "average_video_play",
                "average_video_play_per_user", "video_views_p25",
                "video_views_p50", "video_views_p75", "video_views_p100",
                "engaged_view", "profile_visits", "profile_visits_rate",
                "clicks_on_music_disc", "spend", "impressions", "clicks", "ctr",
                "cpc", "cpm", "reach", "frequency", "conversion",
                "video_play_actions", "likes", "comments", "shares", "follows",
                "clicks", "currency", "budget", "campaign_budget",
                "placement_type", "smart_target", "cost_per_conversion",
                "conversion_rate", "conversion_rate_v2", "image_mode"
            ])
    },
    "auction_adgroup_performance": {
        "report_type":
            "BASIC",
        "service_type":
            "AUCTION",
        "data_level":
            "AUCTION_ADGROUP",
        "page_size":
            1000,
        "filter":
            json.dumps([{
                "field_name": "ad_status",
                "filter_type": "IN",
                "filter_value": "[\"STATUS_ALL\"]"
            }]),
        "dimensions":
            json.dumps(["adgroup_id", "country_code", "stat_time_day"]),
        "metrics":
            json.dumps([
                "campaign_id", "campaign_name", "adgroup_name", "reach",
                "frequency"
            ])
    }
}


def get_secret(project_id: str, secret_name: str) -> str:
    """Getting credentials from Google Secret Manager."""
    name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    secret_client = secretmanager.SecretManagerServiceClient()
    response = secret_client.access_secret_version(name=name)
    payload = response.payload.data.decode("UTF-8")
    return payload


# TODO Copy from table_creation_utils. Should be investigated how to share
# commonly used code for pipelines.
def _create_bq_schema_from_mapping(mapping_file: Path,
                                   target_field: str) -> list:
    """Generates BigQuery schema from mapping file.

    Args:
        mapping_file (Path): Schema mapping file path.
        target_field (str): Name of column with target field names.

    Return:
        BQ schema as list of fields and datatypes.
    """
    schema = []
    with open(mapping_file, encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f, delimiter=","):
            type_ = "STRING"
            schema.append(SchemaField(name=row[target_field], field_type=type_))

    return schema


def create_bq_schema(mapping_file: Path) -> Dict[str, List]:
    """Creates final BQ Schema.

    This function adds additional fields to schema created from mapping and
    converts to BQ API format which is usable for Beam BQ sink.
    It also adds recordstamp field to the final schema.

    Args:
        mapping_file (Path): Schema mapping file path.
    """
    bq_schema = _create_bq_schema_from_mapping(mapping_file,
                                               target_field="TargetField")

    output_schema = copy.deepcopy(bq_schema)

    if "recordstamp" not in {field.name for field in bq_schema}:
        output_schema.append(
            SchemaField(name="recordstamp", field_type="TIMESTAMP"))

    schema_in_beam_format = {
        "fields": [field.to_api_repr() for field in output_schema]
    }

    return schema_in_beam_format


def create_column_mapping(path_to_mapping: str) -> dict:
    """Creates a mapping dictionary which resolves connection between
    CSV mapping files API fields and table columns in BigQuery.
    """
    map_dict = {}
    with open(path_to_mapping, encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f, delimiter=","):
            map_dict[row[_SOURCE_FIELD]] = row[_TARGET_FIELD]

    map_dict[_RECORDSTAMP_COLUMN] = _RECORDSTAMP_COLUMN

    return map_dict


def get_latest_load_date(client: Client, project: str, dataset: str,
                         table: str) -> Optional[date]:
    """Get latest data load date from the given table."""
    query_job = client.query(
        ("SELECT DATE(MAX(stat_time_day)) AS latest_load_date FROM "
         f"`{project}.{dataset}.{table}`"))
    results = query_job.result()

    if results.total_rows == 0:
        return None

    return next(results).latest_load_date


def calculate_query_time_range(latest_load_date: Optional[date],
                               today: date) -> Dict[str, str]:
    """Calculates time range of report query based on the latest
    successful data load.

    The maximum time range is 30 days because the reporting API supports
    30 days of time range in case of daily granularity.
    """

    # TikTok stores insights for a maximum amount of days.
    earliest_available_date = today - timedelta(days=_MAX_AVAILABLE_DAYS)

    # For incremental load, load previous 3 days again. But no earlier
    # than the earliest available date.
    if latest_load_date:
        start_date = max(latest_load_date - timedelta(days=3),
                         earliest_available_date)

    # For initial load (no latest load date), load max available days.
    else:
        start_date = earliest_available_date

    # Load up to yesterday as today's data is not yet finalized.
    end_date = today - timedelta(days=1)

    report_time_range = {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat()
    }

    return report_time_range
