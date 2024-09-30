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
"""Pipeline part of report processing.

This module contains the elements of TikTok API response processing.
"""

from typing import Any, Callable, Dict

import apache_beam as beam
from apache_beam.transforms.ptransform import ptransform_fn

_RECORDSTAMP_COLUMN = "recordstamp"


def _create_report_row(item, timestamp):
    """It combines the metrics and dimensions with system field recordstamp."""
    return {
        _RECORDSTAMP_COLUMN: timestamp,
        **item["dimensions"],
        **item["metrics"],
    }


def _rename_fields(item: Dict[str, Any], mapping: Dict[str,
                                                       str]) -> Dict[str, Any]:
    """Renames fields of processed item regarding schema csv file."""
    return {mapping[field]: value for (field, value) in item.items()}


@ptransform_fn
def process_api_data(pcoll, getter: Callable, query: dict, timestamp: float,
                     column_mapping: dict):
    """Fetches entities from TikTok Marketing API by the getter method.

    See the API overview below:
    https://ads.tiktok.com/marketing_api/docs?id=1735712062490625

    Args:
        pcoll (PCollection): Collection of data to process
        getter (Callable): Function from TikTok API connector to get data.
        query (dict): Query params for endpoint.
        timestamp (float): Timestamp of recording the data.
        column_mapping (dict): Column renames based on user config.

    Returns:
        PCollection: Processed items.
    """
    # yapf: disable
    return (pcoll
            | "Get data" >> beam.FlatMap(lambda adv_data:
                                         getter(query=query,
                                                advertiser_data=adv_data))
            | "Create rows" >> beam.Map(_create_report_row, timestamp=timestamp)
            | "Rename columns" >> beam.Map(_rename_fields,
                                                      mapping=column_mapping)
    )
