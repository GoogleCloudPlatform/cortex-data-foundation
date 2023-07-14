# Copyright 2022 Google LLC
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
"""A lightweight BigQuery component for reading ads_to_sap_modified_mappings
view.
Made as a workaround for b/273265766.
"""

import apache_beam as beam

from pipeline_job.pipeline_options import CatgapOptions
from pipeline_job.read_from_bq import ReadFromBigQueryLight


class ReadModifiedAdGroups(beam.PTransform):
    """It reads ads_to_sap_modified_mappings view
    as a list of ad_group_id for Ad Groups
    that were manually modified in the spreadsheet
    so that their mappings are different from the most recent mappings
    in ads_to_sap_prod_hier_log table.
    """

    def expand(self, input_or_inputs):
        ct_options = input_or_inputs.pipeline.options.view_as(CatgapOptions)

        ads_groups_with_modifications = (
            f"`{ct_options.source_project}.{ct_options.k9_processing_dataset}."
            "ads_to_sap_modified_mappings`")
        query = f"SELECT ad_group_id FROM {ads_groups_with_modifications}"

        results = (input_or_inputs | ReadFromBigQueryLight(
            query=query, location=ct_options.bigquery_location))
        return (results | beam.Map(lambda elem: str(elem["ad_group_id"])))
