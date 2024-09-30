# -- Copyright 2024 Google LLC
# --
# -- Licensed under the Apache License, Version 2.0 (the "License");
# -- you may not use this file except in compliance with the License.
# -- You may obtain a copy of the License at
# --
# --     https://www.apache.org/licenses/LICENSE-2.0
# --
# -- Unless required by applicable law or agreed to in writing, software
# -- distributed under the License is distributed on an "AS IS" BASIS,
# -- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# -- See the License for the specific language governing permissions and
# -- limitations under the License.

/* Reporting base view - representing data from CDC layer. */

SELECT
  ad.id AS adgroup_ad_id,
  CAST(SPLIT(ad_group, '/')[ORDINAL(4)] AS INT64) AS adgroup_id,
  *
FROM `{{ project_id_src }}.{{ marketing_googleads_datasets_cdc }}.adgroup_ad`
