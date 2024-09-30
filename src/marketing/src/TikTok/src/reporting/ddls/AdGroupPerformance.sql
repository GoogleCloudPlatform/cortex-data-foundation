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

/* This is reporting level base view.
*
* This view is representing a table from CDC layer in reporting.
*/


SELECT
  stat_time_day,
  advertiser_id,
  campaign_id,
  adgroup_id,
  country_code, -- noqa: L034
  * EXCEPT (stat_time_day, advertiser_id, campaign_id, adgroup_id, country_code, recordstamp)
FROM `{{ project_id_src }}.{{ marketing_tiktok_datasets_cdc }}.auction_adgroup_performance`
