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

/* This is a sample script showing how to directly use the AdGroupPerformanceDailyAgg table
* that contain pre-aggregated measures as reported by TikTok reporting API.
* Some of the measures are non-additive and hence should not be aggregated directly further.
* Instead such fields should be aggregated and calculated
* by using the underlying core additive measures.
*
* All cost metrics are presented in advertiser currency.
*/

SELECT
  date,
  advertiser_id,
  campaign_id,
  adgroup_id,
  campaign_name,
  adgroup_name,
  advertiser_name,
  country_code,
  currency,
  timezone,
  display_timezone,
  total_cost,
  total_impressions,
  total_clicks,
  COALESCE(SAFE_DIVIDE(total_clicks, total_impressions), 0) * 100 AS click_through_rate,
  COALESCE(SAFE_DIVIDE(total_cost, total_clicks), 0) AS cost_per_click,
  COALESCE(SAFE_DIVIDE(total_cost, (total_impressions / 1000)), 0) AS cost_per_thousand_impressions,
  -- reach_daily and frequency_daily are partially aggregatable metrics over a country_code,
  -- but non-aggregatable over an adgroup/date.
  reach_daily,
  COALESCE(SAFE_DIVIDE(total_impressions, reach_daily), 0) AS frequency_daily,
  total_conversions,
  COALESCE(SAFE_DIVIDE(conversion_eligible_cost, total_conversions), 0) AS cost_per_conversion,
  total_video_views,
  (total_likes + total_comments + total_shares + total_follows + total_clicks) AS total_engagement,
  COALESCE(
    SAFE_DIVIDE(
      total_likes + total_comments + total_shares + total_follows + total_clicks,
      total_impressions
    ),
    0)
    AS engagement_rate -- noqa: L003
FROM `{{ project_id_tgt }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformanceDailyAgg`
