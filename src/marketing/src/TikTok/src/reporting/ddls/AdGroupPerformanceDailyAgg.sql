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

/* The overall amount of engagement with daily granularity.*/


WITH
  AdPerformance AS (
    SELECT
      stat_time_day,
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
      SUM(spend) AS total_cost,
      SUM(impressions) AS total_impressions,
      SUM(clicks) AS total_clicks,
      SUM(conversion) AS total_conversions,
      SUM(video_play_actions) AS total_video_views,
      SUM(likes) AS total_likes,
      SUM(comments) AS total_comments,
      SUM(shares) AS total_shares,
      SUM(follows) AS total_follows,
      SUM(conversion * cost_per_conversion) AS conversion_eligible_cost
    FROM `{{ project_id_tgt }}.{{ marketing_tiktok_datasets_reporting }}.AdPerformance`
    GROUP BY
      stat_time_day,
      advertiser_id,
      advertiser_name,
      adgroup_id,
      adgroup_name,
      campaign_id,
      campaign_name,
      country_code,
      currency,
      timezone,
      display_timezone
  )
SELECT
  AdPerformance.stat_time_day AS date,
  AdPerformance.advertiser_id,
  AdPerformance.campaign_id,
  AdPerformance.adgroup_id,
  AdPerformance.campaign_name,
  AdPerformance.adgroup_name,
  AdPerformance.advertiser_name,
  AdPerformance.country_code,
  AdPerformance.currency,
  AdPerformance.timezone,
  AdPerformance.display_timezone,
  AdPerformance.total_cost,
  AdPerformance.total_impressions,
  AdPerformance.total_clicks,
  -- reach_daily is a partially aggregatable metric over a country_code,
  -- but non-aggregatable over an adgroup/date.
  AdGroupPerformance.reach AS reach_daily,
  AdPerformance.total_conversions,
  AdPerformance.total_video_views,
  AdPerformance.total_likes,
  AdPerformance.total_comments,
  AdPerformance.total_shares,
  AdPerformance.total_follows,
  AdPerformance.conversion_eligible_cost
FROM AdPerformance
INNER JOIN
  `{{ project_id_tgt }}.{{ marketing_tiktok_datasets_reporting }}.AdGroupPerformance`
    AS AdGroupPerformance
  ON
    AdPerformance.advertiser_id = AdGroupPerformance.advertiser_id
    AND AdPerformance.campaign_id = AdGroupPerformance.campaign_id
    AND AdPerformance.adgroup_id = AdGroupPerformance.adgroup_id
    AND AdPerformance.country_code = AdGroupPerformance.country_code
    AND AdPerformance.stat_time_day = AdGroupPerformance.stat_time_day
