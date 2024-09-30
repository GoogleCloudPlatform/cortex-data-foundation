# -- Copyright 2024 Google LLC
# --
# -- Licensed under the Apache License, Version 2.0 (the "License");
# -- you may not use this file except in compliance with the License.
# -- You may obtain a copy of the License at
# --
# --      https://www.apache.org/licenses/LICENSE-2.0
# --
# -- Unless required by applicable law or agreed to in writing, software
# -- distributed under the License is distributed on an "AS IS" BASIS,
# -- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# -- See the License for the specific language governing permissions and
# -- limitations under the License.

/* Sample script showing how to calculate KPIs at Ad Set and Country level from daily aggregate
   table.

   Note that some of the measures and derived measures are non-additive (e.g. ratios), so
   care needs to be taken when aggregating them further (e.g. from daily to monthly level).
*/


SELECT
  agg.report_date,
  agg.adset_id,
  country_details.country,
  ARRAY_TO_STRING(ARRAY(SELECT DISTINCT name FROM UNNEST(agg.targeting_audiences) ORDER BY 1), ' | ')
    AS targeting_audiences_names,
  agg.account_id,
  agg.campaign_id,
  agg.account_name,
  agg.account_currency,
  agg.account_timezone_name,
  agg.adset_name,
  agg.campaign_name,
  agg.campaign_status,
  agg.campaign_created_time,
  agg.campaign_objective,
  agg.campaign_start_time,
  agg.campaign_stop_time,
  country_details.impressions,
  country_details.clicks,
  country_details.spend,
  -- NOTE:  reach is additive by country, but not by adset and date.
  country_details.reach,
  SAFE_DIVIDE(
    country_details.impressions,
    country_details.reach
  ) AS frequency,
  country_details.post_engagements,
  country_details.page_engagements,
  country_details.link_clicks,
  country_details.post_shares,
  country_details.post_reactions,
  country_details.post_saves,
  country_details.post_comments,
  country_details.video_views,
  country_details.page_likes,
  country_details.photo_views,
  country_details.video_avg_time_watched_actions_video_views, -- non-additive metric
  country_details.video_p25_watched_actions_video_views,
  country_details.video_p50_watched_actions_video_views,
  country_details.video_p75_watched_actions_video_views,
  country_details.video_p95_watched_actions_video_views,
  SAFE_DIVIDE(
    country_details.spend,
    country_details.video_p95_watched_actions_video_views
  ) AS cost_per_completed_view, -- non-additive metric
  SAFE_DIVIDE(
    country_details.video_p95_watched_actions_video_views,
    country_details.impressions
  ) * 100 AS view_through_rate, -- non-additive metric
  SAFE_DIVIDE(
    country_details.post_engagements,
    country_details.reach
  ) * 100 AS engagement_rate,
  (
    country_details.link_clicks
    + country_details.post_shares
    + country_details.post_reactions
    + country_details.post_saves
    + country_details.post_comments
    + country_details.page_likes
    + country_details.video_views
    + country_details.photo_views
  ) AS total_engagements,
  SAFE_DIVIDE(
    country_details.spend,
    country_details.post_engagements
  ) AS cost_per_engagement, -- non-additive metric
  SAFE_DIVIDE(
    country_details.spend,
    SAFE_DIVIDE(country_details.impressions, 1000)
  ) AS cpm, -- non-additive metric
  SAFE_DIVIDE(
    country_details.spend,
    SAFE_DIVIDE(country_details.reach, 1000)
  ) AS cpp,
  SAFE_DIVIDE(
    country_details.link_clicks,
    country_details.impressions
  ) * 100 AS link_ctr -- non-additive metric
FROM
  `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.AdsetInsightsDailyAgg` AS agg
CROSS JOIN UNNEST(country_details) AS country_details
