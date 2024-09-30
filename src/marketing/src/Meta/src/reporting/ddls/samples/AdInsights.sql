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

/* Sample script showing how to calculate KPIs at Ad level from daily aggregate table.

   Note that some of the measures and derived measures are non-additive (e.g. ratios), so
   care needs to be taken when aggregating them further (e.g. from daily to monthly level).
*/

SELECT
  report_date,
  ad_id,
  campaign_id,
  account_id,
  adset_id,
  account_name,
  campaign_name,
  adset_name,
  ad_name,
  creative_id,
  creative_name,
  account_timezone_name,
  campaign_status,
  campaign_created_time,
  campaign_objective,
  campaign_start_time,
  campaign_stop_time,
  impressions,
  clicks,
  spend,
  SAFE_DIVIDE(impressions, reach) AS frequency, -- non-additive metric
  reach, -- non-additive metric
  video_avg_time_watched_actions_video_views, -- non-additive metric
  video_p25_watched_actions_video_views,
  video_p50_watched_actions_video_views,
  video_p75_watched_actions_video_views,
  video_p95_watched_actions_video_views,
  likes,
  loves,
  hahas,
  wows,
  sads,
  angry,
  post_engagements,
  page_engagements,
  link_clicks,
  post_shares,
  post_reactions,
  post_saves,
  post_comments,
  video_views,
  page_likes,
  photo_views,
  SAFE_DIVIDE(
    spend,
    video_p95_watched_actions_video_views
  ) AS cost_per_completed_view, -- non-additive metric
  SAFE_DIVIDE(
    post_engagements,
    reach
  ) * 100 AS engagement_rate, -- non-additive metric
  SAFE_DIVIDE(
    video_p95_watched_actions_video_views,
    impressions
  ) * 100 AS view_through_rate, -- non-additive metric
  (
    link_clicks
    + post_shares
    + post_reactions
    + post_saves
    + post_comments
    + page_likes
    + video_views
    + photo_views
  ) AS total_engagements,
  SAFE_DIVIDE(spend, post_engagements) AS cost_per_engagement, -- non-additive metric
  SAFE_DIVIDE(spend, SAFE_DIVIDE(impressions, 1000)) AS cpm, -- non-additive metric
  SAFE_DIVIDE(spend, SAFE_DIVIDE(reach, 1000)) AS cpp, -- non-additive metric
  SAFE_DIVIDE(link_clicks, impressions) * 100 AS link_ctr -- non-additive metric
FROM `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.AdInsightsDailyAgg`
