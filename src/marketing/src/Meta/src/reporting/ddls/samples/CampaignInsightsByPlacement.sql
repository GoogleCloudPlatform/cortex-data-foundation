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

/* Sample script showing how to calculate KPIs at Campaign and Placement level from daily aggregate
   table.

   Note that some of the measures and derived measures are non-additive (e.g. ratios), so
   care needs to be taken when aggregating them further (e.g. from daily to monthly level).
*/


SELECT
  campaign_agg.report_date,
  campaign_agg.campaign_id,
  placement_details.publisher_platform,
  placement_details.platform_position,
  campaign_agg.campaign_name,
  campaign_agg.account_id,
  campaign_agg.account_name,
  campaign_agg.account_currency,
  campaign_agg.account_timezone_name,
  campaign_agg.campaign_status,
  campaign_agg.campaign_created_time,
  campaign_agg.campaign_objective,
  campaign_agg.campaign_start_time,
  campaign_agg.campaign_stop_time,
  placement_details.impressions,
  placement_details.clicks,
  placement_details.spend,
  placement_details.reach, -- non-additive metric
  SAFE_DIVIDE(placement_details.impressions, placement_details.reach) AS frequency, -- non-additive metric
  placement_details.post_engagements,
  placement_details.page_engagements,
  placement_details.link_clicks,
  placement_details.post_shares,
  placement_details.post_reactions,
  placement_details.post_saves,
  placement_details.post_comments,
  placement_details.video_views,
  placement_details.page_likes,
  placement_details.photo_views,
  placement_details.video_avg_time_watched_actions_video_views, -- non-additive metric
  placement_details.video_p25_watched_actions_video_views,
  placement_details.video_p50_watched_actions_video_views,
  placement_details.video_p75_watched_actions_video_views,
  placement_details.video_p95_watched_actions_video_views,
  SAFE_DIVIDE(placement_details.spend, placement_details.video_p95_watched_actions_video_views)
    AS cost_per_completed_view, -- non-additive metric
  SAFE_DIVIDE(
    placement_details.post_engagements,
    placement_details.reach
  ) * 100 AS engagement_rate, -- non-additive metric
  SAFE_DIVIDE(
    placement_details.video_p95_watched_actions_video_views,
    placement_details.impressions
  ) * 100 AS view_through_rate, -- non-additive metric
  (
    placement_details.link_clicks
    + placement_details.post_shares
    + placement_details.post_reactions
    + placement_details.post_saves
    + placement_details.post_comments
    + placement_details.page_likes
    + placement_details.video_views
    + placement_details.photo_views
  ) AS total_engagements,
  SAFE_DIVIDE(placement_details.spend, placement_details.post_engagements) AS cost_per_engagement, -- non-additive metric
  SAFE_DIVIDE(placement_details.spend, SAFE_DIVIDE(placement_details.impressions, 1000)) AS cpm, -- non-additive metric
  SAFE_DIVIDE(placement_details.spend, SAFE_DIVIDE(placement_details.reach, 1000)) AS cpp, -- non-additive metric
  SAFE_DIVIDE(
    placement_details.link_clicks,
    placement_details.impressions
  ) * 100 AS link_ctr -- non-additive metric
FROM `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.CampaignInsightsDailyAgg` AS campaign_agg
CROSS JOIN UNNEST(campaign_agg.placement_details) AS placement_details
