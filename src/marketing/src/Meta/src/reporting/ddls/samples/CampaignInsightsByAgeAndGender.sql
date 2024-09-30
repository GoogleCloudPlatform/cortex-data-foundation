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

/* Sample script showing how to calculate KPIs at Campaign, Age and Gender level from daily
   aggregate table.

   Note that some of the measures and derived measures are non-additive (e.g. ratios), so
   care needs to be taken when aggregating them further (e.g. from daily to monthly level).
*/


SELECT
  campaign_agg.report_date,
  campaign_agg.campaign_id,
  age_gender_details.age,
  age_gender_details.gender,
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
  age_gender_details.impressions,
  age_gender_details.clicks,
  age_gender_details.spend,
  -- NOTE: reach is additive by age and gender, but not by campaign and date.
  age_gender_details.reach,
  SAFE_DIVIDE(age_gender_details.impressions, age_gender_details.reach) AS frequency,
  age_gender_details.post_engagements,
  age_gender_details.page_engagements,
  age_gender_details.link_clicks,
  age_gender_details.post_shares,
  age_gender_details.post_reactions,
  age_gender_details.post_saves,
  age_gender_details.post_comments,
  age_gender_details.video_views,
  age_gender_details.page_likes,
  age_gender_details.photo_views,
  age_gender_details.video_avg_time_watched_actions_video_views, -- non-additive metric
  age_gender_details.video_p25_watched_actions_video_views,
  age_gender_details.video_p50_watched_actions_video_views,
  age_gender_details.video_p75_watched_actions_video_views,
  age_gender_details.video_p95_watched_actions_video_views,
  SAFE_DIVIDE(
    age_gender_details.spend,
    age_gender_details.video_p95_watched_actions_video_views)
      AS cost_per_completed_view, -- non-additive metric --noqa: LT02
  SAFE_DIVIDE(age_gender_details.post_engagements, age_gender_details.reach) * 100 AS engagement_rate,
  SAFE_DIVIDE(
    age_gender_details.video_p95_watched_actions_video_views,
    age_gender_details.impressions
  ) * 100 AS view_through_rate, -- non-additive metric
  (
    age_gender_details.link_clicks
    + age_gender_details.post_shares
    + age_gender_details.post_reactions
    + age_gender_details.post_saves
    + age_gender_details.post_comments
    + age_gender_details.page_likes
    + age_gender_details.video_views
    + age_gender_details.photo_views
  ) AS total_engagements,
  SAFE_DIVIDE(age_gender_details.spend, age_gender_details.post_engagements) AS cost_per_engagement, -- non-additive metric
  SAFE_DIVIDE(age_gender_details.spend, SAFE_DIVIDE(age_gender_details.impressions, 1000)) AS cpm, -- non-additive metric
  SAFE_DIVIDE(age_gender_details.spend, SAFE_DIVIDE(age_gender_details.reach, 1000)) AS cpp,
  SAFE_DIVIDE(
    age_gender_details.link_clicks,
    age_gender_details.impressions
  ) * 100 AS link_ctr -- non-additive metric
FROM `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.CampaignInsightsDailyAgg` AS campaign_agg
CROSS JOIN UNNEST(campaign_agg.age_gender_details) AS age_gender_details
