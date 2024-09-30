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


/* Sample script showing how to calculate KPIs at Ad Set, Age and Gender level from daily aggregate
   table.

   Note that some of the measures and derived measures are non-additive (e.g. ratios), so
   care needs to be taken when aggregating them further (e.g. from daily to monthly level).
*/

SELECT
  agg.report_date,
  agg.adset_id,
  age_gender_details.age,
  age_gender_details.gender,
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
  age_gender_details.impressions,
  age_gender_details.clicks,
  age_gender_details.spend,
  /* Please note that reach is additive by age and gender, but not by adset and date. */
  age_gender_details.reach,
  SAFE_DIVIDE(
    age_gender_details.impressions,
    age_gender_details.reach
  ) AS frequency,
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
    age_gender_details.video_p95_watched_actions_video_views
  ) AS cost_per_completed_view, -- non-additive metric
  SAFE_DIVIDE(
    age_gender_details.video_p95_watched_actions_video_views,
    age_gender_details.impressions
  ) * 100 AS view_through_rate, -- non-additive metric
  SAFE_DIVIDE(
    age_gender_details.post_engagements,
    age_gender_details.reach
  ) * 100 AS engagement_rate,
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
  SAFE_DIVIDE(
    age_gender_details.spend,
    age_gender_details.post_engagements
  ) AS cost_per_engagement, -- non-additive metric
  SAFE_DIVIDE(
    age_gender_details.spend,
    SAFE_DIVIDE(age_gender_details.impressions, 1000)
  ) AS cpm, -- non-additive metric
  SAFE_DIVIDE(
    age_gender_details.spend,
    SAFE_DIVIDE(age_gender_details.reach, 1000)
  ) AS cpp,
  SAFE_DIVIDE(
    age_gender_details.link_clicks,
    age_gender_details.impressions
  ) * 100 AS link_ctr -- non-additive metric
FROM
  `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.AdsetInsightsDailyAgg` AS agg
CROSS JOIN UNNEST(age_gender_details) AS age_gender_details
