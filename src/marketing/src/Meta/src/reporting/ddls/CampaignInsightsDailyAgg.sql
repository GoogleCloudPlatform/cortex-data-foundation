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

/* Aggregated table at Campaign and date levels, with additional breakdowns by country, age / gender, placement, and platform. */

WITH
  CountryDetails AS (
    SELECT
      campaign_id,
      report_date,
      country,
      impressions,
      clicks,
      spend,
      reach,
      frequency,
      `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.AggregateActions`(actions) AS actions,
      `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.AggregateVideoViews`(video_avg_time_watched_actions) AS video_avg_time_watched_actions_video_views,
      `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.AggregateVideoViews`(video_p25_watched_actions) AS video_p25_watched_actions_video_views,
      `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.AggregateVideoViews`(video_p50_watched_actions) AS video_p50_watched_actions_video_views,
      `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.AggregateVideoViews`(video_p75_watched_actions) AS video_p75_watched_actions_video_views,
      `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.AggregateVideoViews`(video_p95_watched_actions) AS video_p95_watched_actions_video_views
    FROM `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.CampaignInsightsByCountry`
  ),

  GroupedCountryDetails AS (
    SELECT
      campaign_id,
      report_date,
      ARRAY_AGG(
        STRUCT(
          country,
          COALESCE(impressions, 0) AS impressions,
          COALESCE(clicks, 0) AS clicks,
          COALESCE(spend, 0) AS spend,
          COALESCE(reach, 0) AS reach,
          COALESCE(frequency, 0) AS frequency,
          actions.post_engagements,
          actions.page_engagements,
          actions.link_clicks,
          actions.post_shares,
          actions.post_reactions,
          actions.post_saves,
          actions.post_comments,
          actions.video_views,
          actions.page_likes,
          actions.photo_views,
          video_avg_time_watched_actions_video_views,
          video_p25_watched_actions_video_views,
          video_p50_watched_actions_video_views,
          video_p75_watched_actions_video_views,
          video_p95_watched_actions_video_views
        )
      ) AS country_details
    FROM CountryDetails
    GROUP BY
      campaign_id,
      report_date
  ),

  PlacementDetails AS (
    SELECT
      campaign_id,
      report_date,
      publisher_platform,
      platform_position,
      impressions,
      clicks,
      spend,
      reach,
      frequency,
      `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.AggregateActions`(actions) AS actions,
      `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.AggregateVideoViews`(video_avg_time_watched_actions) AS video_avg_time_watched_actions_video_views,
      `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.AggregateVideoViews`(video_p25_watched_actions) AS video_p25_watched_actions_video_views,
      `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.AggregateVideoViews`(video_p50_watched_actions) AS video_p50_watched_actions_video_views,
      `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.AggregateVideoViews`(video_p75_watched_actions) AS video_p75_watched_actions_video_views,
      `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.AggregateVideoViews`(video_p95_watched_actions) AS video_p95_watched_actions_video_views
    FROM
      `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.CampaignInsightsByPlacement`
  ),

  GroupedPlacementDetails AS (
    SELECT
      campaign_id,
      report_date,
      ARRAY_AGG(
        STRUCT(
          publisher_platform,
          platform_position,
          COALESCE(impressions, 0) AS impressions,
          COALESCE(clicks, 0) AS clicks,
          COALESCE(spend, 0) AS spend,
          COALESCE(reach, 0) AS reach,
          COALESCE(frequency, 0) AS frequency,
          actions.post_engagements,
          actions.page_engagements,
          actions.link_clicks,
          actions.post_shares,
          actions.post_reactions,
          actions.post_saves,
          actions.post_comments,
          actions.video_views,
          actions.page_likes,
          actions.photo_views,
          video_avg_time_watched_actions_video_views,
          video_p25_watched_actions_video_views,
          video_p50_watched_actions_video_views,
          video_p75_watched_actions_video_views,
          video_p95_watched_actions_video_views
        )
      ) AS placement_details
    FROM PlacementDetails
    GROUP BY
      campaign_id,
      report_date
  ),

  AgeGenderDetails AS (
    SELECT
      campaign_id,
      report_date,
      age,
      gender,
      impressions,
      clicks,
      spend,
      reach,
      frequency,
      `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.AggregateActions`(actions) AS actions,
      `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.AggregateVideoViews`(video_avg_time_watched_actions) AS video_avg_time_watched_actions_video_views,
      `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.AggregateVideoViews`(video_p25_watched_actions) AS video_p25_watched_actions_video_views,
      `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.AggregateVideoViews`(video_p50_watched_actions) AS video_p50_watched_actions_video_views,
      `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.AggregateVideoViews`(video_p75_watched_actions) AS video_p75_watched_actions_video_views,
      `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.AggregateVideoViews`(video_p95_watched_actions) AS video_p95_watched_actions_video_views
    FROM
      `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.CampaignInsightsByAgeAndGender`
  ),

  GroupedAgeGenderDetails AS (
    SELECT
      campaign_id,
      report_date,
      ARRAY_AGG(
        STRUCT(
          age,
          gender,
          COALESCE(impressions, 0) AS impressions,
          COALESCE(clicks, 0) AS clicks,
          COALESCE(spend, 0) AS spend,
          COALESCE(reach, 0) AS reach,
          COALESCE(frequency, 0) AS frequency,
          actions.post_engagements,
          actions.page_engagements,
          actions.link_clicks,
          actions.post_shares,
          actions.post_reactions,
          actions.post_saves,
          actions.post_comments,
          actions.video_views,
          actions.page_likes,
          actions.photo_views,
          video_avg_time_watched_actions_video_views,
          video_p25_watched_actions_video_views,
          video_p50_watched_actions_video_views,
          video_p75_watched_actions_video_views,
          video_p95_watched_actions_video_views
        )
      ) AS age_gender_details
    FROM AgeGenderDetails
    GROUP BY
      campaign_id,
      report_date
  ),

  PlatformDetails AS (
    SELECT
      campaign_id,
      report_date,
      publisher_platform,
      impressions,
      clicks,
      spend,
      reach,
      frequency,
      `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.AggregateActions`(actions) AS actions,
      `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.AggregateVideoViews`(video_avg_time_watched_actions) AS video_avg_time_watched_actions_video_views,
      `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.AggregateVideoViews`(video_p25_watched_actions) AS video_p25_watched_actions_video_views,
      `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.AggregateVideoViews`(video_p50_watched_actions) AS video_p50_watched_actions_video_views,
      `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.AggregateVideoViews`(video_p75_watched_actions) AS video_p75_watched_actions_video_views,
      `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.AggregateVideoViews`(video_p95_watched_actions) AS video_p95_watched_actions_video_views
    FROM `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.CampaignInsightsByPlatform`
  ),

  GroupedPlatformDetails AS (
    SELECT
      campaign_id,
      report_date,
      ARRAY_AGG(
        STRUCT(
          publisher_platform,
          COALESCE(impressions, 0) AS impressions,
          COALESCE(clicks, 0) AS clicks,
          COALESCE(spend, 0) AS spend,
          COALESCE(reach, 0) AS reach,
          COALESCE(frequency, 0) AS frequency,
          actions.post_engagements,
          actions.page_engagements,
          actions.link_clicks,
          actions.post_shares,
          actions.post_reactions,
          actions.post_saves,
          actions.post_comments,
          actions.video_views,
          actions.page_likes,
          actions.photo_views,
          video_avg_time_watched_actions_video_views,
          video_p25_watched_actions_video_views,
          video_p50_watched_actions_video_views,
          video_p75_watched_actions_video_views,
          video_p95_watched_actions_video_views
        )
      ) AS platform_details
    FROM PlatformDetails
    GROUP BY
      campaign_id,
      report_date
  ),

  CampaignInsights AS (
    SELECT
      campaign_id,
      report_date,
      campaign_name,
      account_id,
      account_name,
      account_currency,
      impressions,
      clicks,
      spend,
      reach,
      frequency,
      `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.AggregateActions`(actions) AS actions,
      `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.AggregateVideoViews`(video_avg_time_watched_actions) AS video_avg_time_watched_actions_video_views,
      `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.AggregateVideoViews`(video_p25_watched_actions) AS video_p25_watched_actions_video_views,
      `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.AggregateVideoViews`(video_p50_watched_actions) AS video_p50_watched_actions_video_views,
      `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.AggregateVideoViews`(video_p75_watched_actions) AS video_p75_watched_actions_video_views,
      `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.AggregateVideoViews`(video_p95_watched_actions) AS video_p95_watched_actions_video_views
    FROM `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.CampaignInsights`
  )

SELECT
  CI.report_date,
  CI.campaign_id,
  CI.campaign_name,
  CI.account_id,
  CI.account_name,
  CI.account_currency,
  AdAccount.timezone_name AS account_timezone_name,
  Campaign.status AS campaign_status,
  Campaign.created_time AS campaign_created_time,
  Campaign.objective AS campaign_objective,
  Campaign.start_time AS campaign_start_time,
  Campaign.stop_time AS campaign_stop_time,
  COALESCE(CI.impressions, 0) AS impressions,
  COALESCE(CI.clicks, 0) AS clicks,
  COALESCE(CI.spend, 0) AS spend,
  COALESCE(CI.frequency, 0) AS frequency,
  COALESCE(CI.reach, 0) AS reach,
  CI.video_avg_time_watched_actions_video_views,
  CI.video_p25_watched_actions_video_views,
  CI.video_p50_watched_actions_video_views,
  CI.video_p75_watched_actions_video_views,
  CI.video_p95_watched_actions_video_views,
  CI.actions.likes,
  CI.actions.loves,
  CI.actions.hahas,
  CI.actions.wows,
  CI.actions.sads,
  CI.actions.angry,
  CI.actions.post_engagements,
  CI.actions.page_engagements,
  CI.actions.link_clicks,
  CI.actions.post_shares,
  CI.actions.post_reactions,
  CI.actions.post_saves,
  CI.actions.post_comments,
  CI.actions.video_views,
  CI.actions.page_likes,
  CI.actions.photo_views,
  GroupedCountryDetails.country_details,
  GroupedPlacementDetails.placement_details,
  GroupedAgeGenderDetails.age_gender_details,
  GroupedPlatformDetails.platform_details
FROM CampaignInsights AS CI
INNER JOIN `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.AdAccount` AS AdAccount
  ON CI.account_id = AdAccount.account_id
INNER JOIN `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.Campaign` AS Campaign
  ON CI.campaign_id = Campaign.campaign_id
LEFT JOIN GroupedCountryDetails
  ON
    CI.campaign_id = GroupedCountryDetails.campaign_id
    AND CI.report_date = GroupedCountryDetails.report_date
LEFT JOIN GroupedPlacementDetails
  ON
    CI.campaign_id = GroupedPlacementDetails.campaign_id
    AND CI.report_date = GroupedPlacementDetails.report_date
LEFT JOIN GroupedAgeGenderDetails
  ON
    CI.campaign_id = GroupedAgeGenderDetails.campaign_id
    AND CI.report_date = GroupedAgeGenderDetails.report_date
LEFT JOIN GroupedPlatformDetails
  ON
    CI.campaign_id = GroupedPlatformDetails.campaign_id
    AND CI.report_date = GroupedPlatformDetails.report_date
