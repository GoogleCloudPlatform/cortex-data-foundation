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

/* Aggregated table at Ad and date levels, with additional breakdowns by country, age / gender, placement, and platform. */

WITH
  CountryDetails AS (
    SELECT
      ad_id,
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
    FROM `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.AdInsightsByCountry`
  ),

  GroupedCountryDetails AS (
    SELECT
      ad_id,
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
      ad_id,
      report_date
  ),

  PlacementDetails AS (
    SELECT
      ad_id,
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
      `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.AdInsightsByPlacement`
  ),

  GroupedPlacementDetails AS (
    SELECT
      ad_id,
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
      ad_id,
      report_date
  ),

  AgeGenderDetails AS (
    SELECT
      ad_id,
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
      `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.AdInsightsByAgeAndGender`
  ),

  GroupedAgeGenderDetails AS (
    SELECT
      ad_id,
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
      ad_id,
      report_date
  ),

  PlatformDetails AS (
    SELECT
      ad_id,
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
    FROM `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.AdInsightsByPlatform`
  ),

  GroupedPlatformDetails AS (
    SELECT
      ad_id,
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
      ad_id,
      report_date
  ),

  AdInsights AS (
    SELECT
      report_date,
      ad_id,
      campaign_id,
      account_id,
      account_name,
      adset_id,
      adset_name,
      ad_name,
      campaign_name,
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
    FROM `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.AdInsights`
  )

SELECT
  AI.report_date,
  AI.ad_id,
  AI.campaign_id,
  AI.account_id,
  AI.account_name,
  AI.adset_id,
  AI.adset_name,
  AI.ad_name,
  Ads.creative.id AS creative_id,
  Ads.creative.name AS creative_name,
  AdAccount.currency AS account_currency,
  AdAccount.timezone_name AS account_timezone_name,
  AI.campaign_name,
  Campaign.status AS campaign_status,
  Campaign.created_time AS campaign_created_time,
  Campaign.objective AS campaign_objective,
  Campaign.start_time AS campaign_start_time,
  Campaign.stop_time AS campaign_stop_time,
  COALESCE(AI.impressions, 0) AS impressions,
  COALESCE(AI.clicks, 0) AS clicks,
  COALESCE(AI.spend, 0) AS spend,
  COALESCE(AI.frequency, 0) AS frequency,
  COALESCE(AI.reach, 0) AS reach,
  AI.video_avg_time_watched_actions_video_views,
  AI.video_p25_watched_actions_video_views,
  AI.video_p50_watched_actions_video_views,
  AI.video_p75_watched_actions_video_views,
  AI.video_p95_watched_actions_video_views,
  AI.actions.likes,
  AI.actions.loves,
  AI.actions.hahas,
  AI.actions.wows,
  AI.actions.sads,
  AI.actions.angry,
  AI.actions.post_engagements,
  AI.actions.page_engagements,
  AI.actions.link_clicks,
  AI.actions.post_shares,
  AI.actions.post_reactions,
  AI.actions.post_saves,
  AI.actions.post_comments,
  AI.actions.video_views,
  AI.actions.page_likes,
  AI.actions.photo_views,
  GroupedCountryDetails.country_details,
  GroupedPlacementDetails.placement_details,
  GroupedAgeGenderDetails.age_gender_details,
  GroupedPlatformDetails.platform_details
FROM AdInsights AS AI
INNER JOIN `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.AdAccount` AS AdAccount
  ON AI.account_id = AdAccount.account_id
INNER JOIN `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.Campaign` AS Campaign
  ON AI.campaign_id = Campaign.campaign_id
INNER JOIN `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.Adset` AS Adset
  ON AI.adset_id = Adset.adset_id
INNER JOIN `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.Ads` AS Ads
  ON AI.ad_id = Ads.ad_id
LEFT JOIN GroupedCountryDetails
  ON
    AI.ad_id = GroupedCountryDetails.ad_id
    AND AI.report_date = GroupedCountryDetails.report_date
LEFT JOIN GroupedPlacementDetails
  ON
    AI.ad_id = GroupedPlacementDetails.ad_id
    AND AI.report_date = GroupedPlacementDetails.report_date
LEFT JOIN GroupedAgeGenderDetails
  ON
    AI.ad_id = GroupedAgeGenderDetails.ad_id
    AND AI.report_date = GroupedAgeGenderDetails.report_date
LEFT JOIN GroupedPlatformDetails
  ON
    AI.ad_id = GroupedPlatformDetails.ad_id
    AND AI.report_date = GroupedPlatformDetails.report_date
