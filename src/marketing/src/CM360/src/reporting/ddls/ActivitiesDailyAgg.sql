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

/* Daily Aggregated Report of Activities.
*
* This query aggregates activities on daily basis.
*/

#-- ## EXPERIMENTAL

WITH
  AggregatedActivities AS (
    SELECT
      Activities.account_id,
      Activities.dv360_advertiser_id AS advertiser_id,
      Advertisers.advertiser AS advertiser_name,
      Activities.dv360_campaign_id AS campaign_id,
      Campaigns.campaign AS campaign_name,
      Campaigns.campaign_start_date,
      Campaigns.campaign_end_date,
      Activities.ad_id,
      Ads.ad AS ad_name,
      Ads.ad_type,
      DATE(TIMESTAMP_MICROS(Activities.event_time)) AS date,
      Activities.dv360_insertion_order_id AS insertion_order,
      Activities.dv360_line_item_id AS line_item_id,
      COALESCE(Activities.dv360_device_type, Activities.browser_platform_id) AS device,
      Activities.dv360_browser_platform_id,
      Activities.browser_platform_id,
      Browsers.browser_platform,
      Activities.dv360_site_id AS site_name,
      Activities.dv360_country_code AS country_code,
      Activities.state_region,
      States.state_region_full_name AS state_region_name,
      Activities.dv360_url AS url,
      Activities.dv360_matching_targeted_segments AS audience_type,
      Activities.dv360_browser_timezone_offset_minutes AS browser_timezone_offset_minutes,
      (Activities.event_type = 'CONVERSION' AND Activities.event_subtype = 'POSTCLICK')
      AS is_post_click_conversion, -- noqa: L003
      (Activities.event_type = 'CONVERSION' AND Activities.event_subtype = 'POSTVIEW')
      AS is_post_view_conversion, -- noqa: L003
      Activities.dv360_revenue_advertiser_currency AS revenue_advertiser_currency,
      Activities.dv360_total_media_cost_advertiser_currency AS total_media_cost_advertiser_currency
    FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.Activities` AS Activities
    INNER JOIN
      `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.MatchTableAdvertisers`
        AS Advertisers
      ON
        Activities.account_id = Advertisers.account_id
        AND Activities.advertiser_id = Advertisers.advertiser_id
    INNER JOIN
      `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.MatchTableCampaigns`
        AS Campaigns
      ON Activities.campaign_id = Campaigns.campaign_id
    INNER JOIN
      `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.MatchTableAds`
        AS Ads
      ON Activities.ad_id = Ads.ad_id
    LEFT JOIN
      `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.MatchTableBrowsers`
        AS Browsers
      ON
        Activities.account_id = Browsers.account_id
        AND Activities.browser_platform_id = Browsers.browser_platform_id
    LEFT JOIN
      `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.MatchTableStates`
        AS States
      ON
        Activities.account_id = States.account_id
        AND States.state_region = CONCAT(Activities.dv360_country_code, '-', Activities.state_region)
  )
SELECT
  account_id,
  advertiser_id,
  advertiser_name,
  campaign_id,
  campaign_name,
  campaign_start_date,
  campaign_end_date,
  ad_id,
  ad_name,
  ad_type,
  date,
  insertion_order,
  line_item_id,
  device,
  dv360_browser_platform_id,
  browser_platform_id,
  browser_platform,
  site_name,
  country_code,
  state_region,
  state_region_name,
  url,
  audience_type,
  browser_timezone_offset_minutes,
  COUNTIF(is_post_click_conversion) AS total_post_click_conversions,
  COUNTIF(is_post_view_conversion) AS total_post_view_conversions,
  COUNTIF(is_post_click_conversion) + COUNTIF(is_post_view_conversion) AS total_conversions,
  SUM(revenue_advertiser_currency) / 1000000000 AS total_revenue_advertiser_currency,
  SUM(total_media_cost_advertiser_currency) / 1000000000 AS total_media_cost_advertiser_currency
FROM
  AggregatedActivities
GROUP BY
  account_id,
  advertiser_id,
  advertiser_name,
  campaign_id,
  campaign_name,
  campaign_start_date,
  campaign_end_date,
  ad_id,
  ad_name,
  ad_type,
  date,
  insertion_order,
  line_item_id,
  device,
  dv360_browser_platform_id,
  browser_platform_id,
  browser_platform,
  site_name,
  country_code,
  state_region,
  state_region_name,
  url,
  audience_type,
  browser_timezone_offset_minutes
