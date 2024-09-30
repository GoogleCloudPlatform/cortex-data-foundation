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

/* Daily Aggregated Report of Clicks.
*
* This query aggregates clicks on daily basis.
*/

#-- ## EXPERIMENTAL

WITH
  AggregatedClicks AS (
    SELECT
      Clicks.account_id,
      Clicks.dv360_advertiser_id AS advertiser_id,
      Advertisers.advertiser AS advertiser_name,
      Clicks.dv360_campaign_id AS campaign_id,
      Campaigns.campaign AS campaign_name,
      Campaigns.campaign_start_date,
      Campaigns.campaign_end_date,
      Clicks.ad_id,
      Ads.ad AS ad_name,
      Ads.ad_type,
      Clicks.impression_id,
      DATE(TIMESTAMP_MICROS(Clicks.event_time)) AS date,
      Clicks.dv360_insertion_order_id AS insertion_order,
      Clicks.dv360_line_item_id AS line_item_id,
      COALESCE(Clicks.dv360_device_type, Clicks.browser_platform_id) AS device,
      Clicks.dv360_browser_platform_id,
      Clicks.browser_platform_id,
      Browsers.browser_platform,
      Clicks.dv360_site_id AS site_name,
      Clicks.dv360_country_code AS country_code,
      Clicks.state_region,
      States.state_region_full_name AS state_region_name,
      Clicks.dv360_url AS url,
      Clicks.dv360_matching_targeted_segments AS audience_type,
      Clicks.dv360_browser_timezone_offset_minutes AS browser_timezone_offset_minutes,
      (Clicks.event_type = 'CLICK') AS is_click,
      Clicks.dv360_revenue_advertiser_currency AS revenue_advertiser_currency,
      Clicks.dv360_total_media_cost_advertiser_currency AS total_media_cost_advertiser_currency
    FROM `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.Clicks` AS Clicks
    INNER JOIN
      `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.MatchTableAdvertisers`
        AS Advertisers
      ON
        Clicks.account_id = Advertisers.account_id
        AND Clicks.advertiser_id = Advertisers.advertiser_id
    INNER JOIN
      `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.MatchTableCampaigns`
        AS Campaigns
      ON Clicks.campaign_id = Campaigns.campaign_id
    INNER JOIN
      `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.MatchTableAds`
        AS Ads
      ON Clicks.ad_id = Ads.ad_id
    LEFT JOIN
      `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.MatchTableBrowsers`
        AS Browsers
      ON
        Clicks.account_id = Browsers.account_id
        AND Clicks.browser_platform_id = Browsers.browser_platform_id
    LEFT JOIN
      `{{ project_id_tgt }}.{{ marketing_cm360_datasets_reporting }}.MatchTableStates`
        AS States
      ON
        Clicks.account_id = States.account_id
        AND States.state_region = CONCAT(Clicks.dv360_country_code, '-', Clicks.state_region)
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
  COUNTIF(is_click) AS total_clicks,
  SUM(revenue_advertiser_currency) / 1000000000 AS total_revenue_advertiser_currency,
  SUM(total_media_cost_advertiser_currency) / 1000000000 AS total_media_cost_advertiser_currency
FROM
  AggregatedClicks
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
