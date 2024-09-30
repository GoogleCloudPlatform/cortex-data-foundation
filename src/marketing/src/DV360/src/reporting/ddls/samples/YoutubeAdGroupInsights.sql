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

/* This is a sample SQL script showing how to aggregate and report measures from AdInsights table
   at date and adgroup level.
*/

SELECT
  date AS report_date,
  youtube_ad_group_id,
  trueview_ad_group AS trueview_ad_group_name,
  line_item_id,
  campaign_id,
  partner_id,
  advertiser_id,
  insertion_order_id,
  campaign AS campaign_name,
  partner AS partner_name,
  advertiser AS advertiser_name,
  insertion_order AS insertion_order_name,
  line_item AS line_item_name,
  -- Use 'partner_currency' and 'revenue_partner_currency' columns if you want to use revenue
  -- in partner currencies.
  -- Similarly, for revenue in advertiser currency, use 'advertiser_currency' and
  -- 'revenue_advertiser_currency' columns.
  SUM(revenue_usd) AS total_revenue_usd,
  SUM(impressions) AS total_impressions,
  SUM(clicks) AS total_clicks,
  SUM(youtube_engagements) AS total_youtube_engagements,
  SUM(youtube_views) AS total_youtube_views,
  SAFE_DIVIDE(SUM(revenue_usd), SUM(impressions)) * 1000 AS cpm,
  SAFE_DIVIDE(SUM(clicks), SUM(impressions)) * 100 AS ctr,
  SAFE_DIVIDE(SUM(revenue_usd), SUM(clicks)) AS cpc,
  SAFE_DIVIDE(SUM(youtube_views), SUM(impressions)) * 100 AS vr,
  SAFE_DIVIDE(SUM(revenue_usd), SUM(youtube_views)) AS cpv
FROM `{{ project_id_tgt }}.{{ marketing_dv360_datasets_reporting }}.AdInsights`
WHERE line_item_type IN ('YouTube & partners', 'Demand Generation')
GROUP BY
  report_date,
  youtube_ad_group_id,
  trueview_ad_group_name,
  line_item_id,
  campaign_id,
  partner_id,
  advertiser_id,
  insertion_order_id,
  campaign_name,
  partner_name,
  advertiser_name,
  insertion_order_name,
  line_item_name
