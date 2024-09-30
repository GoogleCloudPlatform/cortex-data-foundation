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

/* Adgroup Insights breakdown by Age & Gender at Date level. */

SELECT
  Insights.date,
  Insights.youtube_ad_group_id,
  Insights.youtube_age,
  Insights.gender,
  Insights.line_item_id,
  Details.campaign_id,
  Insights.partner_id,
  Insights.advertiser_id,
  Insights.insertion_order_id,
  Details.campaign,
  Insights.partner,
  Insights.partner_currency,
  Insights.advertiser,
  Insights.advertiser_currency,
  Insights.insertion_order,
  Insights.line_item,
  Details.line_item_type,
  Insights.trueview_ad_group,
  Insights.revenue_usd,
  Insights.impressions,
  Insights.clicks,
  Insights.youtube_engagements,
  Insights.revenue_partner_currency,
  Insights.revenue_advertiser_currency,
  Insights.youtube_views
FROM `{{ project_id_src }}.{{ marketing_dv360_datasets_cdc }}.adgroup_insights_by_age_gender` AS Insights
INNER JOIN `{{ project_id_src }}.{{ marketing_dv360_datasets_cdc }}.lineitem_details` AS Details
  ON
    Insights.date = Details.date
    AND Insights.line_item_id = Details.line_item_id
