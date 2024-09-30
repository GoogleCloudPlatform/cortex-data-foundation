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

/* Line Item Insights breakdown by Country, Device, Browser, Environment at Date level. */

SELECT
  date,
  line_item_id,
  device_type,
  browser,
  environment,
  country_code,
  partner_id,
  advertiser_id,
  campaign_id,
  insertion_order_id,
  partner,
  partner_currency,
  advertiser,
  advertiser_currency,
  campaign,
  insertion_order,
  line_item,
  line_item_type,
  line_item_start_date,
  line_item_end_date,
  impressions,
  clicks,
  revenue_usd,
  engagements,
  revenue_partner_currency,
  revenue_advertiser_currency,
  youtube_views
FROM `{{ project_id_src }}.{{ marketing_dv360_datasets_cdc }}.lineitem_insights`
