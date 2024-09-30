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

/* Sample script showing how to calculate KPIs at Event level from daily aggregate table.

Note that some of the measures and derived measures are non-additive (e.g. total distinct users for event X), so care needs to be taken when aggregating them.
If weekly / monthly aggregation is desired, replace them with the corresponding measures from the weekly / monthly aggregate tables.
*/

SELECT
  property_id,
  report_date,
  page_location,
  campaign_id,
  campaign_name,
  currency_code,
  collected_traffic_source_manual_campaign_id,
  collected_traffic_source_manual_campaign_name,
  collected_traffic_source_manual_source,
  collected_traffic_source_manual_medium,
  traffic_source_name,
  traffic_source_medium,
  traffic_source_source,
  geo_country,
  percent_scrolled,
  total_user_engagement_time_msec,
  num_of_distinct_daily_users_engaged, -- non-additive metric
  num_of_distinct_daily_users, -- non-additive metric
  num_of_distinct_daily_users_with_click_events, -- non-additive metric
  num_of_distinct_daily_users_with_scroll_events, -- non-additive metric
  num_of_purchases,
  num_of_distinct_daily_users_with_purchase_events, -- non-additive metric
  num_of_pageviews,
  num_of_distinct_daily_users_with_page_views, -- non-additive metric
  num_of_view_items,
  num_of_distinct_daily_users_with_view_item_events, -- non-additive metric
  num_of_add_to_carts,
  num_of_distinct_daily_users_with_add_to_cart_events, -- non-additive metric
  num_of_begin_checkouts,
  num_of_distinct_daily_users_with_begin_checkout_events, -- non-additive metric
  total_items_sold,
  total_revenue,
  SAFE_DIVIDE(num_of_purchases, num_of_distinct_daily_users_with_purchase_events) AS avg_purchases_per_user,
  SAFE_DIVIDE(total_revenue, total_items_sold) AS avg_purchase_price_per_item,
  SAFE_DIVIDE(total_revenue, num_of_purchases) AS avg_purchase_value,
  SAFE_DIVIDE(total_items_sold, num_of_purchases) AS avg_items_per_purchase,
  SAFE_DIVIDE(num_of_distinct_daily_users_with_click_events, num_of_distinct_daily_users_with_page_views) AS page_click_through_rate,
  SAFE_DIVIDE(total_user_engagement_time_msec, num_of_distinct_daily_users_engaged) / 1000 AS avg_time_spent_on_page_in_seconds
FROM `{{ project_id_tgt }}.{{ marketing_ga4_datasets_reporting }}.EventsDailyAgg`
