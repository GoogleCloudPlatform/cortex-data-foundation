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

/* Event-related measures at Month level.

Additional dimensions apply depending on the event type.
*/

SELECT
  property_id,
  DATE_TRUNC(event_date, MONTH) AS month_start_date,
  event_params_page_location AS page_location,
  event_params_campaign_id AS campaign_id,
  event_params_campaign_name AS campaign_name,
  event_params_currency_code AS currency_code,
  collected_traffic_source.manual_campaign_id AS collected_traffic_source_manual_campaign_id,
  collected_traffic_source.manual_campaign_name AS collected_traffic_source_manual_campaign_name,
  collected_traffic_source.manual_source AS collected_traffic_source_manual_source,
  collected_traffic_source.manual_medium AS collected_traffic_source_manual_medium,
  traffic_source.name AS traffic_source_name,
  traffic_source.medium AS traffic_source_medium,
  traffic_source.source AS traffic_source_source,
  geo.country AS geo_country,
  event_params_percent_scrolled AS percent_scrolled,
  COUNT(DISTINCT user_pseudo_id) AS num_of_distinct_monthly_users,
  COUNT(DISTINCT IF(event_name = 'user_engagement', user_pseudo_id, NULL)) AS num_of_distinct_monthly_users_engaged,
  COUNT(DISTINCT IF(event_name = 'click', user_pseudo_id, NULL)) AS num_of_distinct_monthly_users_with_click_events,
  COUNT(DISTINCT IF(event_name = 'page_view', user_pseudo_id, NULL)) AS num_of_distinct_monthly_users_with_page_views,
  COUNT(DISTINCT IF(event_name = 'scroll', user_pseudo_id, NULL)) AS num_of_distinct_monthly_users_with_scroll_events,
  COUNT(DISTINCT IF(event_name = 'purchase', user_pseudo_id, NULL)) AS num_of_distinct_monthly_users_with_purchase_events,
  COUNT(DISTINCT IF(event_name = 'view_item', user_pseudo_id, NULL)) AS num_of_distinct_monthly_users_with_view_item_events,
  COUNT(DISTINCT IF(event_name = 'add_to_cart', user_pseudo_id, NULL)) AS num_of_distinct_monthly_users_with_add_to_cart_events,
  COUNT(DISTINCT IF(event_name = 'begin_checkout', user_pseudo_id, NULL)) AS num_of_distinct_monthly_users_with_begin_checkout_events
FROM `{{ project_id_tgt }}.{{ marketing_ga4_datasets_reporting }}.Events`
GROUP BY
  property_id,
  month_start_date,
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
  percent_scrolled
