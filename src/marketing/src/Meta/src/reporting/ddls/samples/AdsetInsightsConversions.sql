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

/* Sample script to show how to use Conversion details for Adset from the daily aggregate table.
   Similar query can be done for conversion_values, purchase_roas and website_purchase_roas.
*/


SELECT
  adset_agg.report_date,
  adset_agg.adset_id,
  adset_agg.account_id,
  adset_agg.campaign_id,
  adset_agg.account_name,
  adset_agg.account_currency,
  adset_agg.account_timezone_name,
  adset_agg.campaign_name,
  adset_agg.adset_name,
  adset_agg.campaign_status,
  adset_agg.campaign_created_time,
  adset_agg.campaign_objective,
  adset_agg.campaign_start_time,
  adset_agg.campaign_stop_time,
  conversion._1d_click AS conversions_1d_click,
  conversion._1d_ev AS conversions_1d_ev,
  conversion._1d_view AS conversions_1d_view,
  conversion._28d_click AS conversions_28d_click,
  conversion._28d_view AS conversions_28d_view,
  conversion._7d_click AS conversions_7d_click,
  conversion._7d_view AS conversions_7d_view,
  conversion.action_canvas_component_name AS conversions_action_canvas_component_name,
  conversion.action_carousel_card_id AS conversions_action_carousel_card_id,
  conversion.action_carousel_card_name AS conversions_action_carousel_card_name,
  conversion.action_destination AS conversions_action_destination,
  conversion.action_device AS conversions_action_device,
  conversion.action_target_id AS conversions_action_target_id,
  conversion.action_type AS conversions_action_type,
  conversion.action_video_sound AS conversions_action_video_sound,
  conversion.action_video_type AS conversions_action_video_type,
  conversion.dda AS conversions_dda,
  conversion.inline AS conversions_inline,
  conversion.value AS conversions_value
FROM `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.AdsetInsightsDailyAgg` AS adset_agg
CROSS JOIN UNNEST(conversions) AS conversion
