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

/* Adset measures at Adset, date and country level. */


SELECT
  report_date,
  adset_id,
  country,
  created_time AS created_date,
  updated_time AS updated_date,
  adset_name,
  account_id,
  account_name,
  campaign_id,
  campaign_name,
  `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.TransformMetaActionStats`(action_values)
    AS action_values,
  `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.TransformMetaActionStats`(actions)
    AS actions,
  `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.TransformMetaActionStats`(ad_click_actions)
    AS ad_click_actions,
  `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.TransformMetaActionStats`(ad_impression_actions)
    AS ad_impression_actions,
  `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.TransformMetaActionStats`(catalog_segment_actions)
    AS catalog_segment_actions,
  `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.TransformMetaActionStats`(catalog_segment_value)
    AS catalog_segment_value,
  `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.TransformMetaActionStats`(catalog_segment_value_mobile_purchase_roas)
    AS catalog_segment_value_mobile_purchase_roas,
  `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.TransformMetaActionStats`(catalog_segment_value_omni_purchase_roas)
    AS catalog_segment_value_omni_purchase_roas,
  `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.TransformMetaActionStats`(catalog_segment_value_website_purchase_roas)
    AS catalog_segment_value_website_purchase_roas,
  `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.TransformMetaActionStats`(conversion_values)
    AS conversion_values,
  `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.TransformMetaActionStats`(conversions)
    AS conversions,
  `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.TransformMetaActionStats`(converted_product_quantity)
    AS converted_product_quantity,
  `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.TransformMetaActionStats`(converted_product_value)
    AS converted_product_value,
  `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.TransformMetaActionStats`(cost_per_15_sec_video_view)
    AS cost_per_15_sec_video_view,
  `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.TransformMetaActionStats`(cost_per_2_sec_continuous_video_view)
    AS cost_per_2_sec_continuous_video_view,
  `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.TransformMetaActionStats`(cost_per_action_type)
    AS cost_per_action_type,
  `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.TransformMetaActionStats`(cost_per_ad_click)
    AS cost_per_ad_click,
  `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.TransformMetaActionStats`(cost_per_conversion)
    AS cost_per_conversion,
  `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.TransformMetaActionStats`(cost_per_one_thousand_ad_impression)
    AS cost_per_one_thousand_ad_impression,
  `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.TransformMetaActionStats`(cost_per_outbound_click)
    AS cost_per_outbound_click,
  `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.TransformMetaActionStats`(cost_per_thruplay)
    AS cost_per_thruplay,
  `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.TransformMetaActionStats`(cost_per_unique_action_type)
    AS cost_per_unique_action_type,
  `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.TransformMetaActionStats`(cost_per_unique_outbound_click)
    AS cost_per_unique_outbound_click,
  `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.TransformMetaActionStats`(instant_experience_outbound_clicks)
    AS instant_experience_outbound_clicks,
  `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.TransformMetaActionStats`(interactive_component_tap)
    AS interactive_component_tap,
  `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.TransformMetaActionStats`(mobile_app_purchase_roas)
    AS mobile_app_purchase_roas,
  `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.TransformMetaActionStats`(outbound_clicks)
    AS outbound_clicks,
  `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.TransformMetaActionStats`(outbound_clicks_ctr)
    AS outbound_clicks_ctr,
  `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.TransformMetaActionStats`(purchase_roas)
    AS purchase_roas,
  `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.TransformMetaActionStats`(video_30_sec_watched_actions)
    AS video_30_sec_watched_actions,
  `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.TransformMetaActionStats`(video_avg_time_watched_actions)
    AS video_avg_time_watched_actions,
  `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.TransformMetaActionStats`(video_continuous_2_sec_watched_actions)
    AS video_continuous_2_sec_watched_actions,
  `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.TransformMetaActionStats`(video_p100_watched_actions)
    AS video_p100_watched_actions,
  `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.TransformMetaActionStats`(video_p25_watched_actions)
    AS video_p25_watched_actions,
  `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.TransformMetaActionStats`(video_p50_watched_actions)
    AS video_p50_watched_actions,
  `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.TransformMetaActionStats`(video_p75_watched_actions)
    AS video_p75_watched_actions,
  `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.TransformMetaActionStats`(video_p95_watched_actions)
    AS video_p95_watched_actions,
  `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.TransformMetaActionStats`(video_play_actions)
    AS video_play_actions,
  `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.TransformMetaActionStats`(video_time_watched_actions)
    AS video_time_watched_actions,
  `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.TransformMetaActionStats`(website_ctr)
    AS website_ctr,
  `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.TransformMetaActionStats`(website_purchase_roas)
    AS website_purchase_roas,
  * EXCEPT (
    report_date,
    adset_id,
    country,
    created_time,
    updated_time,
    adset_name,
    account_id,
    account_name,
    campaign_id,
    campaign_name,
    ad_id,
    ad_name,
    actions,
    recordstamp,
    action_values,
    ad_click_actions,
    ad_impression_actions,
    catalog_segment_actions,
    catalog_segment_value,
    catalog_segment_value_mobile_purchase_roas,
    catalog_segment_value_omni_purchase_roas,
    catalog_segment_value_website_purchase_roas,
    conversion_values,
    conversions,
    converted_product_quantity,
    converted_product_value,
    cost_per_15_sec_video_view,
    cost_per_2_sec_continuous_video_view,
    cost_per_action_type,
    cost_per_ad_click,
    cost_per_conversion,
    cost_per_one_thousand_ad_impression,
    cost_per_outbound_click,
    cost_per_thruplay,
    cost_per_unique_action_type,
    cost_per_unique_outbound_click,
    instant_experience_outbound_clicks,
    interactive_component_tap,
    mobile_app_purchase_roas,
    outbound_clicks,
    outbound_clicks_ctr,
    purchase_roas,
    video_30_sec_watched_actions,
    video_avg_time_watched_actions,
    video_continuous_2_sec_watched_actions,
    video_p100_watched_actions,
    video_p25_watched_actions,
    video_p50_watched_actions,
    video_p75_watched_actions,
    video_p95_watched_actions,
    video_play_actions,
    video_time_watched_actions,
    website_ctr,
    website_purchase_roas
  )
FROM `{{ project_id_src }}.{{ marketing_meta_datasets_cdc }}.adset_insights_by_country`
