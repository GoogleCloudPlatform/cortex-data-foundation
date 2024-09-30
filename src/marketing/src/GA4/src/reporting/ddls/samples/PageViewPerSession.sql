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

/* Sample script showing how to calculate total page views at date and session level. */

SELECT
  property_id,
  event_date AS report_date,
  -- NOTE: user_pseudo_id is joined with session_id to get a unique identifier for each session.
  -- See https://support.google.com/analytics/answer/9191807?hl=en
  CONCAT(user_pseudo_id, '; ', event_params_ga_session_id) AS session_id,
  collected_traffic_source.manual_campaign_id AS collected_traffic_source_manual_campaign_id,
  collected_traffic_source.manual_campaign_name AS collected_traffic_source_manual_campaign_name,
  collected_traffic_source.manual_source AS collected_traffic_source_manual_source,
  collected_traffic_source.manual_medium AS collected_traffic_source_manual_medium,
  traffic_source.name AS traffic_source_name,
  traffic_source.medium AS traffic_source_medium,
  traffic_source.source AS traffic_source_source,
  COUNT(event_name) AS num_of_page_views
FROM `{{ project_id_tgt }}.{{ marketing_ga4_datasets_reporting }}.Events`
WHERE
  event_name = 'page_view'
GROUP BY
  property_id,
  report_date,
  session_id,
  collected_traffic_source_manual_campaign_id,
  collected_traffic_source_manual_campaign_name,
  collected_traffic_source_manual_source,
  collected_traffic_source_manual_medium,
  traffic_source_name,
  traffic_source_medium,
  traffic_source_source
