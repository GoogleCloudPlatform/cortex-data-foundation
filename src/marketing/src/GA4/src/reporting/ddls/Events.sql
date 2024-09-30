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

/* Google Analytics 4 Events Details */

SELECT
  property_id,
  PARSE_DATE('%Y%m%d', event_date) AS event_date,
  TIMESTAMP_MICROS(event_timestamp) AS event_timestamp,
  TIMESTAMP_MICROS(COALESCE(event_previous_timestamp, 0)) AS event_previous_timestamp,
  event_name,
  COALESCE(event_value_in_usd, 0.0) AS event_value_in_usd,
  event_bundle_sequence_id,
  COALESCE(event_server_timestamp_offset, 0) AS event_server_timestamp_offset,
  COALESCE(batch_event_index, 0) AS batch_event_index,
  COALESCE(batch_ordering_id, 0) AS batch_ordering_id,
  COALESCE(batch_page_id, 0) AS batch_page_id,
  (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS event_params_page_location,
  (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS event_params_ga_session_id,
  (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'percent_scrolled') AS event_params_percent_scrolled,
  (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'engagement_time_msec') AS event_params_engagement_time_msec,
  (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'campaign_id') AS event_params_campaign_id,
  (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'campaign') AS event_params_campaign_name,
  (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'gclid') AS event_params_gclid,
  (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'currency') AS event_params_currency_code,
  * EXCEPT (
    property_id,
    event_date,
    event_timestamp,
    event_previous_timestamp,
    event_name,
    event_value_in_usd,
    event_bundle_sequence_id,
    event_server_timestamp_offset,
    batch_event_index,
    batch_ordering_id,
    batch_page_id
  )
FROM
  (
    {% for property in marketing_ga4_datasets_cdc %}
      SELECT
        {{ property.property_id }} AS property_id, *
      -- ## CORTEX-CUSTOMER: If you are using Google Analytics 360 and use "Fresh daily" Export type, replace 'events_*' below with 'events_fresh_*'.
      FROM `{{ project_id_src }}.{{ property.name }}.events_*`
      {% if not loop.last %} UNION ALL {% endif %}
    {% endfor %}
  )
