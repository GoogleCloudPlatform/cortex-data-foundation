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

/* Sets the null values for an ActionStats array to zeros.
*  If the whole array is empty or null, it creates one element with the default zero values.
*  Returns the updated array.
*/

CREATE OR REPLACE FUNCTION `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.TransformMetaActionStatsDailyAgg`(
  meta_action_stats ARRAY<STRUCT<
    _1d_click INT64,
    _1d_ev INT64,
    _1d_view INT64,
    _28d_click INT64,
    _28d_view INT64,
    _7d_click INT64,
    _7d_view INT64,
    action_canvas_component_name STRING,
    action_carousel_card_id STRING,
    action_carousel_card_name STRING,
    action_destination STRING,
    action_device STRING,
    action_reaction STRING,
    action_target_id STRING,
    action_type STRING,
    action_video_sound STRING,
    action_video_type STRING,
    dda INT64,
    inline INT64,
    value FLOAT64
  >>
)
RETURNS ARRAY<STRUCT<
  _1d_click INT64,
  _1d_ev INT64,
  _1d_view INT64,
  _28d_click INT64,
  _28d_view INT64,
  _7d_click INT64,
  _7d_view INT64,
  action_canvas_component_name STRING,
  action_carousel_card_id STRING,
  action_carousel_card_name STRING,
  action_destination STRING,
  action_device STRING,
  action_target_id STRING,
  action_type STRING,
  action_video_sound STRING,
  action_video_type STRING,
  dda INT64,
  inline INT64,
  value FLOAT64
>>
AS (
  (
    SELECT
      IF(
        meta_action_stats IS NOT NULL AND ARRAY_LENGTH(meta_action_stats) > 0,
        ARRAY_AGG(
          STRUCT(
            COALESCE(_1d_click, 0) AS _1d_click,
            COALESCE(_1d_ev, 0) AS _1d_ev,
            COALESCE(_1d_view, 0) AS _1d_view,
            COALESCE(_28d_click, 0) AS _28d_click,
            COALESCE(_28d_view, 0) AS _28d_view,
            COALESCE(_7d_click, 0) AS _7d_click,
            COALESCE(_7d_view, 0) AS _7d_view,
            action_canvas_component_name,
            action_carousel_card_id,
            action_carousel_card_name,
            action_destination,
            action_device,
            action_target_id,
            action_type,
            action_video_sound,
            action_video_type,
            COALESCE(dda, 0) AS dda,
            COALESCE(inline, 0) AS inline,
            COALESCE(value, 0.0) AS value
          )
        ),
        [
          STRUCT(
            0 AS _1d_click,
            0 AS _1d_ev,
            0 AS _1d_view,
            0 AS _28d_click,
            0 AS _28d_view,
            0 AS _7d_click,
            0 AS _7d_view,
            CAST(NULL AS STRING) AS action_canvas_component_name,
            CAST(NULL AS STRING) AS action_carousel_card_id,
            CAST(NULL AS STRING) AS action_carousel_card_name,
            CAST(NULL AS STRING) AS action_destination,
            CAST(NULL AS STRING) AS action_device,
            CAST(NULL AS STRING) AS action_target_id,
            CAST(NULL AS STRING) AS action_type,
            CAST(NULL AS STRING) AS action_video_sound,
            CAST(NULL AS STRING) AS action_video_type,
            0 AS dda,
            0 AS inline,
            0.0 AS value
          )
        ]
      )
    FROM
      UNNEST(meta_action_stats)
  )
);
