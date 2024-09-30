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


/* Converts Meta APIs "ads action stats" type JSON Array to BQ array of struct. */


CREATE OR REPLACE FUNCTION `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.TransformMetaActionStats`(input_json_array JSON)
AS (
  ARRAY(
    SELECT
      STRUCT(
        LAX_INT64(array_row.1d_click) AS _1d_click,
        LAX_INT64(array_row.1d_ev) AS _1d_ev,
        LAX_INT64(array_row.1d_view) AS _1d_view,
        LAX_INT64(array_row.28d_click) AS _28d_click,
        LAX_INT64(array_row.28d_view) AS _28d_view,
        LAX_INT64(array_row.7d_click) AS _7d_click,
        LAX_INT64(array_row.7d_view) AS _7d_view,
        LAX_STRING(array_row.action_canvas_component_name) AS action_canvas_component_name,
        LAX_STRING(array_row.action_carousel_card_id) AS action_carousel_card_id,
        LAX_STRING(array_row.action_carousel_card_name) AS action_carousel_card_name,
        LAX_STRING(array_row.action_destination) AS action_destination,
        LAX_STRING(array_row.action_device) AS action_device,
        LAX_STRING(array_row.action_reaction) AS action_reaction,
        LAX_STRING(array_row.action_target_id) AS action_target_id,
        LAX_STRING(array_row.action_type) AS action_type,
        LAX_STRING(array_row.action_video_sound) AS action_video_sound,
        LAX_STRING(array_row.action_video_type) AS action_video_type,
        LAX_INT64(array_row.dda) AS dda,
        LAX_INT64(array_row.inline) AS inline,
        LAX_FLOAT64(array_row.value) AS value
      )
    FROM UNNEST(JSON_QUERY_ARRAY(input_json_array)) AS array_row
  )
);
