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

/* Returns sum of video views from the video_watched_actions array. */

CREATE OR REPLACE FUNCTION `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.AggregateVideoViews`(
  video_watched_actions ARRAY<STRUCT<
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
RETURNS FLOAT64
AS (
  COALESCE(
    (
      SELECT SUM(IF(action_type = 'video_view', value, 0))
      FROM UNNEST(video_watched_actions)
    ),
    0
  )
);
