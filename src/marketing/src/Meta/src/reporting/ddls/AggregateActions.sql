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

/* Returns a structure which contains the sum of actions and reactions from the actions array. */

CREATE OR REPLACE FUNCTION `{{ project_id_tgt }}.{{ marketing_meta_datasets_reporting }}.AggregateActions`(
  actions ARRAY<STRUCT<
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
AS (
  (
    SELECT AS STRUCT
      COALESCE(SUM(IF(action_type = 'post_engagement', CAST(value AS INT64), 0)), 0) AS post_engagements,
      COALESCE(SUM(IF(action_type = 'page_engagement', CAST(value AS INT64), 0)), 0) AS page_engagements,
      COALESCE(SUM(IF(action_type = 'link_click', CAST(value AS INT64), 0)), 0) AS link_clicks,
      COALESCE(SUM(IF(action_type = 'post', CAST(value AS INT64), 0)), 0) AS post_shares,
      COALESCE(SUM(IF(action_type = 'post_reaction' AND action_reaction IS NULL, CAST(value AS INT64), 0)), 0) AS post_reactions,
      COALESCE(SUM(IF(action_type = 'onsite_conversion.post_save', CAST(value AS INT64), 0)), 0) AS post_saves,
      COALESCE(SUM(IF(action_type = 'comment', CAST(value AS INT64), 0)), 0) AS post_comments,
      COALESCE(SUM(IF(action_type = 'video_view', CAST(value AS INT64), 0)), 0) AS video_views,
      COALESCE(SUM(IF(action_type = 'like', CAST(value AS INT64), 0)), 0) AS page_likes,
      COALESCE(SUM(IF(action_type = 'photo_view', CAST(value AS INT64), 0)), 0) AS photo_views,
      COALESCE(SUM(IF(action_reaction = 'like', CAST(value AS INT64), 0)), 0) AS likes,
      COALESCE(SUM(IF(action_reaction = 'love', CAST(value AS INT64), 0)), 0) AS loves,
      COALESCE(SUM(IF(action_reaction = 'haha', CAST(value AS INT64), 0)), 0) AS hahas,
      COALESCE(SUM(IF(action_reaction = 'wow', CAST(value AS INT64), 0)), 0) AS wows,
      COALESCE(SUM(IF(action_reaction = 'sad', CAST(value AS INT64), 0)), 0) AS sads,
      COALESCE(SUM(IF(action_reaction = 'angry', CAST(value AS INT64), 0)), 0) AS angry
    FROM
      UNNEST(actions)
  )
);
