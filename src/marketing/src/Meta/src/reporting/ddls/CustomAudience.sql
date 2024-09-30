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

/* Custom Audience dimension details. */


SELECT
  id AS custom_audience_id,
  ARRAY(
    SELECT LAX_INT64(lookalike_audience_id)
    FROM UNNEST(JSON_QUERY_ARRAY(lookalike_audience_ids, '$')) AS lookalike_audience_id)
      AS lookalike_audience_ids, --noqa:LT02
  STRUCT(
    LAX_STRING(lookalike_spec.type) AS type,
    LAX_FLOAT64(lookalike_spec.starting_ratio) AS starting_ratio,
    LAX_FLOAT64(lookalike_spec.ratio) AS ratio,
    LAX_STRING(lookalike_spec.country) AS country,
    ARRAY(
      SELECT
        STRUCT(
          LAX_INT64(origin.id) AS id,
          LAX_STRING(origin.name) AS name,
          LAX_STRING(origin.type) AS type
        )
      FROM UNNEST(JSON_QUERY_ARRAY(lookalike_spec.origin)) AS origin
    ) AS origin,
    ARRAY(
      SELECT LAX_STRING(target_countries)
      FROM UNNEST(JSON_QUERY_ARRAY(lookalike_spec.target_countries)) AS target_countries
    ) AS target_countries
  ) AS lookalike_spec,
  * EXCEPT (id, lookalike_audience_ids, lookalike_spec, recordstamp)
FROM `{{ project_id_src }}.{{ marketing_meta_datasets_cdc }}.custom_audience`
