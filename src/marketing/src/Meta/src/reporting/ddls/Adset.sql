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

/* AdSet dimension details. */


WITH
  -- Get all custom audience details at adset level
  CustomAudience AS (
    SELECT
      id,
      ARRAY_AGG(
        STRUCT(
          custom_audience_id AS id,
          custom_audience_name AS name,
          custom_audience_subtype AS subtype
        )
      ) AS custom_audiences
    FROM (
      SELECT
        adset.id,
        LAX_INT64(target_custom_audience.id) AS custom_audience_id,
        LAX_STRING(target_custom_audience.name) AS custom_audience_name,
        CASE
          WHEN custom_audience.subtype IN ('CUSTOM', 'ENGAGEMENT')
            AND custom_audience.lookalike_spec IS NULL
            THEN 'CUSTOM'
          WHEN custom_audience.subtype = 'LOOKALIKE'
            AND custom_audience.lookalike_spec IS NOT NULL
            THEN 'LOOKALIKE'
          ELSE NULL
        END AS custom_audience_subtype
      FROM `{{ project_id_src }}.{{ marketing_meta_datasets_cdc }}.adset` AS adset
      CROSS JOIN UNNEST(JSON_EXTRACT_ARRAY(adset.targeting.custom_audiences)) AS target_custom_audience
      LEFT OUTER JOIN `{{ project_id_src }}.{{ marketing_meta_datasets_cdc }}.custom_audience` AS custom_audience
        ON LAX_INT64(target_custom_audience.id) = custom_audience.id
    )
    GROUP BY id
  )

SELECT
  Adset.id AS adset_id,
  -- Bring all audience types together
  ARRAY_CONCAT(
    COALESCE(
      ARRAY(
        SELECT AS STRUCT
          LAX_INT64(interests.id) AS id,
          LAX_STRING(interests.name) AS name,
          'INTERESTS' AS subtype
        FROM UNNEST(JSON_QUERY_ARRAY(Adset.targeting.flexible_spec)) AS flex_spec,
          UNNEST(JSON_QUERY_ARRAY(flex_spec.interests)) AS interests
        WHERE interests IS NOT NULL
      ),
      []
    ),
    COALESCE(CustomAudience.custom_audiences, [])
  ) AS targeting_audiences,
  Adset.* EXCEPT (id, recordstamp)
FROM `{{ project_id_src }}.{{ marketing_meta_datasets_cdc }}.adset` AS Adset
LEFT OUTER JOIN CustomAudience ON CustomAudience.id = Adset.id
