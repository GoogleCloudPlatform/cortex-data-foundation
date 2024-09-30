--  Copyright 2024 Google Inc.

--  Licensed under the Apache License, Version 2.0 (the "License");
--  you may not use this file except in compliance with the License.
--  You may obtain a copy of the License at

--      http://www.apache.org/licenses/LICENSE-2.0

--  Unless required by applicable law or agreed to in writing, software
--  distributed under the License is distributed on an "AS IS" BASIS,
--  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
--  See the License for the specific language governing permissions and
--  limitations under the License.

--  Merging RAW table to CDC based ON given row identifier columns and
--  RecordStamp.
--  Higher or equal RecordStamp indicates that values should be updated.
--  Otherwise new records are inserted into CDC table.


-- CDC Logic
-- Deduplicating source data based on processing time (RecordStamp).

MERGE `{{ target_project_id }}.{{ target_ds }}.{{ target_table }}` AS T
USING (
  WITH
    `NewDataSinceLatestLoad` AS (
      SELECT
        -- Cast RAW layer fields.
        {% for column, target_data_type in sfmc_column_type_mapping.items() -%}
        {%- if column in system_fields.keys() -%}
          {{ column }}
        {%- elif  target_data_type == 'TIMESTAMP' -%}
          SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E*S', {{ column }}, '{{ source_timezone }}') AS {{ column }}
        {%- elif target_data_type == 'BOOL' -%}
          COALESCE(SAFE_CAST(SAFE_CAST({{ column }} AS INT64) AS BOOLEAN), SAFE_CAST({{ column }} AS BOOLEAN)) AS {{ column }}
        {%- else -%}
          SAFE_CAST({{ column }} AS {{ target_data_type }}) AS {{ column }}
        {%- endif -%}
        {%- if not loop.last -%},
        {% endif %}
        {%- endfor %}
      FROM `{{ source_project_id }}.{{ source_ds }}.{{ source_table }}`
      WHERE
        `RecordStamp` >= (
          SELECT COALESCE(MAX(`RecordStamp`), TIMESTAMP('1970-01-01 00:00:00+00'))
          FROM `{{ target_project_id }}.{{ target_ds }}.{{ target_table }}`)
        -- Validate row identifier values are not null to avoid inconsistency in CDC logic.
        {%- for identifier in row_identifiers %}
        AND `{{ identifier }}` IS NOT NULL
        {%- endfor %}
    )
  SELECT *
  FROM `NewDataSinceLatestLoad`
  QUALIFY ROW_NUMBER() OVER (
      PARTITION BY
        {%- for identifier in row_identifiers %}
        `{{ identifier }}`
        {%- if not loop.last +%}, {%- endif -%}
        {%- endfor %}
      ORDER BY `RecordStamp` DESC) = 1 -- Order to get latest values for deduplication.
) AS S
ON (
  {%- for identifier in row_identifiers -%}
  {% if not loop.first +%} AND {%+ endif %}T.`{{ identifier }}` = S.`{{ identifier }}`
  {%- endfor -%}
)
WHEN MATCHED AND S.`RecordStamp` >= T.`RecordStamp`
-- Update when data exists already.
THEN
  UPDATE SET
      {%- for column in columns %}
      `{{ column }}` = S.`{{ column }}`
        {%- if not loop.last -%}
            ,
        {%- endif -%}
      {%- endfor %}
WHEN NOT MATCHED BY TARGET
-- Insert when data is not presented in target table.
THEN
  INSERT (
      {%- for column in columns %}
      `{{ column }}`
        {%- if not loop.last -%}
            ,
        {%- endif -%}
      {%- endfor %}
    )
    VALUES (
      {%- for column in columns %}
      S.`{{ column }}`
        {%- if not loop.last -%}
            ,
        {%- endif -%}
      {%- endfor %}
    );
