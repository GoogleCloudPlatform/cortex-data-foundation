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
--  recordstamp.
--  Higher or equal recordstamp indicates that values should be updated.
--  Otherwise new records are inserted into CDC table.


-- CDC Logic
-- Deduplicating source data based on processing time (recordstamp).


MERGE `{{ target_project_id }}.{{ target_ds }}.{{ target_table }}` AS T
USING (
  WITH
    `NewDataSinceLatestLoad` AS (
      SELECT
        {%- for name, data_type in meta_field_type_mapping.items() %}
        {%- if data_type=='JSON' %}
        PARSE_JSON({{ name }}) AS {{ name }}
        {%- elif data_type=='TIMESTAMP' and name != 'recordstamp' %}
        PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S%z', TRIM({{ name }}, '\"')) AS {{ name }}
        {%- elif  data_type =='DATETIME' %}
        DATETIME(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S%z', TRIM({{ name }}, '\"'))) AS {{ name }}
        {%- elif data_type=='DATE' and name != 'report_date' %}
        PARSE_DATE('%F', TRIM({{ name }}, '\"')) AS {{ name }}
        {%- elif data_type in ('INT64', 'FLOAT64', 'BOOL', 'STRING') %}
        CAST(TRIM({{ name }}, '\"') AS {{ data_type }}) AS {{ name }}
        {%- else %}
        -- This condition branch is only for the system fields report_date and recordstamp.
        {{ name }}
      {%- endif -%}
      {% if not loop.last %}, {% endif %}
      {%- endfor %}
      FROM `{{ source_project_id }}.{{ source_ds }}.{{ source_table }}`
      WHERE
        `recordstamp` >= (
          SELECT COALESCE(MAX(`recordstamp`), TIMESTAMP('1970-01-01 00:00:00+00'))
          FROM `{{ target_project_id }}.{{ target_ds }}.{{ target_table }}`)
        -- Filter out rows with null row identifier values to avoid inconsistency in CDC logic.
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
      ORDER BY `recordstamp` DESC) = 1 -- Order to get latest values for deduplication.
) AS S
ON (
  {%- for identifier in row_identifiers -%}
  {% if not loop.first +%} AND {%- endif %} T.`{{ identifier }}` = S.`{{ identifier }}`
  {%- endfor -%}
)
WHEN MATCHED AND S.`recordstamp` >= T.`recordstamp`
-- Update when data already exists.
THEN
  UPDATE SET
    {%- for column in meta_field_type_mapping.keys() %}
    `{{ column }}` = S.`{{ column }}`
      {%- if not loop.last -%}
          ,
      {%- endif -%}
    {%- endfor %}
WHEN NOT MATCHED BY TARGET
-- Insert when data is not present in the target table.
THEN
  INSERT (
    {%- for column in meta_field_type_mapping.keys() %}
    `{{ column }}`
      {%- if not loop.last -%}
          ,
      {%- endif -%}
    {%- endfor %}
  )
  VALUES (
    {%- for column in meta_field_type_mapping.keys() %}
    S.`{{ column }}`
      {%- if not loop.last -%}
          ,
      {%- endif -%}
    {%- endfor %}
  );
