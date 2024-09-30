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

/* Merging RAW table to CDC based ON given row identifier columns and
 * recordstamp.
 * Higher or equal recordstamp indicates that values should be updated.
 * Otherwise new records are inserted into CDC table.
 * account_id is extracted from source_file_name and stored into CDC table.

 * CDC Logic
 * Deduplicating source data based on the source_file_name and processing time (recordstamp).
 */

MERGE `{{ target_project_id }}.{{ target_ds }}.{{ table }}` AS T
USING (
  WITH `NewDataSinceLatestLoad` AS (
    SELECT *,
      CAST(REGEXP_EXTRACT(`source_file_name`, r'account(\d{1,})_') AS INTEGER) AS `account_id`
    FROM `{{ source_project_id }}.{{ source_ds }}.{{ table }}`
    WHERE `recordstamp` >= (
      SELECT COALESCE(MAX(`recordstamp`), TIMESTAMP('1970-01-01 00:00:00+00'))
      FROM `{{ target_project_id }}.{{ target_ds }}.{{ table }}`)
      -- Validate row identifier values are not null to avoid inconsistency in CDC logic.
      {%- for identifier in row_identifiers if identifier != 'account_id' %}
      -- TODO investigate why row_identifiers could be NULL.
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
      ORDER BY `source_file_name` DESC, `recordstamp` DESC) = 1 -- Order to get latest values for deduplication.
) AS S
ON (
  {%- for identifier in row_identifiers -%}
  {% if not loop.first +%} AND {%- endif %} T.`{{ identifier }}` = S.`{{ identifier }}`
  {%- endfor -%}
  )
WHEN MATCHED AND S.`recordstamp` >= T.`recordstamp` THEN
  -- Update when data exists already.
  UPDATE SET
    {%- for column in columns %}
    `{{ column }}` = S.`{{ column }}`
    {%- if not loop.last -%}
        ,
    {%- endif -%}
    {%- endfor %}

WHEN NOT MATCHED BY TARGET THEN
  -- Insert when data is not presented in target table.
  INSERT (
    `account_id`,
    {%- for column in columns %}
    `{{ column }}`
    {%- if not loop.last -%}
        ,
    {%- endif -%}
    {%- endfor %}
  )
  VALUES(
    S.`account_id`,
    {%- for column in columns %}
    S.`{{ column }}`
    {%- if not loop.last -%}
        ,
    {%- endif -%}
    {%- endfor %}
  );
