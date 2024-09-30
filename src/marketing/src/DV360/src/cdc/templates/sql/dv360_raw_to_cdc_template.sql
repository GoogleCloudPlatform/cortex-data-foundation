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

/* Merging RAW table to CDC based on provided row identifier columns and
 * processing timestamp (recordstamp) column.
 * Processing timestamp for Raw table is extracted from table name,
 * per DV360 Offline Reporting conventions.
 */


MERGE `{{ target_project_id }}.{{ target_ds }}.{{ target_table }}` AS T
USING (
  WITH
    `LatestRawData` AS (
      SELECT
        PARSE_TIMESTAMP(
          '%Y%m%d %H%M%S',
          CONCAT(
            -- Execution date.
            ARRAY_REVERSE(SPLIT(_TABLE_SUFFIX, '_'))[ORDINAL(2)],
            ' ',
            -- Execution time.
            ARRAY_REVERSE(SPLIT(_TABLE_SUFFIX, '_'))[ORDINAL(1)]
          )
        ) AS recordstamp,
        {%- for identifier in nullable_identifiers %}
          COALESCE(`{{ identifier }}`, 'UNKNOWN') AS `{{ identifier }}`,
        {%- endfor %}
        *
        {%- if nullable_identifiers %} EXCEPT ({{ nullable_identifiers|join(', ') }})
      {%- endif %} -- Required columns
      FROM `{{ source_project_id }}.{{ source_ds }}.{{ source_table }}_*`
      -- Validate row identifier values are not null to avoid inconsistency in CDC logic.
      WHERE
      {%- for identifier in row_identifiers | reject("in", nullable_identifiers) %}
        {% if not loop.first %}AND {%+ endif -%}
        `{{ identifier }}` IS NOT NULL
      {%- endfor %}
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY
          {%- for identifier in row_identifiers %}
          `{{ identifier }}`
          {%- if not loop.last +%}, {%- endif -%}
          {%- endfor %}
        ORDER BY recordstamp DESC
      ) = 1
    )
  SELECT *
  FROM `LatestRawData`
  WHERE `recordstamp` >= (
      SELECT COALESCE(MAX(`recordstamp`), TIMESTAMP('1970-01-01 00:00:00+00'))
      FROM `{{ target_project_id }}.{{ target_ds }}.{{ target_table }}`
    )
) AS S -- Order to get latest values for deduplication.
  ON (
    {%- for identifier in row_identifiers -%}
      {% if not loop.first +%} AND {%+ endif %}T.`{{ identifier }}` = S.`{{ identifier }}`
    {%- endfor -%}
  )
WHEN MATCHED AND S.`recordstamp` > T.`recordstamp` THEN
-- Update when data exists already.
  UPDATE
    SET
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
    {%- endfor +%}
  )
  VALUES (
    {%- for column in columns %}
    S.`{{ column }}`
      {%- if not loop.last -%}
        ,
      {%- endif -%}
    {%- endfor %}
  );
