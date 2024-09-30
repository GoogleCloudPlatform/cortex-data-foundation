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

/* Creating RAW view based ON recordstamp.
 * Deduplicating source data based on the latest processing time (recordstamp).
 */

-- ## EXPERIMENTAL

CREATE OR REPLACE VIEW `{{ project_id_src }}.{{ dataset_raw_landing_marketing_googleads }}.{{ raw_view }}`
AS (
  SELECT
  {%- for column in columns %}
  {{ column }}
  {%- if not loop.last %},{%- endif %}
  {%- endfor %}
  FROM `{{ project_id_src }}.{{ dataset_raw_landing_marketing_googleads }}.{{ raw_table }}` AS S1
  INNER JOIN (
      SELECT
        {%- for identifier in row_identifiers %}
        `{{ identifier }}`,
        {%- endfor %}
        MAX(recordstamp) AS recordstamp
      FROM `{{ project_id_src }}.{{ dataset_raw_landing_marketing_googleads }}.{{ raw_table }}`
      GROUP BY
        {%- for identifier in row_identifiers %}
        `{{ identifier }}`
        {%- if not loop.last %},{%- endif %}
        {%- endfor %}
  ) AS S2
  USING (
    {%- for identifier in row_identifiers -%}
    `{{ identifier }}`,
    {%- endfor -%}
    recordstamp
  )
)
