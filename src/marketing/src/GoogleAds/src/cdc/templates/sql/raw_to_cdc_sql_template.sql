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

/* Creating temp table based on RAW view.
 * Merging temp table to CDC based ON given row identifier columns.
 * Equal identifier columns indicates that values should be updated.
 * Otherwise new records are inserted into CDC table.
 */

-- ## EXPERIMENTAL

CREATE TEMP TABLE `{{temp_table}}`
AS
  SELECT * FROM `{{ source_project_id }}.{{ source_ds }}.{{ view }}`;

MERGE `{{ target_project_id }}.{{ target_ds }}.{{ table }}` AS T
USING `{{temp_table}}` AS S
  ON (
    {{ row_identifiers }}
  )
WHEN MATCHED THEN
  UPDATE SET
    {{ columns_identifiers }}
WHEN NOT MATCHED THEN
  INSERT (
    {{ columns }}
  )
  VALUES (
    {{ columns }}
  );
