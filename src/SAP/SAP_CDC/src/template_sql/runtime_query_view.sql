--  Copyright 2022 Google LLC
--
--  Licensed under the Apache License, Version 2.0 (the "License");
--  you may not use this file except in compliance with the License.
--  You may obtain a copy of the License at
--
--      https://www.apache.org/licenses/LICENSE-2.0
--
--  Unless required by applicable law or agreed to in writing, software
--  distributed under the License is distributed on an "AS IS" BASIS,
--  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
--  See the License for the specific language governing permissions and
--  limitations under the License.

WITH
  -- For each unique record (by primary key), find chronologically latest row. This ensures CDC
  -- view only contains one entry for a unique record.
  -- This also handles the conditions where SLT connector may have introduced true duplicate
  -- rows (same recordstamp).
  SourceTable AS (
    SELECT *
    FROM `${base_table}`
    WHERE ${primary_keys_not_null_clause}
    QUALIFY
      ROW_NUMBER() OVER (
        PARTITION BY ${primary_keys}
        ORDER BY
          recordstamp DESC,
          IF(operation_flag = 'D', 'ZZ', operation_flag) DESC -- Prioritize "D" flag within the dups.
        ) = 1
  )
SELECT * EXCEPT (operation_flag, is_deleted, recordstamp)
FROM SourceTable
-- ## CORTEX-CUSTOMER You can use "is_deleted = true" condition along with "operation_flag = 'D'",
-- if that is applicable to your CDC set up.
WHERE IFNULL(operation_flag, 'I') != 'D' -- We don't need to see Deleted records.

