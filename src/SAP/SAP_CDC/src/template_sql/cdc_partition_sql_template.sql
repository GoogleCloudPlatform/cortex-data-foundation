--  Copyright 2021 Google Inc.
  --  Licensed under the Apache License, Version 2.0 (the "License");
  --  you may not use this file except in compliance with the License.
  --  You may obtain a copy of the License at
  --      http://www.apache.org/licenses/LICENSE-2.0
  --  Unless required by applicable law or agreed to in writing, software
  --  distributed under the License is distributed on an "AS IS" BASIS,
  --  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  --  See the License for the specific language governing permissions and
  --  limitations under the License.

  -- this sql optimized template will work well for partitioned tables

DECLARE max_rstamp TIMESTAMP;
DECLARE max_raw_stamp TIMESTAMP;
DECLARE processed_date ARRAY<DATE>;

SET max_raw_stamp = TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 SECOND);

SET max_rstamp = (SELECT IFNULL(MAX(recordstamp), TIMESTAMP('1940-12-25 05:30:00+00')) FROM `${target_table}`);

-- NEW LOGIC FOR PROCESSED DATE
SET processed_date = (
 WITH
      S01 AS (
        SELECT * FROM `${base_table}`
        WHERE recordstamp >= max_rstamp and recordstamp <= max_raw_stamp
        AND ${primary_keys_not_null_clause}
      ),

      -- To handle occasional dups from SLT connector

      S11 AS (

        SELECT * FROM S01 QUALIFY ROW_NUMBER() 
        OVER (  
            PARTITION BY ${primary_keys}
            ORDER BY recordstamp DESC,
            IF (operation_flag = 'D', 'ZZ', operation_flag) DESC 
        ) = 1
        
        -- OLD LOGIC FOR PROCESSED DATE
        -- SELECT ${primary_keys}, recordstamp EXCEPT(row_num)
        -- FROM (
        --    SELECT *, ROW_NUMBER() OVER (PARTITION BY ${primary_keys} ORDER BY recordstamp desc) AS row_num
        --   FROM S01
        -- )
        -- WHERE row_num = 1
)

select ARRAY_AGG(distinct date(T.recordstamp)) from `${target_table}` T inner join S11 S on  ${primary_keys_join_clause}
);

-- NEW MERGE QUERY 
MERGE `${target_table}` AS T
USING (
  WITH
    -- All the rows that arrived in raw table after the latest CDC table refresh.
    NewRawRecords AS (
      SELECT * FROM `${base_table}`
      WHERE recordstamp >= max_rstamp and recordstamp <= max_raw_stamp
      AND ${primary_keys_not_null_clause}
    )
    -- For each unique record (by primary key), find chronologically latest row. This ensures CDC
    -- table only contains one entry for a unique record.
    -- This also handles the conditions where SLT connector may have introduced true duplicate
    -- rows (same recordstamp).
    SELECT *
    FROM NewRawRecords
    QUALIFY
      ROW_NUMBER() OVER (
        PARTITION BY ${primary_keys}
        ORDER BY
          recordstamp DESC,
          IF(operation_flag = 'D', 'ZZ', operation_flag) DESC -- Prioritize "D" flag within the dups.
      ) = 1
  ) AS S
ON date(T.`recordstamp`) IN UNNEST(processed_date) AND 
${primary_keys_join_clause}
WHEN NOT MATCHED AND IFNULL(S.operation_flag, 'I') != 'D' THEN
  INSERT (${fields})
  VALUES (${fields})
WHEN MATCHED AND S.operation_flag = 'D' THEN
  DELETE
WHEN MATCHED AND S.operation_flag IN ('I','U') THEN
  UPDATE SET ${update_fields};