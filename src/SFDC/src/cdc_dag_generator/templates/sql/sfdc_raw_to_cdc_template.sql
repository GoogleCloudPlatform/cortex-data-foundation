--  Copyright 2023 Google Inc.

--  Licensed under the Apache License, Version 2.0 (the "License");
--  you may not use this file except in compliance with the License.
--  You may obtain a copy of the License at

--      http://www.apache.org/licenses/LICENSE-2.0

--  Unless required by applicable law or agreed to in writing, software
--  distributed under the License is distributed on an "AS IS" BASIS,
--  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
--  See the License for the specific language governing permissions and
--  limitations under the License.

-- Max SystemModstamp in the target table
-- Maximum duration of Salesforce Apex transaction is 10 minutes.
-- By handling 10 minutes prior to the last SystemModstamp,
-- we account for transactions that were in-flight
-- during the prior replication
DECLARE max_rs TIMESTAMP;

SET max_rs = TIMESTAMP_SUB((
    SELECT
      COALESCE(MAX(${target_systemmodstamp}),
      TIMESTAMP('1980-01-01 00:00:00+00'))
    FROM
      `${target_table}`
  ),
  INTERVAL 10 MINUTE
);

BEGIN TRANSACTION;

--- Delete rows that are not present in raw. This will get rid of deleted and archived records.
DELETE FROM `${target_table}`
WHERE
  `${target_id}` NOT IN (
    SELECT
      `${source_id}` AS `${target_id}`
    FROM
      `${source_table}`
  );

-- Delete rows that are updated in the source.
-- (Use '>=' to account for _possible_ inconsistency
-- due to SFDC SystemModstamp resolution)
DELETE FROM `${target_table}`
WHERE
  `${target_id}` IN (
    SELECT
      `${source_id}` AS `${target_id}`
    FROM
      `${source_table}`
    WHERE
      `${source_systemmodstamp}` >= max_rs
  );

-- Insert new and updated rows.
-- (Use '>=' to account for _possible_ inconsistency
-- due to SFDC SystemModstamp resolution)
INSERT INTO `${target_table}` (
  ${target_fields}
)
SELECT
  ${field_assignments}
FROM
  `${source_table}` AS SRC
WHERE
  SRC.${source_systemmodstamp} >= max_rs;

COMMIT TRANSACTION;
