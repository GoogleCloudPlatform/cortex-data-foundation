-- Copyright 2024 Google LLC
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--      https://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- Sample script showing how to calculate applied receivables metrics from the daily aggregate table

-- Aggregate table has aggregates converted to target currencies and in case of more than
-- one target currency, be careful while aggregating amounts to avoid double calculation.
-- Group by target currency to avoid this problem.

SELECT
  H.EVENT_DATE,
  H.APPLICATION_TYPE,
  H.BILL_TO_CUSTOMER_NAME,
  H.BUSINESS_UNIT_NAME,
  A.TARGET_CURRENCY_CODE,
  -- If querying amounts, always group by the target currency code or filter to a single value to
  -- avoid double counting converted amounts.
  SUM(A.TOTAL_RECEIVED) AS TOTAL_RECEIVED,
  SUM(A.TOTAL_APPLIED) AS TOTAL_APPLIED
FROM `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.SalesAppliedReceivablesDailyAgg` AS H
-- Use LEFT JOIN UNNEST() to expand nested fields.
LEFT JOIN UNNEST(H.AMOUNTS) AS A
GROUP BY 1, 2, 3, 4, 5;
