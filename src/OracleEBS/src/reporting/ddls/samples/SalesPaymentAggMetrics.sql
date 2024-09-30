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

-- Sample script showing how to calculate payments metrics from the daily aggregate table.

-- Aggregate table has aggregates converted to target currencies and in case of more than
-- one target currency, be careful while aggregating amounts to avoid double calculation.
-- Group by target currency to avoid this problem.

SELECT
  H.TRANSACTION_DATE,
  H.PAYMENT_CLASS_CODE,
  H.BILL_TO_CUSTOMER_NAME,
  H.BUSINESS_UNIT_NAME,
  A.TARGET_CURRENCY_CODE,
  SUM(H.NUM_CLOSED_PAYMENTS) AS NUM_CLOSED_PAYMENTS,
  SUM(H.NUM_PAYMENTS) AS NUM_PAYMENTS,
  -- If querying amounts, always group by the target currency code or filter to a single value to
  -- avoid double counting converted amounts.
  SUM(A.TOTAL_ORIGINAL) AS TOTAL_ORIGINAL,
  SUM(A.TOTAL_REMAINING) AS TOTAL_REMAINING,
  SUM(A.TOTAL_OVERDUE_REMAINING) AS TOTAL_OVERDUE_REMAINING,
  SUM(A.TOTAL_DOUBTFUL_REMAINING) AS TOTAL_DOUBTFUL_REMAINING,
  SUM(A.TOTAL_DISCOUNTED) AS TOTAL_DISCOUNTED,
  SUM(A.TOTAL_APPLIED) AS TOTAL_APPLIED,
  SUM(A.TOTAL_CREDITED) AS TOTAL_CREDITED,
  SUM(A.TOTAL_ADJUSTED) AS TOTAL_ADJUSTED,
  SUM(A.TOTAL_TAX_ORIGINAL) AS TOTAL_TAX_ORIGINAL,
  SUM(A.TOTAL_TAX_REMAINING) AS TOTAL_TAX_REMAINING,
  -- Non-additive metrics
  SAFE_DIVIDE(SUM(H.NUM_CLOSED_PAYMENTS), SUM(H.NUM_PAYMENTS)) AS CLOSED_PAYMENTS_PCT,
  SAFE_DIVIDE(SUM(H.TOTAL_DAYS_TO_PAYMENT), SUM(H.NUM_CLOSED_PAYMENTS)) AS AVG_DAYS_TO_PAYMENT
FROM `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.SalesPaymentsDailyAgg` AS H
-- Use LEFT JOIN UNNEST() to expand nested fields.
LEFT JOIN UNNEST(H.AMOUNTS) AS A
GROUP BY 1, 2, 3, 4, 5;
