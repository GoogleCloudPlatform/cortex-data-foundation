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

-- Sample script showing how to calculate invoice metrics from the daily aggregate table.

-- Aggregate table has aggregates converted to target currencies and in case of more than
-- one target currency, be careful while aggregating amounts to avoid double calculation.
-- Group by target currency to avoid this problem.

SELECT
  I.INVOICE_DATE,
  I.INVOICE_TYPE_NAME,
  I.BILL_TO_CUSTOMER_NAME,
  I.BUSINESS_UNIT_NAME,
  A.TARGET_CURRENCY_CODE,
  SUM(I.NUM_INVOICE_LINES) AS NUM_INVOICE_LINES,
  SUM(I.NUM_INTERCOMPANY_LINES) AS NUM_INTERCOMPANY_LINES,
  -- If querying amounts, always group by the target currency code or filter to a single value to
  -- avoid double counting converted amounts.
  SUM(A.TOTAL_LIST) AS TOTAL_LIST,
  SUM(A.TOTAL_SELLING) AS TOTAL_SELLING,
  SUM(A.TOTAL_INTERCOMPANY_LIST) AS TOTAL_INTERCOMPANY_LIST,
  SUM(A.TOTAL_INTERCOMPANY_SELLING) AS TOTAL_INTERCOMPANY_SELLING,
  SUM(A.TOTAL_DISCOUNT) AS TOTAL_DISCOUNT,
  SUM(A.TOTAL_TRANSACTION) AS TOTAL_TRANSACTION,
  SUM(A.TOTAL_REVENUE) AS TOTAL_REVENUE,
  SUM(A.TOTAL_TAX) AS TOTAL_TAX,
  -- Non-additive metrics
  SAFE_DIVIDE(SUM(I.NUM_INTERCOMPANY_LINES), SUM(I.NUM_INVOICE_LINES)) AS INTERCOMPANY_LINES_PCT
FROM `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.SalesInvoicesDailyAgg` AS I
-- Use LEFT JOIN UNNEST() to expand nested fields.
LEFT JOIN UNNEST(I.AMOUNTS) AS A
GROUP BY 1, 2, 3, 4, 5;
