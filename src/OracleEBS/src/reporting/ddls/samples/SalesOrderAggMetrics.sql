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

-- Sample script showing how to calculate order metrics from the daily aggregate table.

-- SalesOrdersDailyAgg has order header level attributes/measures, and line level
-- attributes/measures grouped to the date. Be careful not to double count the header level measures
-- by querying them separately from the line level measures.
-- Note that the underlying tables only contain additive measures (e.g. counts) so we need to derive
-- non-additive measures (e.g. ratios). Care needs to be taken when aggregating them further
-- (e.g. from daily to monthly level).

-- Header level attributes and measures.
SELECT
  ORDERED_DATE,
  BILL_TO_CUSTOMER_NAME,
  BUSINESS_UNIT_NAME,
  SUM(NUM_ORDERS) AS NUM_ORDERS,
  SUM(NUM_FULFILLED_ORDERS) AS NUM_FULFILLED_ORDERS,
  -- Non-additive metrics
  SAFE_DIVIDE(SUM(NUM_FULFILLED_ORDERS), SUM(NUM_ORDERS)) AS FULFILLED_ORDERS_PCT,
  SAFE_DIVIDE(SUM(NUM_BLOCKED_ORDERS), SUM(NUM_ORDERS)) AS BLOCKED_ORDERS_PCT,
  SAFE_DIVIDE(SUM(NUM_ORDERS_FULFILLED_BY_PROMISE_DATE), SUM(NUM_ORDERS))
    AS ON_TIME_ORDERS_PCT
FROM `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.SalesOrdersDailyAgg`
GROUP BY 1, 2, 3;

-- Line level attributes and measures.
SELECT
  H.ORDERED_DATE,
  H.BILL_TO_CUSTOMER_NAME,
  H.BUSINESS_UNIT_NAME,
  L.ITEM_CATEGORY_NAME,
  L.ITEM_CATEGORY_DESCRIPTION,
  -- If querying amounts, always group by the target currency code or filter to a single value to
  -- avoid double counting converted amounts.
  A.TARGET_CURRENCY_CODE,
  SUM(A.TOTAL_ORDERED) AS TOTAL_ORDERED,
  SUM(A.TOTAL_FULFILLED) AS TOTOAL_FULFILLED,
  -- Non-additive metrics
  SAFE_DIVIDE(SUM(A.TOTAL_FULFILLED), SUM(A.TOTAL_ORDERED)) AS AMOUNT_FULFILLED_PCT
FROM `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.SalesOrdersDailyAgg` AS H
LEFT JOIN UNNEST(H.LINES) AS L
LEFT JOIN UNNEST(L.AMOUNTS) AS A
GROUP BY 1, 2, 3, 4, 5, 6;
