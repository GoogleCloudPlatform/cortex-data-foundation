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

-- Sample script showing how to query order measures from the SalesOrder event table.

-- SalesOrders has order header level attributes/measures, and line level attributes/measures. Be
-- careful not to double count the measures when aggregating if unnesting nested fields like LINES,
-- ITEM_DESCRIPTIONS, etc.
-- Note that the underlying tables only contain additive measures (e.g. counts) so we need to derive
-- non-additive measures (e.g. ratios). Care needs to be taken when aggregating.

SELECT
  H.HEADER_ID,
  H.ORDER_NUMBER,
  H.ORDERED_DATE,
  H.BILL_TO_CUSTOMER_NAME,
  H.BUSINESS_UNIT_NAME,
  H.HEADER_STATUS,
  L.LINE_ID,
  L.LINE_NUMBER,
  L.LINE_STATUS,
  ID.TEXT AS ITEM_DESCRIPTION_TEXT,
  IC.CATEGORY_NAME,
  IC.DESCRIPTION AS CATEGORY_DESCRIPTION,
  L.ORDERED_QUANTITY,
  H.CURRENCY_CODE,
  L.ORDERED_AMOUNT,
  L.UNIT_LIST_PRICE,
  L.UNIT_SELLING_PRICE,
  L.SCHEDULE_SHIP_DATE,
  L.REQUEST_DATE
FROM `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.SalesOrders` AS H
-- Use LEFT JOIN UNNEST() to expand nested fields.
LEFT JOIN UNNEST(H.LINES) AS L
LEFT JOIN UNNEST(L.ITEM_DESCRIPTIONS) AS ID
LEFT JOIN UNNEST(L.ITEM_CATEGORIES) AS IC
WHERE
  -- Filter out return orders and lines.
  ORDER_CATEGORY_CODE = 'ORDER'
  AND L.LINE_CATEGORY_CODE = 'ORDER'
  -- View the relevant category set
  AND IC.CATEGORY_SET_ID = 1100000425
  -- View US item descriptions
  AND ID.LANGUAGE = 'US';
