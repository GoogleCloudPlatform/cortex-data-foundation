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

-- Sample script showing how to query invoice measures from the SalesInvoices event table.

-- SalesInvoices has invoice header level attributes/measures, and line level attributes/measures.
-- Be careful not to double count the measures when aggregating if unnesting nested fields like
-- LINES, ITEM_DESCRIPTIONS, etc.

SELECT
  H.INVOICE_ID,
  H.INVOICE_NUMBER,
  H.INVOICE_DATE,
  H.BILL_TO_CUSTOMER_NAME,
  H.BUSINESS_UNIT_NAME,
  H.NUM_LINES,
  H.CURRENCY_CODE,
  ID.TEXT AS ITEM_DESCRIPTION_TEXT,
  IC.CATEGORY_NAME,
  IC.DESCRIPTION AS CATEGORY_DESCRIPTION,
  -- Header level additive measures
  H.TOTAL_TRANSACTION_AMOUNT,
  H.TOTAL_REVENUE_AMOUNT,
  H.TOTAL_TAX_AMOUNT,
  -- Line level additive measures
  L.LINE_ID,
  L.LINE_NUMBER,
  L.INVOICED_QUANTITY,
  L.ORDERED_QUANTITY,
  L.CREDITED_QUANTITY,
  L.QUANTITY_UOM,
  L.UNIT_LIST_PRICE,
  L.UNIT_SELLING_PRICE,
  L.GROSS_UNIT_SELLING_PRICE,
  L.UNIT_DISCOUNT_PRICE,
  L.TRANSACTION_AMOUNT,
  L.REVENUE_AMOUNT,
  L.TAX_AMOUNT
FROM `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.SalesInvoices` AS H
-- Use LEFT JOIN UNNEST() to expand nested fields.
LEFT JOIN UNNEST(H.LINES) AS L
LEFT JOIN UNNEST(L.ITEM_DESCRIPTIONS) AS ID
LEFT JOIN UNNEST(L.ITEM_CATEGORIES) AS IC
WHERE
  -- Filter out non-invoice transactions like credit memo, debit memo, deposit and guarantee
  H.INVOICE_TYPE = 'INV'
  -- View the relevant category set
  AND IC.CATEGORY_SET_ID = 1100000425
  -- View US item descriptions
  AND ID.LANGUAGE = 'US';
