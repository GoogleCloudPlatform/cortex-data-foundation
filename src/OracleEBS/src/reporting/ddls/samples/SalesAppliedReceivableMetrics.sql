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

-- Sample script showing how to query measures from the SalesAppliedReceivables event table.

SELECT
  RECEIVABLE_APPLICATION_ID,
  -- APPLICATION_TYPE: Type of entity the amount was applied to including: CASH (Cash Receipt),
  -- CM (Credit Memo)
  APPLICATION_TYPE,
  BILL_TO_CUSTOMER_NUMBER,
  BUSINESS_UNIT_NAME,
  EVENT_DATE,
  CURRENCY_CODE,
  AMOUNT_APPLIED,
  -- Both cash and credit memo are applied towards an invoice so INVOICE columns will
  -- always be populated.
  INVOICE.INVOICE_NUMBER,
  INVOICE.INVOICE_DATE,
  INVOICE.TOTAL_TRANSACTION_AMOUNT,
  INVOICE.TOTAL_REVENUE_AMOUNT,
  INVOICE.TOTAL_TAX_AMOUNT,
  -- Cash receipt columns will be null for credit memo application (APPLICATION_TYPE = 'CM')
  CASH_RECEIPT.RECEIPT_NUMBER,
  CASH_RECEIPT.DEPOSIT_DATE AS CASH_DEPOSIT_DATE,
  CASH_RECEIPT.STATUS AS CASH_RECEIPT_STATUS,
  CASH_RECEIPT.AMOUNT AS CASH_RECEIPT_AMOUNT
FROM `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.SalesAppliedReceivables`
WHERE APPLICATION_TYPE = 'CASH';
