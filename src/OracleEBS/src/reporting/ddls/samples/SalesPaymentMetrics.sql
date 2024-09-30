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

-- Sample script showing how to query payment measures from the SalesPayments event table.

SELECT
  PAYMENT_SCHEDULE_ID,
  CASH_RECEIPT_ID,
  INVOICE_NUMBER,
  PAYMENT_CLASS_CODE,
  PAYMENT_STATUS_CODE,
  BILL_TO_CUSTOMER_NAME,
  BUSINESS_UNIT_NAME,
  TRANSACTION_DATE,
  CURRENCY_CODE,
  AMOUNT_DUE_ORIGINAL,
  AMOUNT_DUE_REMAINING,
  AMOUNT_DISCOUNTED,
  AMOUNT_APPLIED,
  AMOUNT_CREDITED,
  AMOUNT_ADJUSTED,
  TAX_ORIGINAL,
  TAX_REMAINING
FROM `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.SalesPayments`
WHERE
  -- Filter out non-payment transactions like invoices, credit memo, debit memo etc.
  IS_PAYMENT_TRANSACTION;
