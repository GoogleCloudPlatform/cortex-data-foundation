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

-- Payments base fact view.

SELECT
  PAYMENT_SCHEDULE_ID,
  CASH_RECEIPT_ID,
  CUSTOMER_TRX_ID AS INVOICE_ID,
  TRX_NUMBER AS INVOICE_NUMBER,
  CLASS,
  STATUS,
  CUSTOMER_SITE_USE_ID AS SITE_USE_ID,
  ORG_ID AS BUSINESS_UNIT_ID,
  TRX_DATE AS TRANSACTION_DATE,
  GL_DATE AS LEDGER_DATE,
  ACTUAL_DATE_CLOSED AS PAYMENT_CLOSE_DATE,
  DUE_DATE,
  EXCHANGE_DATE,
  INVOICE_CURRENCY_CODE AS CURRENCY_CODE,
  AMOUNT_DUE_ORIGINAL,
  AMOUNT_DUE_REMAINING,
  DISCOUNT_TAKEN_EARNED,
  AMOUNT_APPLIED,
  AMOUNT_CREDITED,
  AMOUNT_ADJUSTED,
  TAX_ORIGINAL,
  TAX_REMAINING,
  CREATION_DATE AS CREATION_TS,
  LAST_UPDATE_DATE AS LAST_UPDATE_TS
FROM
  `{{ project_id_src }}.{{ oracle_ebs_datasets_cdc }}.AR_PAYMENT_SCHEDULES_ALL`
