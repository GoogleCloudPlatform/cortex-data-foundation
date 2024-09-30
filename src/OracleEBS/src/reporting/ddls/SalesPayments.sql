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

-- SalesPayments reporting table.

WITH
  Payments AS (
    SELECT
      * REPLACE (
        COALESCE(EXCHANGE_DATE, TRANSACTION_DATE) AS EXCHANGE_DATE,
        IF(STATUS = 'CL', PAYMENT_CLOSE_DATE, NULL) AS PAYMENT_CLOSE_DATE
      ),
      DUE_DATE < CURRENT_DATE AND AMOUNT_DUE_REMAINING > 0 AS IS_OPEN_AND_OVERDUE,
      DUE_DATE < PAYMENT_CLOSE_DATE AND AMOUNT_DUE_REMAINING = 0 AS WAS_CLOSED_LATE
    FROM
      `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.Payments`
  )
SELECT
  Payments.PAYMENT_SCHEDULE_ID,
  Payments.CASH_RECEIPT_ID,
  Payments.INVOICE_ID,
  Payments.INVOICE_NUMBER,
  Payments.CLASS AS PAYMENT_CLASS_CODE,
  -- Payment transactions will relate to a CASH_RECEIPT_ID while non-payment transactions will
  -- relate to an INVOICE_ID. Non-payments transactions include payment classes like invoices,
  -- debit memos, credit memos, etc.
  Payments.CLASS = 'PMT' AS IS_PAYMENT_TRANSACTION,
  Payments.STATUS AS PAYMENT_STATUS_CODE,
  Payments.SITE_USE_ID AS BILL_TO_SITE_USE_ID,
  Customer.ACCOUNT_NUMBER AS BILL_TO_CUSTOMER_NUMBER,
  Customer.ACCOUNT_NAME AS BILL_TO_CUSTOMER_NAME,
  Customer.COUNTRY_NAME AS BILL_TO_CUSTOMER_COUNTRY,
  Payments.BUSINESS_UNIT_ID,
  BusinessUnit.BUSINESS_UNIT_NAME,
  BusinessUnit.LEDGER_ID,
  Ledger.LEDGER_NAME,
  Payments.TRANSACTION_DATE,
  CalDate.CALMONTH AS TRANSACTION_MONTH_NUM,
  CalDate.CALQUARTER AS TRANSACTION_QUARTER_NUM,
  CalDate.CALYEAR AS TRANSACTION_YEAR_NUM,
  Payments.LEDGER_DATE,
  FiscalDate.PERIOD_TYPE AS FISCAL_PERIOD_TYPE,
  FiscalDate.PERIOD_SET_NAME AS FISCAL_PERIOD_SET_NAME,
  FiscalDate.PERIOD_NAME AS FISCAL_GL_PERIOD_NAME,
  FiscalDate.PERIOD_NUM AS FISCAL_GL_PERIOD_NUM,
  FiscalDate.QUARTER_NUM AS FISCAL_GL_QUARTER_NUM,
  FiscalDate.YEAR_NUM AS FISCAL_GL_YEAR_NUM,
  Payments.PAYMENT_CLOSE_DATE,
  Payments.DUE_DATE,
  Payments.EXCHANGE_DATE,
  Payments.IS_OPEN_AND_OVERDUE,
  Payments.WAS_CLOSED_LATE,
  Payments.IS_OPEN_AND_OVERDUE AND DATE_DIFF(CURRENT_DATE, Payments.DUE_DATE, DAY) > 90
    AS IS_DOUBTFUL,
  Payments.CURRENCY_CODE,
  Payments.AMOUNT_DUE_ORIGINAL,
  Payments.AMOUNT_DUE_REMAINING,
  Payments.DISCOUNT_TAKEN_EARNED AS AMOUNT_DISCOUNTED,
  Payments.AMOUNT_APPLIED,
  Payments.AMOUNT_CREDITED,
  Payments.AMOUNT_ADJUSTED,
  Payments.TAX_ORIGINAL,
  Payments.TAX_REMAINING,
  IF(Payments.IS_OPEN_AND_OVERDUE, DATE_DIFF(CURRENT_DATE, Payments.DUE_DATE, DAY), NULL)
    AS DAYS_OVERDUE,
  IF(Payments.WAS_CLOSED_LATE, DATE_DIFF(Payments.PAYMENT_CLOSE_DATE, Payments.DUE_DATE, DAY), NULL)
    AS DAYS_LATE,
  DATE_DIFF(Payments.PAYMENT_CLOSE_DATE, Invoices.INVOICE_DATE, DAY) AS DAYS_TO_PAYMENT,
  Payments.CREATION_TS,
  Payments.LAST_UPDATE_TS
FROM Payments
LEFT OUTER JOIN
  `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.SalesInvoices` AS Invoices
  USING (INVOICE_ID)
LEFT OUTER JOIN
  `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.CustomerSiteUseMD` AS Customer
  ON
    Payments.SITE_USE_ID = Customer.SITE_USE_ID
LEFT OUTER JOIN
  `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.BusinessUnitMD` AS BusinessUnit
  ON
    Payments.BUSINESS_UNIT_ID = BusinessUnit.BUSINESS_UNIT_ID
LEFT OUTER JOIN
  `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.LedgerMD` AS Ledger
  ON
    BusinessUnit.LEDGER_ID = Ledger.LEDGER_ID
LEFT OUTER JOIN
  `{{ project_id_src }}.{{ k9_datasets_processing }}.calendar_date_dim` AS CalDate
  ON
    Payments.TRANSACTION_DATE = CalDate.DATE
LEFT OUTER JOIN
  `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.FiscalDateMD` AS FiscalDate
  ON
    Payments.LEDGER_DATE = FiscalDate.FISCAL_DATE
    AND Ledger.PERIOD_TYPE = FiscalDate.PERIOD_TYPE
    AND Ledger.PERIOD_SET_NAME = FiscalDate.PERIOD_SET_NAME
