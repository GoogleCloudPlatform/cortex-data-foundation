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

-- SalesAppliedReceivables reporting table.

WITH
  Applications AS (
    SELECT
      RECEIVABLE_APPLICATION_ID,
      APPLICATION_TYPE,
      CASH_RECEIPT_ID,
      CASH_RECEIPT_HISTORY_ID,
      INVOICE_ID,
      AMOUNT_APPLIED,
      CREATION_TS,
      LAST_UPDATE_TS
    FROM
      `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.AppliedReceivables`
    WHERE DISPLAY = 'Y'
  ),
  CashReceipts AS (
    SELECT
      Receipt.CASH_RECEIPT_ID,
      Receipt.BILL_TO_SITE_USE_ID,
      Customer.ACCOUNT_NUMBER AS BILL_TO_CUSTOMER_NUMBER,
      Customer.ACCOUNT_NAME AS BILL_TO_CUSTOMER_NAME,
      Customer.COUNTRY_NAME AS BILL_TO_CUSTOMER_COUNTRY,
      Receipt.BUSINESS_UNIT_ID,
      BusinessUnit.BUSINESS_UNIT_NAME,
      Receipt.LEDGER_ID,
      Receipt.RECEIPT_DATE,
      CalDate.CalMonth AS RECEIPT_MONTH_NUM,
      CalDate.CalQuarter AS RECEIPT_QUARTER_NUM,
      CalDate.CalYear AS RECEIPT_YEAR_NUM,
      COALESCE(Receipt.EXCHANGE_DATE, Receipt.RECEIPT_DATE) AS EXCHANGE_DATE,
      Receipt.CURRENCY_CODE,
      STRUCT(
        Receipt.RECEIPT_NUMBER,
        Receipt.DEPOSIT_DATE,
        Receipt.STATUS,
        -- Nulls should be considered confirmed.
        COALESCE(Receipt.CONFIRMED_FLAG = 'Y', TRUE) AS IS_CONFIRMED,
        Receipt.AMOUNT
      ) AS CASH_RECEIPT
    FROM
      `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.CashReceipts` AS Receipt
    LEFT OUTER JOIN
      `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.CustomerSiteUseMD` AS Customer
      ON
        Receipt.BILL_TO_SITE_USE_ID = Customer.SITE_USE_ID
    LEFT OUTER JOIN
      `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.BusinessUnitMD` AS BusinessUnit
      ON
        Receipt.BUSINESS_UNIT_ID = BusinessUnit.BUSINESS_UNIT_ID
    LEFT OUTER JOIN
      `{{ project_id_src }}.{{ k9_datasets_processing }}.calendar_date_dim` AS CalDate
      ON
        Receipt.RECEIPT_DATE = CalDate.DATE
  ),
  JoinedApplications AS (
    SELECT
      Applications.RECEIVABLE_APPLICATION_ID,
      Applications.APPLICATION_TYPE,
      Applications.CASH_RECEIPT_ID,
      Applications.CASH_RECEIPT_HISTORY_ID,
      Applications.INVOICE_ID,
      COALESCE(CashReceipts.BILL_TO_SITE_USE_ID, Invoices.BILL_TO_SITE_USE_ID)
        AS BILL_TO_SITE_USE_ID,
      COALESCE(CashReceipts.BILL_TO_CUSTOMER_NUMBER, Invoices.BILL_TO_CUSTOMER_NUMBER)
        AS BILL_TO_CUSTOMER_NUMBER,
      COALESCE(CashReceipts.BILL_TO_CUSTOMER_NAME, Invoices.BILL_TO_CUSTOMER_NAME)
        AS BILL_TO_CUSTOMER_NAME,
      COALESCE(CashReceipts.BILL_TO_CUSTOMER_COUNTRY, Invoices.BILL_TO_CUSTOMER_COUNTRY)
        AS BILL_TO_CUSTOMER_COUNTRY,
      COALESCE(CashReceipts.BUSINESS_UNIT_ID, Invoices.BUSINESS_UNIT_ID) AS BUSINESS_UNIT_ID,
      COALESCE(CashReceipts.BUSINESS_UNIT_NAME, Invoices.BUSINESS_UNIT_NAME) AS BUSINESS_UNIT_NAME,
      COALESCE(CashReceipts.LEDGER_ID, Invoices.LEDGER_ID) AS LEDGER_ID,
      COALESCE(CashReceipts.RECEIPT_DATE, Invoices.INVOICE_DATE) AS EVENT_DATE,
      COALESCE(CashReceipts.RECEIPT_MONTH_NUM, Invoices.INVOICE_MONTH_NUM) AS EVENT_MONTH_NUM,
      COALESCE(CashReceipts.RECEIPT_QUARTER_NUM, Invoices.INVOICE_QUARTER_NUM) AS EVENT_QUARTER_NUM,
      COALESCE(CashReceipts.RECEIPT_YEAR_NUM, Invoices.INVOICE_YEAR_NUM) AS EVENT_YEAR_NUM,
      COALESCE(CashReceipts.EXCHANGE_DATE, Invoices.EXCHANGE_DATE) AS EXCHANGE_DATE,
      COALESCE(CashReceipts.CURRENCY_CODE, Invoices.CURRENCY_CODE) AS CURRENCY_CODE,
      -- SalesInvoices has LEDGER_DATE at the line level, however we do not attempt to use it
      -- because many AppliedReceviables records do not include an INVOICE_LINE_ID to select a line.
      COALESCE(ReceiptHistory.LEDGER_DATE, Invoices.INVOICE_DATE) AS LEDGER_DATE,
      Applications.AMOUNT_APPLIED,
      Applications.CREATION_TS,
      Applications.LAST_UPDATE_TS,
      CashReceipts.CASH_RECEIPT,
      STRUCT(
        Invoices.INVOICE_NUMBER,
        Invoices.INVOICE_TYPE_ID,
        Invoices.INVOICE_TYPE,
        Invoices.INVOICE_TYPE_NAME,
        Invoices.INVOICE_DATE,
        Invoices.IS_COMPLETE,
        Invoices.TOTAL_TRANSACTION_AMOUNT,
        Invoices.TOTAL_REVENUE_AMOUNT,
        Invoices.TOTAL_TAX_AMOUNT
      ) AS INVOICE
    FROM Applications
    LEFT OUTER JOIN CashReceipts
      USING (CASH_RECEIPT_ID)
    LEFT OUTER JOIN
      `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.CashReceiptHistories`
        AS ReceiptHistory
      USING (CASH_RECEIPT_HISTORY_ID)
    LEFT OUTER JOIN
      `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.SalesInvoices` AS Invoices
      ON
        Applications.INVOICE_ID = Invoices.INVOICE_ID
  )
SELECT
  JoinedApplications.RECEIVABLE_APPLICATION_ID,
  JoinedApplications.APPLICATION_TYPE,
  JoinedApplications.CASH_RECEIPT_ID,
  JoinedApplications.CASH_RECEIPT_HISTORY_ID,
  JoinedApplications.INVOICE_ID,
  JoinedApplications.BILL_TO_SITE_USE_ID,
  JoinedApplications.BILL_TO_CUSTOMER_NUMBER,
  JoinedApplications.BILL_TO_CUSTOMER_NAME,
  JoinedApplications.BILL_TO_CUSTOMER_COUNTRY,
  JoinedApplications.BUSINESS_UNIT_ID,
  JoinedApplications.BUSINESS_UNIT_NAME,
  JoinedApplications.LEDGER_ID,
  Ledger.LEDGER_NAME,
  JoinedApplications.EVENT_DATE,
  JoinedApplications.EVENT_MONTH_NUM,
  JoinedApplications.EVENT_QUARTER_NUM,
  JoinedApplications.EVENT_YEAR_NUM,
  JoinedApplications.LEDGER_DATE,
  FiscalDate.PERIOD_TYPE AS FISCAL_PERIOD_TYPE,
  FiscalDate.PERIOD_SET_NAME AS FISCAL_PERIOD_SET_NAME,
  FiscalDate.PERIOD_NAME AS FISCAL_GL_PERIOD_NAME,
  FiscalDate.PERIOD_NUM AS FISCAL_GL_PERIOD_NUM,
  FiscalDate.QUARTER_NUM AS FISCAL_GL_QUARTER_NUM,
  FiscalDate.YEAR_NUM AS FISCAL_GL_YEAR_NUM,
  JoinedApplications.EXCHANGE_DATE,
  JoinedApplications.CURRENCY_CODE,
  JoinedApplications.AMOUNT_APPLIED,
  JoinedApplications.CREATION_TS,
  JoinedApplications.LAST_UPDATE_TS,
  JoinedApplications.CASH_RECEIPT,
  JoinedApplications.INVOICE
FROM JoinedApplications
LEFT OUTER JOIN
  `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.LedgerMD` AS Ledger
  ON
    JoinedApplications.LEDGER_ID = Ledger.LEDGER_ID
LEFT OUTER JOIN
  `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.FiscalDateMD` AS FiscalDate
  ON
    JoinedApplications.LEDGER_DATE = FiscalDate.FISCAL_DATE
    AND Ledger.PERIOD_TYPE = FiscalDate.PERIOD_TYPE
    AND Ledger.PERIOD_SET_NAME = FiscalDate.PERIOD_SET_NAME
