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

-- SalesInvoices reporting table.

WITH
  LatestLedger AS (
    SELECT
      INVOICE_LINE_ID,
      LEDGER_ID,
      MAX(LEDGER_DATE) AS LEDGER_DATE
    FROM
      `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.InvoiceLineLedger`
    GROUP BY 1, 2
  ),
  TrxLineDetails AS (
    SELECT
      LatestLedger.INVOICE_LINE_ID,
      LatestLedger.LEDGER_DATE,
      FiscalDate.PERIOD_TYPE AS FISCAL_PERIOD_TYPE,
      FiscalDate.PERIOD_SET_NAME AS FISCAL_PERIOD_SET_NAME,
      FiscalDate.PERIOD_NAME AS FISCAL_GL_PERIOD_NAME,
      FiscalDate.PERIOD_NUM AS FISCAL_GL_PERIOD_NUM,
      FiscalDate.QUARTER_NUM AS FISCAL_GL_QUARTER_NUM,
      FiscalDate.YEAR_NUM AS FISCAL_GL_YEAR_NUM
    FROM
      LatestLedger
    LEFT OUTER JOIN
      `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.LedgerMD` AS Ledger
      ON
        LatestLedger.LEDGER_ID = Ledger.LEDGER_ID
    LEFT OUTER JOIN
      `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.FiscalDateMD` AS FiscalDate
      ON
        LatestLedger.LEDGER_DATE = FiscalDate.FISCAL_DATE
        AND Ledger.PERIOD_TYPE = FiscalDate.PERIOD_TYPE
        AND Ledger.PERIOD_SET_NAME = FiscalDate.PERIOD_SET_NAME
  ),
  InvoiceLines AS (
    SELECT
      INVOICE_ID,
      LINE_ID,
      LINK_TO_LINE_ID,
      LINE_NUMBER,
      SAFE_CAST(INTERFACE_LINE_ATTRIBUTE6 AS INT64) AS ORDER_LINE_ID,
      DESCRIPTION,
      INVENTORY_ITEM_ID,
      INVOICED_QUANTITY,
      ORDERED_QUANTITY,
      CREDITED_QUANTITY,
      QUANTITY_UOM,
      UNIT_LIST_PRICE,
      UNIT_SELLING_PRICE,
      COALESCE(GROSS_UNIT_SELLING_PRICE, UNIT_SELLING_PRICE) AS GROSS_UNIT_SELLING_PRICE,
      CREATION_TS,
      LAST_UPDATE_TS,
      TRANSACTION_AMOUNT,
      REVENUE_AMOUNT
    FROM
      `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.InvoiceLines`
    WHERE
      INTERFACE_LINE_CONTEXT = 'ORDER ENTRY'
      AND LINE_TYPE != 'TAX'
  ),
  TaxInvoiceLines AS (
    SELECT
      LINK_TO_LINE_ID,
      SUM(TRANSACTION_AMOUNT) AS TRANSACTION_AMOUNT
    FROM
      `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.InvoiceLines`
    WHERE
      LINE_TYPE = 'TAX'
    GROUP BY 1
  ),
  SalesOrderLines AS (
    SELECT
      H.HEADER_ID,
      H.ORDER_NUMBER,
      H.ORDERED_DATE,
      H.ORDER_SOURCE_ID,
      H.ORDER_SOURCE_NAME,
      L.LINE_ID,
      L.ITEM_ORGANIZATION_ID,
      L.ITEM_ORGANIZATION_NAME,
      L.INVENTORY_ITEM_ID,
      L.ITEM_PART_NUMBER,
      L.ITEM_DESCRIPTIONS,
      L.ITEM_CATEGORIES
    FROM `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.SalesOrders` AS H
    LEFT JOIN UNNEST(H.LINES) AS L
  ),
  -- Join invoice lines with order lines and process invoice header level measures.
  JoinedLines AS (
    SELECT
      InvoiceLines.INVOICE_ID,
      SUM(InvoiceLines.TRANSACTION_AMOUNT) AS TOTAL_TRANSACTION_AMOUNT,
      SUM(InvoiceLines.REVENUE_AMOUNT) AS TOTAL_REVENUE_AMOUNT,
      SUM(TaxInvoiceLines.TRANSACTION_AMOUNT) AS TOTAL_TAX_AMOUNT,
      COUNT(InvoiceLines.LINE_ID) AS NUM_LINES,
      COUNTIF(SalesOrderLines.ORDER_SOURCE_ID = 10) AS NUM_INTERCOMPANY_LINES,
      ARRAY_AGG(
        STRUCT(
          InvoiceLines.LINE_ID,
          InvoiceLines.LINE_NUMBER,
          SalesOrderLines.HEADER_ID AS ORDER_HEADER_ID,
          SalesOrderLines.ORDER_NUMBER AS ORDER_HEADER_NUMBER,
          InvoiceLines.ORDER_LINE_ID,
          SalesOrderLines.ORDER_SOURCE_ID,
          SalesOrderLines.ORDER_SOURCE_NAME,
          SalesOrderLines.ORDERED_DATE,
          InvoiceLines.DESCRIPTION AS LINE_DESCRIPTION,
          SalesOrderLines.ORDER_SOURCE_ID = 10 AS IS_INTERCOMPANY,
          TrxLineDetails.LEDGER_DATE,
          TrxLineDetails.FISCAL_PERIOD_TYPE,
          TrxLineDetails.FISCAL_PERIOD_SET_NAME,
          TrxLineDetails.FISCAL_GL_PERIOD_NAME,
          TrxLineDetails.FISCAL_GL_PERIOD_NUM,
          TrxLineDetails.FISCAL_GL_QUARTER_NUM,
          TrxLineDetails.FISCAL_GL_YEAR_NUM,
          SalesOrderLines.ITEM_ORGANIZATION_ID,
          SalesOrderLines.ITEM_ORGANIZATION_NAME,
          SalesOrderLines.INVENTORY_ITEM_ID,
          SalesOrderLines.ITEM_PART_NUMBER,
          SalesOrderLines.ITEM_DESCRIPTIONS,
          SalesOrderLines.ITEM_CATEGORIES,
          InvoiceLines.INVOICED_QUANTITY,
          InvoiceLines.ORDERED_QUANTITY,
          InvoiceLines.CREDITED_QUANTITY,
          InvoiceLines.QUANTITY_UOM,
          InvoiceLines.UNIT_LIST_PRICE,
          InvoiceLines.UNIT_SELLING_PRICE,
          InvoiceLines.GROSS_UNIT_SELLING_PRICE,
          InvoiceLines.UNIT_LIST_PRICE - InvoiceLines.GROSS_UNIT_SELLING_PRICE
            AS UNIT_DISCOUNT_PRICE,
          InvoiceLines.TRANSACTION_AMOUNT,
          InvoiceLines.REVENUE_AMOUNT,
          TaxInvoiceLines.TRANSACTION_AMOUNT AS TAX_AMOUNT,
          InvoiceLines.CREATION_TS,
          InvoiceLines.LAST_UPDATE_TS
        )
      ) AS LINES
    FROM
      InvoiceLines
    LEFT OUTER JOIN
      SalesOrderLines
      ON
        InvoiceLines.ORDER_LINE_ID = SalesOrderLines.LINE_ID
    LEFT OUTER JOIN
      TrxLineDetails
      ON
        InvoiceLines.LINE_ID = TrxLineDetails.INVOICE_LINE_ID
    LEFT OUTER JOIN
      TaxInvoiceLines
      ON
        InvoiceLines.LINE_ID = TaxInvoiceLines.LINK_TO_LINE_ID
    GROUP BY 1
  )
-- Join invoice headers to dimensions.
SELECT
  Invoices.INVOICE_ID,
  Invoices.INVOICE_NUMBER,
  Invoices.INVOICE_TYPE_ID,
  InvoiceTypes.TYPE AS INVOICE_TYPE,
  InvoiceTypes.NAME AS INVOICE_TYPE_NAME,
  Invoices.BILL_TO_SITE_USE_ID,
  Customer.ACCOUNT_NUMBER AS BILL_TO_CUSTOMER_NUMBER,
  Customer.ACCOUNT_NAME AS BILL_TO_CUSTOMER_NAME,
  Customer.COUNTRY_NAME AS BILL_TO_CUSTOMER_COUNTRY,
  Invoices.BUSINESS_UNIT_ID,
  BusinessUnit.BUSINESS_UNIT_NAME,
  Invoices.LEDGER_ID,
  Ledger.LEDGER_NAME,
  Invoices.INVOICE_DATE,
  CalDate.CALMONTH AS INVOICE_MONTH_NUM,
  CalDate.CALQUARTER AS INVOICE_QUARTER_NUM,
  CalDate.CALYEAR AS INVOICE_YEAR_NUM,
  COALESCE(Invoices.EXCHANGE_DATE, Invoices.INVOICE_DATE) AS EXCHANGE_DATE,
  Invoices.COMPLETE_FLAG = 'Y' AS IS_COMPLETE,
  Invoices.INVOICE_CURRENCY_CODE AS CURRENCY_CODE,
  JoinedLines.NUM_LINES,
  JoinedLines.NUM_INTERCOMPANY_LINES,
  JoinedLines.TOTAL_TRANSACTION_AMOUNT,
  JoinedLines.TOTAL_REVENUE_AMOUNT,
  JoinedLines.TOTAL_TAX_AMOUNT,
  Invoices.CREATION_TS,
  Invoices.LAST_UPDATE_TS,
  JoinedLines.LINES
FROM
  `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.InvoiceHeaders` AS Invoices
-- Invoice headers without any ORDER ENTRY invoice lines are excluded.
INNER JOIN
  JoinedLines
  ON
    Invoices.INVOICE_ID = JoinedLines.INVOICE_ID
LEFT OUTER JOIN
  `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.InvoiceTypeMD` AS InvoiceTypes
  ON
    Invoices.BUSINESS_UNIT_ID = InvoiceTypes.BUSINESS_UNIT_ID
    AND Invoices.INVOICE_TYPE_ID = InvoiceTypes.INVOICE_TYPE_ID
LEFT OUTER JOIN
  `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.CustomerSiteUseMD` AS Customer
  ON
    Invoices.BILL_TO_SITE_USE_ID = Customer.SITE_USE_ID
LEFT OUTER JOIN
  `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.BusinessUnitMD` AS BusinessUnit
  ON
    Invoices.BUSINESS_UNIT_ID = BusinessUnit.BUSINESS_UNIT_ID
LEFT OUTER JOIN
  `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.LedgerMD` AS Ledger
  ON
    Invoices.LEDGER_ID = Ledger.LEDGER_ID
LEFT OUTER JOIN
  `{{ project_id_src }}.{{ k9_datasets_processing }}.calendar_date_dim` AS CalDate
  ON
    Invoices.INVOICE_DATE = CalDate.DATE
