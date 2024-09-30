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

-- SalesAppliedReceivablesDailyAgg aggregate reporting table.

WITH
  -- Get a single amount received for each cash receipt since multiple SalesAppliedReceivables
  -- records can reference the same cash receipt.
  ReceiptAmount AS (
    SELECT
      CASH_RECEIPT_ID,
      ANY_VALUE(CASH_RECEIPT.AMOUNT) AS RECEIVED
    FROM
      `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.SalesAppliedReceivables`
    GROUP BY CASH_RECEIPT_ID
  ),
  ReceivablesAgg AS (
    SELECT
      Receivables.EVENT_DATE,
      ANY_VALUE(Receivables.EVENT_MONTH_NUM) AS EVENT_MONTH_NUM,
      ANY_VALUE(Receivables.EVENT_QUARTER_NUM) AS EVENT_QUARTER_NUM,
      ANY_VALUE(Receivables.EVENT_YEAR_NUM) AS EVENT_YEAR_NUM,
      Receivables.EXCHANGE_DATE,
      Receivables.APPLICATION_TYPE,
      COALESCE(Receivables.BILL_TO_SITE_USE_ID, -1) AS BILL_TO_SITE_USE_ID,
      ANY_VALUE(Receivables.BILL_TO_CUSTOMER_NUMBER) AS BILL_TO_CUSTOMER_NUMBER,
      ANY_VALUE(Receivables.BILL_TO_CUSTOMER_NAME) AS BILL_TO_CUSTOMER_NAME,
      ANY_VALUE(Receivables.BILL_TO_CUSTOMER_COUNTRY) AS BILL_TO_CUSTOMER_COUNTRY,
      COALESCE(Receivables.BUSINESS_UNIT_ID, -1) AS BUSINESS_UNIT_ID,
      ANY_VALUE(Receivables.BUSINESS_UNIT_NAME) AS BUSINESS_UNIT_NAME,
      Receivables.CURRENCY_CODE,
      SUM(ReceiptAmount.RECEIVED) AS TOTAL_RECEIVED,
      SUM(Receivables.AMOUNT_APPLIED) AS TOTAL_APPLIED
    FROM
      `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.SalesAppliedReceivables`
        AS Receivables
    LEFT OUTER JOIN ReceiptAmount
      ON Receivables.CASH_RECEIPT_ID = ReceiptAmount.CASH_RECEIPT_ID
    GROUP BY
      -- noqa: disable=RF02
      EVENT_DATE, EXCHANGE_DATE, APPLICATION_TYPE, BILL_TO_SITE_USE_ID, BUSINESS_UNIT_ID,
      CURRENCY_CODE
      -- noqa: enable=RF02
  ),
  ConvertedAgg AS (
    SELECT
      ReceivablesAgg.EVENT_DATE,
      ANY_VALUE(ReceivablesAgg.EVENT_MONTH_NUM) AS EVENT_MONTH_NUM,
      ANY_VALUE(ReceivablesAgg.EVENT_QUARTER_NUM) AS EVENT_QUARTER_NUM,
      ANY_VALUE(ReceivablesAgg.EVENT_YEAR_NUM) AS EVENT_YEAR_NUM,
      ReceivablesAgg.APPLICATION_TYPE,
      ReceivablesAgg.BILL_TO_SITE_USE_ID,
      ANY_VALUE(ReceivablesAgg.BILL_TO_CUSTOMER_NUMBER) AS BILL_TO_CUSTOMER_NUMBER,
      ANY_VALUE(ReceivablesAgg.BILL_TO_CUSTOMER_NAME) AS BILL_TO_CUSTOMER_NAME,
      ANY_VALUE(ReceivablesAgg.BILL_TO_CUSTOMER_COUNTRY) AS BILL_TO_CUSTOMER_COUNTRY,
      ReceivablesAgg.BUSINESS_UNIT_ID,
      ANY_VALUE(ReceivablesAgg.BUSINESS_UNIT_NAME) AS BUSINESS_UNIT_NAME,
      TargetCurrs AS TARGET_CURRENCY_CODE,
      -- noqa: disable=LT02
      SUM(
        `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.GetConvertedAmount`(
          ReceivablesAgg.CURRENCY_CODE, TargetCurrs,
          CurrRates.CONVERSION_RATE, ReceivablesAgg.TOTAL_RECEIVED))
        AS TOTAL_RECEIVED,
      SUM(
        `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.GetConvertedAmount`(
          ReceivablesAgg.CURRENCY_CODE, TargetCurrs,
          CurrRates.CONVERSION_RATE, ReceivablesAgg.TOTAL_APPLIED))
        AS TOTAL_APPLIED,
      LOGICAL_OR(ReceivablesAgg.CURRENCY_CODE != TargetCurrs AND CurrRates.CONVERSION_RATE IS NULL)
        AS IS_INCOMPLETE_CONVERSION
    -- noqa: enable=LT02
    FROM
      ReceivablesAgg
    LEFT JOIN
      UNNEST({{ oracle_ebs_currency_conversion_targets }}) AS TargetCurrs
    LEFT JOIN
      `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.CurrencyRateMD` AS CurrRates
      ON
        (
          COALESCE(ReceivablesAgg.EXCHANGE_DATE, ReceivablesAgg.EVENT_DATE)
          = CurrRates.CONVERSION_DATE
        )
        AND ReceivablesAgg.CURRENCY_CODE = CurrRates.FROM_CURRENCY
        AND TargetCurrs = CurrRates.TO_CURRENCY
    GROUP BY
      EVENT_DATE, APPLICATION_TYPE, BILL_TO_SITE_USE_ID, BUSINESS_UNIT_ID, TARGET_CURRENCY_CODE -- noqa: RF02
  )
SELECT
  EVENT_DATE,
  ANY_VALUE(EVENT_MONTH_NUM) AS EVENT_MONTH_NUM,
  ANY_VALUE(EVENT_QUARTER_NUM) AS EVENT_QUARTER_NUM,
  ANY_VALUE(EVENT_YEAR_NUM) AS EVENT_YEAR_NUM,
  APPLICATION_TYPE,
  BILL_TO_SITE_USE_ID,
  ANY_VALUE(BILL_TO_CUSTOMER_NUMBER) AS BILL_TO_CUSTOMER_NUMBER,
  ANY_VALUE(BILL_TO_CUSTOMER_NAME) AS BILL_TO_CUSTOMER_NAME,
  ANY_VALUE(BILL_TO_CUSTOMER_COUNTRY) AS BILL_TO_CUSTOMER_COUNTRY,
  BUSINESS_UNIT_ID,
  ANY_VALUE(BUSINESS_UNIT_NAME) AS BUSINESS_UNIT_NAME,
  ARRAY_AGG(
    STRUCT(
      TARGET_CURRENCY_CODE,
      TOTAL_RECEIVED,
      TOTAL_APPLIED,
      IS_INCOMPLETE_CONVERSION
    )
  ) AS AMOUNTS
FROM
  ConvertedAgg
GROUP BY EVENT_DATE, APPLICATION_TYPE, BILL_TO_SITE_USE_ID, BUSINESS_UNIT_ID
