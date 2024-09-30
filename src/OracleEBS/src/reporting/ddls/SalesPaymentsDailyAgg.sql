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

-- SalesPaymentsDailyAgg aggregate reporting table.

WITH
  PaymentsAgg AS (
    SELECT
      TRANSACTION_DATE,
      ANY_VALUE(TRANSACTION_MONTH_NUM) AS TRANSACTION_MONTH_NUM,
      ANY_VALUE(TRANSACTION_QUARTER_NUM) AS TRANSACTION_QUARTER_NUM,
      ANY_VALUE(TRANSACTION_YEAR_NUM) AS TRANSACTION_YEAR_NUM,
      COALESCE(BILL_TO_SITE_USE_ID, -1) AS BILL_TO_SITE_USE_ID,
      ANY_VALUE(BILL_TO_CUSTOMER_NUMBER) AS BILL_TO_CUSTOMER_NUMBER,
      ANY_VALUE(BILL_TO_CUSTOMER_NAME) AS BILL_TO_CUSTOMER_NAME,
      ANY_VALUE(BILL_TO_CUSTOMER_COUNTRY) AS BILL_TO_CUSTOMER_COUNTRY,
      COALESCE(BUSINESS_UNIT_ID, -1) AS BUSINESS_UNIT_ID,
      ANY_VALUE(BUSINESS_UNIT_NAME) AS BUSINESS_UNIT_NAME,
      PAYMENT_CLASS_CODE,
      ANY_VALUE(IS_PAYMENT_TRANSACTION) AS IS_PAYMENT_TRANSACTION,
      EXCHANGE_DATE,
      CURRENCY_CODE,
      COUNT(PAYMENT_SCHEDULE_ID) AS NUM_PAYMENTS,
      COUNTIF(PAYMENT_STATUS_CODE = 'CL') AS NUM_CLOSED_PAYMENTS,
      SUM(DAYS_TO_PAYMENT) AS TOTAL_DAYS_TO_PAYMENT,
      SUM(AMOUNT_DUE_ORIGINAL) AS TOTAL_ORIGINAL,
      SUM(AMOUNT_DUE_REMAINING) AS TOTAL_REMAINING,
      SUM(
        IF(IS_OPEN_AND_OVERDUE, AMOUNT_DUE_REMAINING, 0))
        AS TOTAL_OVERDUE_REMAINING,
      SUM(
        IF(IS_DOUBTFUL, AMOUNT_DUE_REMAINING, 0))
        AS TOTAL_DOUBTFUL_REMAINING,
      SUM(AMOUNT_DISCOUNTED) AS TOTAL_DISCOUNTED,
      SUM(AMOUNT_APPLIED) AS TOTAL_APPLIED,
      SUM(AMOUNT_CREDITED) AS TOTAL_CREDITED,
      SUM(AMOUNT_ADJUSTED) AS TOTAL_ADJUSTED,
      SUM(TAX_ORIGINAL) AS TOTAL_TAX_ORIGINAL,
      SUM(TAX_REMAINING) AS TOTAL_TAX_REMAINING
    FROM
      `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.SalesPayments`
    GROUP BY
      TRANSACTION_DATE, BILL_TO_SITE_USE_ID, BUSINESS_UNIT_ID, PAYMENT_CLASS_CODE,
      EXCHANGE_DATE, CURRENCY_CODE
  ),
  ConvertedAgg AS (
    SELECT
      PaymentsAgg.TRANSACTION_DATE,
      ANY_VALUE(PaymentsAgg.TRANSACTION_MONTH_NUM) AS TRANSACTION_MONTH_NUM,
      ANY_VALUE(PaymentsAgg.TRANSACTION_QUARTER_NUM) AS TRANSACTION_QUARTER_NUM,
      ANY_VALUE(PaymentsAgg.TRANSACTION_YEAR_NUM) AS TRANSACTION_YEAR_NUM,
      PaymentsAgg.BILL_TO_SITE_USE_ID,
      ANY_VALUE(PaymentsAgg.BILL_TO_CUSTOMER_NUMBER) AS BILL_TO_CUSTOMER_NUMBER,
      ANY_VALUE(PaymentsAgg.BILL_TO_CUSTOMER_NAME) AS BILL_TO_CUSTOMER_NAME,
      ANY_VALUE(PaymentsAgg.BILL_TO_CUSTOMER_COUNTRY) AS BILL_TO_CUSTOMER_COUNTRY,
      PaymentsAgg.BUSINESS_UNIT_ID,
      ANY_VALUE(PaymentsAgg.BUSINESS_UNIT_NAME) AS BUSINESS_UNIT_NAME,
      PaymentsAgg.PAYMENT_CLASS_CODE,
      ANY_VALUE(PaymentsAgg.IS_PAYMENT_TRANSACTION) AS IS_PAYMENT_TRANSACTION,
      SUM(PaymentsAgg.NUM_PAYMENTS) AS NUM_PAYMENTS,
      SUM(PaymentsAgg.NUM_CLOSED_PAYMENTS) AS NUM_CLOSED_PAYMENTS,
      SUM(PaymentsAgg.TOTAL_DAYS_TO_PAYMENT) AS TOTAL_DAYS_TO_PAYMENT,
      TargetCurrs AS TARGET_CURRENCY_CODE,
      -- noqa: disable=LT02
      SUM(
        `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.GetConvertedAmount`(
          PaymentsAgg.CURRENCY_CODE, TargetCurrs,
          CurrRates.CONVERSION_RATE, PaymentsAgg.TOTAL_ORIGINAL))
        AS TOTAL_ORIGINAL,
      SUM(
        `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.GetConvertedAmount`(
          PaymentsAgg.CURRENCY_CODE, TargetCurrs,
          CurrRates.CONVERSION_RATE, PaymentsAgg.TOTAL_REMAINING))
        AS TOTAL_REMAINING,
      SUM(
        `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.GetConvertedAmount`(
          PaymentsAgg.CURRENCY_CODE, TargetCurrs,
          CurrRates.CONVERSION_RATE, PaymentsAgg.TOTAL_OVERDUE_REMAINING))
        AS TOTAL_OVERDUE_REMAINING,
      SUM(
        `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.GetConvertedAmount`(
          PaymentsAgg.CURRENCY_CODE, TargetCurrs,
          CurrRates.CONVERSION_RATE, PaymentsAgg.TOTAL_DOUBTFUL_REMAINING))
        AS TOTAL_DOUBTFUL_REMAINING,
      SUM(
        `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.GetConvertedAmount`(
          PaymentsAgg.CURRENCY_CODE, TargetCurrs,
          CurrRates.CONVERSION_RATE, PaymentsAgg.TOTAL_DISCOUNTED))
        AS TOTAL_DISCOUNTED,
      SUM(
        `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.GetConvertedAmount`(
          PaymentsAgg.CURRENCY_CODE, TargetCurrs,
          CurrRates.CONVERSION_RATE, PaymentsAgg.TOTAL_APPLIED))
        AS TOTAL_APPLIED,
      SUM(
        `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.GetConvertedAmount`(
          PaymentsAgg.CURRENCY_CODE, TargetCurrs,
          CurrRates.CONVERSION_RATE, PaymentsAgg.TOTAL_CREDITED))
        AS TOTAL_CREDITED,
      SUM(
        `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.GetConvertedAmount`(
          PaymentsAgg.CURRENCY_CODE, TargetCurrs,
          CurrRates.CONVERSION_RATE, PaymentsAgg.TOTAL_ADJUSTED))
        AS TOTAL_ADJUSTED,
      SUM(
        `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.GetConvertedAmount`(
          PaymentsAgg.CURRENCY_CODE, TargetCurrs,
          CurrRates.CONVERSION_RATE, PaymentsAgg.TOTAL_TAX_ORIGINAL))
        AS TOTAL_TAX_ORIGINAL,
      SUM(
        `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.GetConvertedAmount`(
          PaymentsAgg.CURRENCY_CODE, TargetCurrs,
          CurrRates.CONVERSION_RATE, PaymentsAgg.TOTAL_TAX_REMAINING))
        AS TOTAL_TAX_REMAINING,
      LOGICAL_OR(PaymentsAgg.CURRENCY_CODE != TargetCurrs AND CurrRates.CONVERSION_RATE IS NULL)
        AS IS_INCOMPLETE_CONVERSION
    -- noqa: enable=LT02
    FROM
      PaymentsAgg
    LEFT JOIN
      UNNEST({{ oracle_ebs_currency_conversion_targets }}) AS TargetCurrs
    LEFT JOIN
      `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.CurrencyRateMD` AS CurrRates
      ON
        (
          COALESCE(PaymentsAgg.EXCHANGE_DATE, PaymentsAgg.TRANSACTION_DATE)
          = CurrRates.CONVERSION_DATE
        )
        AND PaymentsAgg.CURRENCY_CODE = CurrRates.FROM_CURRENCY
        AND TargetCurrs = CurrRates.TO_CURRENCY
    GROUP BY
      -- noqa: disable=RF02
      TRANSACTION_DATE, BILL_TO_SITE_USE_ID, BUSINESS_UNIT_ID, PAYMENT_CLASS_CODE,
      TARGET_CURRENCY_CODE
      -- noqa: enable=RF02
  )
SELECT
  TRANSACTION_DATE,
  ANY_VALUE(TRANSACTION_MONTH_NUM) AS TRANSACTION_MONTH_NUM,
  ANY_VALUE(TRANSACTION_QUARTER_NUM) AS TRANSACTION_QUARTER_NUM,
  ANY_VALUE(TRANSACTION_YEAR_NUM) AS TRANSACTION_YEAR_NUM,
  BILL_TO_SITE_USE_ID,
  ANY_VALUE(BILL_TO_CUSTOMER_NUMBER) AS BILL_TO_CUSTOMER_NUMBER,
  ANY_VALUE(BILL_TO_CUSTOMER_NAME) AS BILL_TO_CUSTOMER_NAME,
  ANY_VALUE(BILL_TO_CUSTOMER_COUNTRY) AS BILL_TO_CUSTOMER_COUNTRY,
  BUSINESS_UNIT_ID,
  ANY_VALUE(BUSINESS_UNIT_NAME) AS BUSINESS_UNIT_NAME,
  PAYMENT_CLASS_CODE,
  ANY_VALUE(IS_PAYMENT_TRANSACTION) AS IS_PAYMENT_TRANSACTION,
  -- These measures use ANY_VALUE because this block is only aggregating the currency amounts
  -- into an array, so non-amount measures should not be re-aggregated to avoid over counting.
  ANY_VALUE(NUM_PAYMENTS) AS NUM_PAYMENTS,
  ANY_VALUE(NUM_CLOSED_PAYMENTS) AS NUM_CLOSED_PAYMENTS,
  ANY_VALUE(TOTAL_DAYS_TO_PAYMENT) AS TOTAL_DAYS_TO_PAYMENT,
  ARRAY_AGG(
    STRUCT(
      TARGET_CURRENCY_CODE,
      TOTAL_ORIGINAL,
      TOTAL_REMAINING,
      TOTAL_OVERDUE_REMAINING,
      TOTAL_DOUBTFUL_REMAINING,
      TOTAL_DISCOUNTED,
      TOTAL_APPLIED,
      TOTAL_CREDITED,
      TOTAL_ADJUSTED,
      TOTAL_TAX_ORIGINAL,
      TOTAL_TAX_REMAINING,
      IS_INCOMPLETE_CONVERSION
    )
  ) AS AMOUNTS
FROM
  ConvertedAgg
GROUP BY TRANSACTION_DATE, BILL_TO_SITE_USE_ID, BUSINESS_UNIT_ID, PAYMENT_CLASS_CODE
