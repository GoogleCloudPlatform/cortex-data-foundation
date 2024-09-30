# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

## PREVIEW

# Update (refresh) stock monthly snapshots table for records between provided dates (inclusive).
#
# `monthly_inventory_aggregation` table must be up to date before calling this function.
#
# @param start_date Starting date of refresh. Must be the first day of a month.
# @param end_date Ending date of refresh. Must be later than start_date.
CREATE OR REPLACE PROCEDURE
`{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.UpdateStockMonthlySnapshots` (
  start_date DATE,
  end_date DATE
)
BEGIN
  CREATE TEMP TABLE LastMonthSnapshot AS
  SELECT
    *
    EXCEPT (
      month_end_date,
      cal_year,
      cal_month
    ),
    LAST_DAY(start_date, MONTH) AS month_end_date,
    EXTRACT(YEAR FROM start_date) AS cal_year,
    EXTRACT(MONTH FROM start_date) AS cal_month
  FROM
    `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.stock_monthly_snapshots`
  WHERE
    month_end_date = DATE_SUB(start_date, INTERVAL 1 DAY);

  CREATE TEMP TABLE CurrentPeriodMovements AS
  SELECT *
  FROM
    `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.monthly_inventory_aggregation`
  WHERE
    month_end_date BETWEEN start_date AND end_date;

  CREATE TEMP TABLE AllMaterialCombinations AS
  SELECT DISTINCT *
  FROM (
    SELECT
      mandt,
      werks,
      matnr,
      charg,
      lgort,
      bukrs,
      bwart,
      insmk,
      sobkz,
      shkzg,
      meins,
      bstaus_sg,
      waers
    FROM
      LastMonthSnapshot
    UNION ALL
    SELECT
      mandt,
      werks,
      matnr,
      charg,
      lgort,
      bukrs,
      bwart,
      insmk,
      sobkz,
      shkzg,
      meins,
      bstaus_sg,
      waers
    FROM
      CurrentPeriodMovements
  );

  DELETE
  FROM
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.stock_monthly_snapshots`
  WHERE
    month_end_date BETWEEN start_date AND end_date;

  INSERT INTO
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.stock_monthly_snapshots`
  (
    mandt, werks, matnr, charg, lgort, bukrs, bwart, insmk, sobkz,
    shkzg, cal_year, cal_month, meins,
    bstaus_sg,
    waers, month_end_date,
    total_monthly_movement_quantity,
    total_monthly_movement_amount,
    amount_monthly_cumulative,
    quantity_monthly_cumulative
  )
  WITH
    DateDim AS (
      SELECT DISTINCT
        calyear AS cal_year,
        calmonth AS cal_month,
        monthenddate AS month_end_date
      FROM
        `{{ project_id_src }}.{{ k9_datasets_processing }}.calendar_date_dim`
      WHERE
        date BETWEEN start_date AND end_date
    ),
    MonthlyCumulativeRaw AS (
      SELECT
        month_end_date,
        cal_year,
        cal_month,
        mandt,
        werks,
        matnr,
        charg,
        lgort,
        bukrs,
        bwart,
        insmk,
        sobkz,
        shkzg,
        meins,
        bstaus_sg,
        waers,
        total_monthly_movement_amount,  -- noqa: RF02
        total_monthly_movement_quantity  -- noqa: RF02
      -- this ensures all month / material combinations exist even if
      -- they don’t exist in current movements
      FROM
        DateDim
      CROSS JOIN
        AllMaterialCombinations
      LEFT JOIN
        CurrentPeriodMovements
        USING
          (
            month_end_date,
            cal_year,
            cal_month,
            mandt,
            werks,
            matnr,
            charg,
            lgort,
            bukrs,
            bwart,
            insmk,
            sobkz,
            shkzg,
            meins,
            bstaus_sg,
            waers
          )
      UNION ALL
      -- take balance of last month and treat it as beginning balance of the first month
      SELECT
        month_end_date,
        cal_year,
        cal_month,
        mandt,
        werks,
        matnr,
        charg,
        lgort,
        bukrs,
        bwart,
        insmk,
        sobkz,
        shkzg,
        meins,
        bstaus_sg,
        waers,
        amount_monthly_cumulative AS total_monthly_movement_amount,
        quantity_monthly_cumulative AS total_monthly_movement_quantity
      FROM
        LastMonthSnapshot
    ),
    MonthlyCumulative AS (
      SELECT
        month_end_date,
        cal_year,
        cal_month,
        mandt,
        werks,
        matnr,
        charg,
        lgort,
        bukrs,
        bwart,
        insmk,
        sobkz,
        shkzg,
        meins,
        bstaus_sg,
        waers,
        SUM(total_monthly_movement_quantity) AS total_monthly_movement_quantity_sum,
        SUM(total_monthly_movement_amount) AS total_monthly_movement_amount_sum
      FROM
        MonthlyCumulativeRaw
      GROUP BY
        month_end_date,
        cal_year,
        cal_month,
        mandt,
        werks,
        matnr,
        charg,
        lgort,
        bukrs,
        bwart,
        insmk,
        sobkz,
        shkzg,
        meins,
        bstaus_sg,
        waers
    )
  SELECT
    mandt,
    werks,
    matnr,
    charg,
    lgort,
    bukrs,
    bwart,
    insmk,
    sobkz,
    shkzg,
    cal_year,
    cal_month,
    meins,
    bstaus_sg,
    waers,
    month_end_date,
    COALESCE(total_monthly_movement_quantity, 0) AS total_monthly_movement_quantity,  -- noqa: RF02
    COALESCE(total_monthly_movement_amount, 0) AS total_monthly_movement_amount,  -- noqa: RF02
    SUM(total_monthly_movement_amount_sum)  -- noqa: RF02
      OVER (
        PARTITION BY
          mandt,
          matnr,
          werks,
          charg,
          lgort,
          bukrs,
          bwart,
          insmk,
          sobkz,
          shkzg,
          meins,
          bstaus_sg,
          waers
        ORDER BY month_end_date ASC
        ROWS UNBOUNDED PRECEDING
      ) AS amount_monthly_cumulative,
    SUM(total_monthly_movement_quantity_sum)  -- noqa: RF02
      OVER (
        PARTITION BY
          mandt,
          matnr,
          werks,
          charg,
          lgort,
          bukrs,
          bwart,
          insmk,
          sobkz,
          shkzg,
          meins,
          bstaus_sg,
          waers
        ORDER BY month_end_date ASC
        ROWS UNBOUNDED PRECEDING
      ) AS quantity_monthly_cumulative
  FROM
    MonthlyCumulative
  LEFT JOIN
    CurrentPeriodMovements
    USING
      (
        month_end_date,
        cal_year,
        cal_month,
        mandt,
        werks,
        matnr,
        charg,
        lgort,
        bukrs,
        bwart,
        insmk,
        sobkz,
        shkzg,
        meins,
        bstaus_sg,
        waers
      )
  QUALIFY
    amount_monthly_cumulative IS NOT NULL OR quantity_monthly_cumulative IS NOT NULL;

END;
