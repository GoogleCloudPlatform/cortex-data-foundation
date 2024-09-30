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

# Update (refresh) stock weekly snapshots table for records between provided dates (inclusive).
#
# `weekly_inventory_aggregation` table must be up to date before calling this function.
# Week start date is defined in the Calendar Date Dimension table (`calendar_date_dim`).
#
# @param start_date Starting date of refresh. Must be the first day of a week.
# @param end_date Ending date of refresh. Must be later than start_date.
CREATE OR REPLACE PROCEDURE
`{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.UpdateStockWeeklySnapshots` (
  start_date DATE,
  end_date DATE
)
BEGIN
  CREATE TEMP TABLE LastWeekSnapshot AS
  SELECT
    * EXCEPT (
      week_end_date,
      cal_year,
      cal_week
    ),
    LAST_DAY(start_date, WEEK) AS week_end_date,
    EXTRACT(YEAR FROM start_date) AS cal_year,
    EXTRACT(WEEK FROM start_date) AS cal_week
  FROM
    `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.stock_weekly_snapshots`
  WHERE
    week_end_date = DATE_SUB(LAST_DAY(start_date, WEEK), INTERVAL 1 WEEK)
    AND
    --skip last incomplete week of the year - will be handled separately
    EXTRACT(YEAR FROM week_end_date) = cal_year;

  CREATE TEMP TABLE CurrentPeriodMovements AS
  SELECT
    *
  FROM
    `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.weekly_inventory_aggregation`
  WHERE
    week_end_date BETWEEN start_date AND end_date
    -- # only include weeks that ends within the current year
    -- # skip last incomplete week of the year - will be handled separately
    -- # i.e. include week 0 till week 51/52 where the last week is complete.
    AND EXTRACT(YEAR FROM week_end_date) = cal_year;

  CREATE TEMP TABLE AllMaterialCombinations AS
  SELECT DISTINCT
    *
  FROM (
    SELECT
      mandt,
      werks,
      matnr,
      charg,
      lgort,
      bukrs,
      meins,
      waers,
      stock_characteristic
    FROM
      LastWeekSnapshot
    UNION ALL
    SELECT
      mandt,
      werks,
      matnr,
      charg,
      lgort,
      bukrs,
      meins,
      waers,
      stock_characteristic
    FROM
      CurrentPeriodMovements
  );

  DELETE FROM
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.stock_weekly_snapshots`
  WHERE
    week_end_date BETWEEN start_date
    AND LAST_DAY(end_date, WEEK);

  INSERT INTO
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.stock_weekly_snapshots`
  (
    mandt, werks, matnr, charg, lgort, bukrs, cal_year, cal_week, meins,
    waers, stock_characteristic, week_end_date,
    total_weekly_movement_quantity,
    total_weekly_movement_amount,
    amount_weekly_cumulative,
    quantity_weekly_cumulative
  )
  WITH
    DateDim AS (
      SELECT DISTINCT
        -- Fix incorrect cal_year and cal_week numbers
        weekEndDate AS week_end_date,
        EXTRACT(YEAR FROM weekEndDate) AS cal_year,
        EXTRACT(WEEK FROM weekEndDate) AS cal_week
      FROM
        `{{ project_id_src }}.{{ k9_datasets_processing }}.calendar_date_dim`
      WHERE
        date BETWEEN start_date AND end_date
    ),
    WeeklyCumulativeRaw AS (
      SELECT
        week_end_date,
        cal_year,
        cal_week,
        mandt,
        werks,
        matnr,
        charg,
        lgort,
        bukrs,
        meins,
        waers,
        stock_characteristic,
        total_weekly_movement_amount,  -- noqa: RF02
        total_weekly_movement_quantity  -- noqa: RF02
      -- # this ensures all week / material combinations exist even if they
      -- # don’t exist in current movements
      FROM
        DateDim
      CROSS JOIN
        AllMaterialCombinations
      LEFT JOIN
        CurrentPeriodMovements
        USING
          (
            week_end_date,
            cal_year,
            cal_week,
            mandt,
            werks,
            matnr,
            charg,
            lgort,
            bukrs,
            meins,
            waers,
            stock_characteristic
          )
      UNION ALL
      --# take balance of last week and treat it as beginning balance
      --# of the first week
      SELECT
        week_end_date,
        cal_year,
        cal_week,
        mandt,
        werks,
        matnr,
        charg,
        lgort,
        bukrs,
        meins,
        waers,
        stock_characteristic,
        amount_weekly_cumulative AS total_weekly_movement_amount,
        quantity_weekly_cumulative AS total_weekly_movement_quantity
      FROM
        LastWeekSnapshot
    ),
    WeeklyCumulative AS (
      SELECT
        week_end_date,
        cal_year,
        cal_week,
        mandt,
        werks,
        matnr,
        charg,
        lgort,
        bukrs,
        meins,
        waers,
        stock_characteristic,
        SUM(total_weekly_movement_quantity) AS total_weekly_movement_quantity_sum,
        SUM(total_weekly_movement_amount) AS total_weekly_movement_amount_sum
      FROM
        WeeklyCumulativeRaw
      GROUP BY
        week_end_date,
        cal_year,
        cal_week,
        mandt,
        werks,
        matnr,
        charg,
        lgort,
        bukrs,
        meins,
        waers,
        stock_characteristic
    )
  SELECT
    mandt,
    werks,
    matnr,
    charg,
    lgort,
    bukrs,
    cal_year,
    cal_week,
    meins,
    waers,
    stock_characteristic,
    week_end_date,
    COALESCE(total_weekly_movement_quantity, 0) AS total_weekly_movement_quantity,  -- noqa: RF02
    COALESCE(total_weekly_movement_amount, 0) AS total_weekly_movement_amount,  -- noqa: RF02
    SUM(total_weekly_movement_amount_sum) OVER (  -- noqa: RF02
      PARTITION BY
        mandt,
        matnr,
        werks,
        charg,
        lgort,
        bukrs,
        meins,
        waers,
        stock_characteristic
      ORDER BY week_end_date ASC
      ROWS UNBOUNDED PRECEDING
    ) AS amount_weekly_cumulative,
    SUM(total_weekly_movement_quantity_sum) OVER (  -- noqa: RF02
      PARTITION BY
        mandt,
        matnr,
        werks,
        charg,
        lgort,
        bukrs,
        meins,
        waers,
        stock_characteristic
      ORDER BY week_end_date ASC
      ROWS UNBOUNDED PRECEDING
    ) AS quantity_weekly_cumulative
  FROM
    WeeklyCumulative
  LEFT JOIN
    CurrentPeriodMovements
    USING
      (
        week_end_date,
        cal_year,
        cal_week,
        mandt,
        werks,
        matnr,
        charg,
        lgort,
        bukrs,
        meins,
        waers,
        stock_characteristic
      )
  QUALIFY
    amount_weekly_cumulative IS NOT NULL OR quantity_weekly_cumulative IS NOT NULL;

END;
