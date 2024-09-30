#-- Copyright 2023 Google LLC
#--
#-- Licensed under the Apache License, Version 2.0 (the "License");
#-- you may not use this file except in compliance with the License.
#-- You may obtain a copy of the License at
#--
#--     https://www.apache.org/licenses/LICENSE-2.0
#--
#-- Unless required by applicable law or agreed to in writing, software
#-- distributed under the License is distributed on an "AS IS" BASIS,
#-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#-- See the License for the specific language governing permissions and
#-- limitations under the License.

--## CORTEX-CUSTOMER: The fiscal table solution works only for those variants(periv)
-- where we have fiscal year as a continuous calendar year. Consider commenting/uncommenting
-- a specific fiscal case as per your requirement.

--CASE 1: If t009.xkale = 'X', Fiscal year is the same as calendar year.
(
  SELECT
    mandt,
    periv,
    dt AS Date,
    CAST(FORMAT_DATE('%Y%m%d', dt) AS INT64) AS DateInt,
    FORMAT_DATE('%Y%m%d', dt) AS DateStr,
    FORMAT_DATE('0%m', dt) AS FiscalPeriod,
    FORMAT_DATE('%Y', dt) AS FiscalYear,
    FORMAT_DATE('%Y0%m', dt) AS FiscalYearPeriod,
    DATE_TRUNC(dt, YEAR) AS FiscalYearFirstDay,
    LAST_DAY(dt, YEAR) AS FiscalYearLastDay,
    IF(EXTRACT(QUARTER FROM dt) IN (1, 2), 1, 2) AS FiscalSemester,
    IF(EXTRACT(QUARTER FROM dt) IN (1, 2), '01', '02') AS FiscalSemesterStr,
    IF(EXTRACT(QUARTER FROM dt) IN (1, 2), '1st Semester', '2nd Semester') AS FiscalSemesterStr2,
    EXTRACT(QUARTER FROM dt) AS FiscalQuarter,
    '0' || EXTRACT(QUARTER FROM dt) AS FiscalQuarterStr,
    'Q' || EXTRACT(QUARTER FROM dt) AS FiscalQuarterStr2,
    EXTRACT(WEEK FROM dt) AS FiscalWeek,
    '0' || EXTRACT(WEEK FROM dt) AS FiscalWeekStr,
    DATE_TRUNC(dt, WEEK) AS WeekStartDate,
    LAST_DAY(dt, WEEK) AS WeekEndDate,
    FORMAT_DATE('%A', dt) AS DayNameLong,
    FORMAT_DATE('%a', dt) AS DayNameShort
  FROM
    `{{ project_id_src }}.{{ dataset_cdc_processed }}.t009`,
    UNNEST(
      GENERATE_DATE_ARRAY(
        DATE_SUB(DATE_TRUNC(CURRENT_DATE(), YEAR), INTERVAL 10 YEAR),
        DATE_ADD(LAST_DAY(CURRENT_DATE(), YEAR), INTERVAL 10 YEAR),
        INTERVAL 1 DAY)
    ) AS dt
  WHERE
    xkale = 'X'
    AND mandt = '{{ mandt }}'
)

UNION ALL
--CASE 2: If t009.xjabh = 'X', Fiscal year is not a calendar year and has fiscal variants.
(
  WITH
    -- Calculate end date for each fiscal year and period combination.
    GetEndDate AS (
      SELECT
        t009.mandt,
        t009.periv,
        t009b.bdatj,
        t009b.bumon,
        t009b.butag,
        t009b.reljr,
        t009b.poper,
        CAST(MIN(t009b.bumon) OVER (PARTITION BY t009.mandt, t009.periv, t009b.bdatj) AS INT64)
          AS MinimumBumon,
        CAST(CONCAT(t009b.bdatj, '-', t009b.bumon, '-', t009b.butag) AS DATE) AS EndDate
      FROM
        `{{ project_id_src }}.{{ dataset_cdc_processed }}.t009` AS t009
      INNER JOIN
        `{{ project_id_src }}.{{ dataset_cdc_processed }}.t009b` AS t009b
        ON
          t009.mandt = t009b.mandt
          AND t009.periv = t009b.periv
      WHERE t009.xjabh = 'X'
        AND t009.mandt = '{{ mandt }}'
    ),

    -- Calculate start date according to fiscal year and period combination.
    GetStartDate AS (
      SELECT
        mandt,
        periv,
        bdatj,
        bumon,
        butag,
        reljr,
        poper,
        EndDate,
        -- Calculate start date based on end date.
        COALESCE(
          DATE_ADD(
            LAG(EndDate) OVER (PARTITION BY mandt, periv ORDER BY bdatj, bumon, butag),
            INTERVAL 1 DAY),
          DATE_TRUNC(EndDate, MONTH)
        ) AS StartDate
      FROM GetEndDate
    ),

    -- Calculate Fiscal Year First and Last Day.
    GetFirstAndLastDay AS (
      SELECT
        mandt,
        periv,
        poper,
        dt,
        MIN(dt) OVER (PARTITION BY mandt, periv, CAST(bdatj AS INT64) + CAST(reljr AS INT64))
          AS FiscalYearFirstDay,
        MAX(dt) OVER (PARTITION BY mandt, periv, CAST(bdatj AS INT64) + CAST(reljr AS INT64))
          AS FiscalYearLastDay,
        LAST_DAY(dt, WEEK) AS WeekEndDate,
        DATE_TRUNC(dt, WEEK) AS WeekStartDate,
        CAST(bdatj AS INT64) + CAST(reljr AS INT64) AS FiscalYear
      FROM GetStartDate,
        UNNEST(GENERATE_DATE_ARRAY(StartDate, EndDate)) AS dt
    ),

    -- Calculate aggregated fields, i.e, FiscalSemester,FiscalWeek and FiscalQuarter.
    GetAggFields AS (
      SELECT
        mandt,
        periv,
        dt AS Date,
        FiscalYearFirstDay,
        FiscalYearLastDay,
        FiscalYear AS FiscalYearInt,
        WeekStartDate,
        WeekEndDate,
        poper AS FiscalPeriod,
        CAST(FiscalYear AS STRING) AS FiscalYear,
        CAST(FORMAT_DATE('%Y%m%d', dt) AS INT64) AS DateInt,
        FORMAT_DATE('%Y%m%d', dt) AS DateStr,
        CASE
          WHEN dt >= FiscalYearFirstDay
            AND dt < DATE_ADD(FiscalYearFirstDay, INTERVAL 6 MONTH)
            THEN 1
          WHEN dt >= DATE_ADD(FiscalYearFirstDay, INTERVAL 6 MONTH)
            AND dt <= FiscalYearLastDay
            THEN 2
        END AS FiscalSemester,
        CASE
          WHEN dt >= FiscalYearFirstDay
            AND dt < DATE_ADD(FiscalYearFirstDay, INTERVAL 3 MONTH)
            THEN 1
          WHEN dt >= DATE_ADD(FiscalYearFirstDay, INTERVAL 3 MONTH)
            AND dt < DATE_ADD(FiscalYearFirstDay, INTERVAL 6 MONTH)
            THEN 2
          WHEN dt >= DATE_ADD(FiscalYearFirstDay, INTERVAL 6 MONTH)
            AND dt < DATE_ADD(FiscalYearFirstDay, INTERVAL 9 MONTH)
            THEN 3
          WHEN dt >= DATE_ADD(FiscalYearFirstDay, INTERVAL 9 MONTH)
            AND dt <= FiscalYearLastDay
            THEN 4
        END AS FiscalQuarter,
        CAST(
          IF(
            FORMAT_DATE('%A', FiscalYearFirstDay) = 'Sunday',
            CEIL(SAFE_DIVIDE(DATE_DIFF(WeekEndDate, FiscalYearFirstDay, DAY), 7)),
            FLOOR(SAFE_DIVIDE(DATE_DIFF(WeekEndDate, FiscalYearFirstDay, DAY), 7))
          ) AS INT64) AS FiscalWeek,
        FORMAT_DATE('%A', dt) AS DayNameLong,
        FORMAT_DATE('%a', dt) AS DayNameShort,
        CONCAT(FiscalYear, poper) AS FiscalYearPeriod
      FROM GetFirstAndLastDay
    )
  SELECT
    mandt,
    periv,
    Date,
    DateInt,
    DateStr,
    FiscalPeriod,
    FiscalYear,
    FiscalYearPeriod,
    FiscalYearFirstDay,
    FiscalYearLastDay,
    FiscalSemester,
    CAST(CONCAT(0, FiscalSemester) AS STRING) AS FiscalSemesterStr,
    IF(FiscalSemester = 1, '1st Semester', '2nd Semester') AS FiscalSemesterStr2,
    FiscalQuarter,
    CAST(CONCAT(0, FiscalQuarter) AS STRING) AS FiscalQuarterStr,
    CAST(CONCAT('Q', FiscalQuarter) AS STRING) AS FiscalQuarterStr2,
    FiscalWeek,
    LPAD(CAST(FiscalWeek AS STRING), 2, '0') AS FiscalWeekStr,
    WeekStartDate,
    WeekEndDate,
    DayNameLong,
    DayNameShort
  FROM GetAggFields
)

UNION ALL
--CASE 3: If t009.xkale IS NULL AND t009.xjabh IS NULL,
--Indicates that the fiscal calendar is grouped into custom fiscal variants.
--It has two scenarios, i.e, t009b.bdatj = '0000' and t0009b.bdatj != '0000'.
(
  WITH
    -- Calculate end date for each fiscal year and period combination for t009b.bdatj = '0000'
    GetEndDateForFirstScenario AS (
      SELECT
        t009.mandt,
        t009.periv,
        cal_year AS bdatj,
        t009b.bumon,
        t009b.butag,
        t009b.reljr,
        t009b.poper,
        CAST(MIN(t009b.bumon) OVER (PARTITION BY t009.mandt, t009.periv) AS INT64)
          AS MinimumBumon,
        SUBSTR(
          MIN(CONCAT(t009b.poper, '-', t009b.bumon, '-', t009b.butag))
            OVER (PARTITION BY t009.mandt, t009.periv), 5)
            AS MinimumPeriod,
        SUBSTR(
          MAX(CONCAT(t009b.poper, '-', t009b.bumon, '-', t009b.butag))
            OVER (PARTITION BY t009.mandt, t009.periv), 5)
            AS MaximumPeriod,
        --If the year is not a leap year,updating end date for Feb as '28 Feb'
        CASE
          WHEN NOT `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.is_leap_year`(cal_year)
            AND t009b.bumon = '02' AND t009b.butag = '29'
            THEN CAST(CONCAT(cal_year, '-', t009b.bumon, '-28') AS DATE)
          ELSE CAST(CONCAT(cal_year, '-', t009b.bumon, '-', t009b.butag) AS DATE)
        END AS EndDate
      FROM
        `{{ project_id_src }}.{{ dataset_cdc_processed }}.t009` AS t009
      INNER JOIN
        `{{ project_id_src }}.{{ dataset_cdc_processed }}.t009b` AS t009b
        ON
          t009.mandt = t009b.mandt
          AND t009.periv = t009b.periv,
        UNNEST(
          GENERATE_ARRAY(
            EXTRACT(YEAR FROM DATE_SUB(CURRENT_DATE(), INTERVAL 10 YEAR)),
            EXTRACT(YEAR FROM DATE_ADD(CURRENT_DATE(), INTERVAL 10 YEAR))
          )) AS cal_year
      WHERE t009.mandt = '{{ mandt }}'
        AND t009.xkale IS NULL
        AND t009.xjabh IS NULL
        AND t009b.bdatj = '0000'
    ),

    -- Calculate start date according to fiscal year and period combination for t009b.bdatj = '0000'
    GetStartDateForFirstScenario AS (
      SELECT
        mandt,
        periv,
        bdatj,
        bumon,
        butag,
        reljr,
        poper,
        EndDate,
        MinimumBumon,
        CAST(bdatj AS INT64) + CAST(reljr AS INT64) AS FiscalYear,
        MIN(reljr) OVER (PARTITION BY mandt, periv) AS MinimumReljr,
        -- If the year is not a leap year,updating min period for Feb as '28 Feb'
        IF(
          NOT `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.is_leap_year`(bdatj)
            AND MinimumPeriod = '02-29',
          REPLACE(MinimumPeriod, '29', '28'),
          MinimumPeriod) AS MinimumPeriod,
        -- If the year is not a leap year,updating max period for Feb as '28 Feb'
        IF(
          NOT `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.is_leap_year`(bdatj)
            AND MaximumPeriod = '02-29',
          REPLACE(MaximumPeriod, '29', '28'),
          MaximumPeriod) AS MaximumPeriod,
        -- Calculate start date accordingly in case fiscal period is greater than 1 month.
        IF(
          MinimumBumon = 01,
          COALESCE(
            DATE_ADD(
              LAG(EndDate) OVER (PARTITION BY mandt, periv ORDER BY bdatj, bumon, butag),
              INTERVAL 1 DAY),
            DATE_TRUNC(EndDate, MONTH)),
          DATE_SUB(DATE_TRUNC(EndDate, MONTH), INTERVAL (MinimumBumon - 1) MONTH)
        ) AS StartDate
      FROM GetEndDateForFirstScenario
    ),

    -- Calculate Fiscal Year First and Last Day.
    GetFirstAndLastDayForFirstScenario AS (
      SELECT
        mandt,
        periv,
        poper,
        dt,
        reljr,
        FiscalYear,
        -- Fiscal Year First Day is calculated according to reljr, if reljr is '+ year',
        -- fiscal year starts in previous calendar year based on the reljr.
        -- For remaining two cases of reljr, fiscal year starts in the same year as of calendar year.
        DATE_SUB(
          DATE_TRUNC(
            CAST(
              CASE
                WHEN MinimumReljr = '0'
                  THEN CONCAT(FiscalYear, '-', MinimumPeriod)
                WHEN SUBSTR(MinimumReljr, 1, 1) = '+'
                  THEN CONCAT(FiscalYear - CAST(SUBSTR(MinimumReljr, 2, 1) AS INT64), '-', MinimumPeriod)
                WHEN SUBSTR(MinimumReljr, 1, 1) = '-'
                  THEN CONCAT(FiscalYear, '-', MinimumPeriod)
              END AS DATE),
            MONTH),
          INTERVAL (MinimumBumon - 1) MONTH) AS FiscalYearFirstDay,
        -- Fiscal Year Last Day is calculated according to reljr, if reljr is '- year',
        -- fiscal year ends in next calendar year based on the reljr.
        -- For remaining two cases of reljr, fiscal year ends in the same year as of calendar year.
        CAST(
          CASE
            WHEN MinimumReljr = '0'
              THEN CONCAT(FiscalYear, '-', MaximumPeriod)
            WHEN SUBSTR(MinimumReljr, 1, 1) = '+'
              THEN CONCAT(FiscalYear, '-', MaximumPeriod)
            WHEN SUBSTR(MinimumReljr, 1, 1) = '-'
              THEN CONCAT(FiscalYear + CAST(SUBSTR(MinimumReljr, 2, 1) AS INT64), '-', MaximumPeriod)
          END AS DATE) AS FiscalYearLastDay,
        DATE_TRUNC(dt, WEEK) AS WeekStartDate,
        LAST_DAY(dt, WEEK) AS WeekEndDate
      FROM GetStartDateForFirstScenario,
        UNNEST(GENERATE_DATE_ARRAY(StartDate, EndDate)) AS dt
    ),

    -- Calculate end date for each fiscal year and period combination for t009b.bdatj != '0000'
    GetEndDateForSecondScenario AS (
      SELECT
        t009.mandt,
        t009.periv,
        t009b.bdatj,
        t009b.bumon,
        t009b.butag,
        t009b.reljr,
        t009b.poper,
        CAST(MIN(t009b.bumon) OVER (PARTITION BY t009.mandt, t009.periv, t009b.bdatj) AS INT64)
          AS MinimumBumon,
        CAST(CONCAT(t009b.bdatj, '-', t009b.bumon, '-', t009b.butag) AS DATE) AS EndDate
      FROM
        `{{ project_id_src }}.{{ dataset_cdc_processed }}.t009` AS t009
      INNER JOIN
        `{{ project_id_src }}.{{ dataset_cdc_processed }}.t009b` AS t009b
        ON
          t009.mandt = t009b.mandt
          AND t009.periv = t009b.periv
      WHERE t009.xkale IS NULL
        AND t009.xjabh IS NULL
        AND t009.mandt = '{{ mandt }}'
        AND t009b.bdatj != '0000'
    ),

    -- Calculate start date according to fiscal year and period combination for t009b.bdatj != '0000'
    GetStartDateForSecondScenario AS (
      SELECT
        mandt,
        periv,
        bdatj,
        bumon,
        butag,
        reljr,
        poper,
        EndDate,
        -- Calculate start date accordingly in case fiscal period is greater than 1 month.
        IF(
          MinimumBumon = 01,
          COALESCE(
            DATE_ADD(
              LAG(EndDate) OVER (PARTITION BY mandt, periv ORDER BY bdatj, bumon, butag),
              INTERVAL 1 DAY),
            DATE_TRUNC(EndDate, MONTH)),
          DATE_SUB(DATE_TRUNC(EndDate, MONTH), INTERVAL (MinimumBumon - 1) MONTH)
        ) AS StartDate
      FROM GetEndDateForSecondScenario
    ),

    -- Calculate Fiscal Year First and Last Day.
    GetFirstAndLastDayForSecondScenario AS (
      SELECT
        mandt,
        periv,
        poper,
        dt,
        reljr,
        CAST(bdatj AS INT64) + CAST(reljr AS INT64) AS FiscalYear,
        MIN(dt) OVER (PARTITION BY mandt, periv, CAST(bdatj AS INT64) + CAST(reljr AS INT64))
          AS FiscalYearFirstDay,
        MAX(dt) OVER (PARTITION BY mandt, periv, CAST(bdatj AS INT64) + CAST(reljr AS INT64))
          AS FiscalYearLastDay,
        DATE_TRUNC(dt, WEEK) AS WeekStartDate,
        LAST_DAY(dt, WEEK) AS WeekEndDate
      FROM GetStartDateForSecondScenario,
        UNNEST(GENERATE_DATE_ARRAY(StartDate, EndDate)) AS dt
    ),

    --Combining the records of both scenarios, i.e, t009b.bdatj ='0000' and t009b.bdatj != '0000'
    CombineBothScenarios AS (
      SELECT * FROM GetFirstAndLastDayForFirstScenario
      UNION ALL
      SELECT * FROM GetFirstAndLastDayForSecondScenario
    ),

    -- Calculate aggregated fields, i.e, FiscalSemester,FiscalWeek and FiscalQuarter.
    GetAggFields AS (
      SELECT
        mandt,
        periv,
        dt AS Date,
        FiscalYearFirstDay,
        FiscalYearLastDay,
        WeekStartDate,
        WeekEndDate,
        poper AS FiscalPeriod,
        CAST(FiscalYear AS STRING) AS FiscalYear,
        CAST(FORMAT_DATE('%Y%m%d', dt) AS INT64) AS DateInt,
        FORMAT_DATE('%Y%m%d', dt) AS DateStr,
        CASE
          WHEN dt >= FiscalYearFirstDay
            AND dt < DATE_ADD(FiscalYearFirstDay, INTERVAL 6 MONTH)
            THEN 1
          WHEN dt >= DATE_ADD(FiscalYearFirstDay, INTERVAL 6 MONTH)
            AND dt <= FiscalYearLastDay
            THEN 2
        END AS FiscalSemester,
        CASE
          WHEN dt >= FiscalYearFirstDay
            AND dt < DATE_ADD(FiscalYearFirstDay, INTERVAL 3 MONTH)
            THEN 1
          WHEN dt >= DATE_ADD(FiscalYearFirstDay, INTERVAL 3 MONTH)
            AND dt < DATE_ADD(FiscalYearFirstDay, INTERVAL 6 MONTH)
            THEN 2
          WHEN dt >= DATE_ADD(FiscalYearFirstDay, INTERVAL 6 MONTH)
            AND dt < DATE_ADD(FiscalYearFirstDay, INTERVAL 9 MONTH)
            THEN 3
          WHEN dt >= DATE_ADD(FiscalYearFirstDay, INTERVAL 9 MONTH)
            AND dt <= FiscalYearLastDay
            THEN 4
        END AS FiscalQuarter,
        IF(
          FORMAT_DATE('%A', FiscalYearFirstDay) = 'Sunday',
          CAST(CEIL(SAFE_DIVIDE(DATE_DIFF(WeekEndDate, FiscalYearFirstDay, DAY), 7)) AS INT64),
          CAST(FLOOR(SAFE_DIVIDE(DATE_DIFF(WeekEndDate, FiscalYearFirstDay, DAY), 7)) AS INT64)
        ) AS FiscalWeek,
        FORMAT_DATE('%A', dt) AS DayNameLong,
        FORMAT_DATE('%a', dt) AS DayNameShort,
        CONCAT(FiscalYear, poper) AS FiscalYearPeriod
      FROM CombineBothScenarios
    )
  SELECT
    mandt,
    periv,
    Date,
    DateInt,
    DateStr,
    FiscalPeriod,
    FiscalYear,
    FiscalYearPeriod,
    FiscalYearFirstDay,
    FiscalYearLastDay,
    FiscalSemester,
    CAST(CONCAT(0, FiscalSemester) AS STRING) AS FiscalSemesterStr,
    IF(FiscalSemester = 1, '1st Semester', '2nd Semester') AS FiscalSemesterStr2,
    FiscalQuarter,
    CAST(CONCAT(0, FiscalQuarter) AS STRING) AS FiscalQuarterStr,
    CAST(CONCAT('Q', FiscalQuarter) AS STRING) AS FiscalQuarterStr2,
    FiscalWeek,
    LPAD(CAST(FiscalWeek AS STRING), 2, '0') AS FiscalWeekStr,
    WeekStartDate,
    WeekEndDate,
    DayNameLong,
    DayNameShort
  FROM GetAggFields
  --## CORTEX-CUSTOMER: The fiscal table solution works for only those variants(periv)
  -- where we have fiscal year as a continuous calendar year.
  WHERE periv NOT IN ('C2')
)
