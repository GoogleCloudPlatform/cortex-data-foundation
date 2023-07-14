# Copyright 2023 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     https://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

/*
*This file a date dimension contains a continuous range of dates that cover the entire date period required for the analysis.
* It also includes columns that will allow a user to filter the data by almost any date logic.
* It can include the day of the week, workdays, weekends, quarters, months, years, or seasons.
*/
CREATE OR REPLACE TABLE `{{ project_id_src }}.{{ k9_datasets_processing }}.calendar_date_dim` AS
SELECT
  dt AS Date,
  CAST(FORMAT_DATE('%Y%m%d', dt) AS INT64) AS DateInt,
  FORMAT_DATE('%Y%m%d', dt) AS DateStr,
  FORMAT_DATE('%Y-%m-%d', dt) AS DateStr2,
  EXTRACT(YEAR FROM dt) AS CalYear,
  IF(EXTRACT(QUARTER FROM dt) IN (1, 2), 1, 2) AS CalSemester,
  EXTRACT(QUARTER FROM dt) AS CalQuarter,
  EXTRACT(MONTH FROM dt) AS CalMonth,
  EXTRACT(WEEK FROM dt) AS CalWeek,
  CAST(EXTRACT(YEAR FROM dt) AS STRING) AS CalYearStr,
  IF(EXTRACT(QUARTER FROM dt) IN (1, 2), '01', '02') AS CalSemesterStr,
  IF(EXTRACT(QUARTER FROM dt) IN (1, 2), 'S1', 'S2') AS CalSemesterStr2,
  '0' || EXTRACT(QUARTER FROM dt) AS CalQuarterStr,
  'Q' || EXTRACT(QUARTER FROM dt) AS CalQuarterStr2,
  FORMAT_DATE('%B', dt) AS CalMonthLongStr,
  FORMAT_DATE('%b', dt) AS CalMonthShortStr,
  '0' || (EXTRACT(WEEK FROM dt)) AS CalWeekStr,
  FORMAT_DATE('%A', dt) AS DayNameLong,
  FORMAT_DATE('%a', dt) AS DayNameShort,
  EXTRACT(DAYOFWEEK FROM dt) AS DayOfWeek,
  EXTRACT(DAY FROM dt) AS DayOfMonth,
  DATE_DIFF(dt, DATE_TRUNC(dt, QUARTER), DAY) + 1 AS DayOfQuarter,
  IF(
    EXTRACT(QUARTER FROM dt) IN (1, 2),
    EXTRACT(DAYOFYEAR FROM dt),
    IF(
      EXTRACT(QUARTER FROM dt) = 3,
      EXTRACT(DAYOFYEAR FROM dt) - EXTRACT(DAYOFYEAR FROM (DATE_TRUNC(dt, QUARTER) - 1)),
      EXTRACT(DAYOFYEAR FROM dt) - EXTRACT(DAYOFYEAR FROM (DATE_TRUNC(DATE_SUB(dt, INTERVAL 3 MONTH), QUARTER)))
    )
  ) AS DayOfSemester,
  EXTRACT(DAYOFYEAR FROM dt) AS DayOfYear,
  IF(
    EXTRACT(QUARTER FROM dt) IN (1, 2),
    EXTRACT(YEAR FROM dt) || 'S1',
    EXTRACT(YEAR FROM dt) || 'S2'
  ) AS YearSemester,
  EXTRACT(YEAR FROM dt) || 'Q' || EXTRACT(QUARTER FROM dt) AS YearQuarter,
  CAST(FORMAT_DATE('%Y%m', dt) AS STRING) AS YearMonth,
  EXTRACT(YEAR FROM dt) || ' ' || FORMAT_DATE('%b', dt) AS YearMonth2,
  FORMAT_DATE('%Y%U', dt) AS YearWeek,
  (DATE_TRUNC(dt, YEAR) = dt) AS IsFirstDayOfYear,
  (LAST_DAY(dt, YEAR) = dt) AS IsLastDayOfYear,
  (EXTRACT(MONTH FROM dt) IN (1, 7) AND EXTRACT(DAY FROM dt) = 1) AS IsFirstDayOfSemester,
  ((EXTRACT(MONTH FROM dt) IN (6) AND EXTRACT(DAY FROM dt) IN (30))
    OR (EXTRACT(MONTH FROM dt) IN (12) AND EXTRACT(DAY FROM dt) IN (31))) AS IsLastDayOfSemester,
  (DATE_TRUNC(dt, QUARTER) = dt) AS IsFirstDayOfQuarter,
  (LAST_DAY(dt, QUARTER) = dt) AS IsLastDayOfQuarter,
  (DATE_TRUNC(dt, MONTH) = dt) AS IsFirstDayOfMonth,
  (LAST_DAY(dt, MONTH) = dt) AS IsLastDayOfMonth,
  (DATE_TRUNC(dt, WEEK) = dt) AS IsFirstDayOfWeek,
  (LAST_DAY(dt, WEEK) = dt) AS IsLastDayOfWeek,
  ((MOD(EXTRACT(YEAR FROM dt), 4) = 0 AND MOD(EXTRACT(YEAR FROM dt), 100) != 0)
    OR MOD(EXTRACT(YEAR FROM dt), 400) = 0) AS IsLeapYear,
  (FORMAT_DATE('%A', dt) NOT IN ('Saturday', 'Sunday')) AS IsWeekDay,
  (FORMAT_DATE('%A', dt) IN ('Saturday', 'Sunday')) AS IsWeekEnd,
  (DATE_TRUNC(dt, WEEK)) AS WeekStartDate,
  (LAST_DAY(dt, WEEK)) AS WeekEndDate,
  (DATE_TRUNC(dt, MONTH)) AS MonthStartDate,
  (LAST_DAY(dt, MONTH)) AS MonthEndDate,
  (EXTRACT(WEEK FROM LAST_DAY(dt, ISOYEAR)) = 53) AS Has53Weeks
FROM UNNEST(GENERATE_DATE_ARRAY(
  DATE_SUB(
    DATE_TRUNC(CURRENT_DATE(), YEAR), INTERVAL 20 YEAR),
  LAST_DAY(DATE_ADD(CURRENT_DATE(), INTERVAL 20 YEAR)),
  INTERVAL 1 DAY)
) AS dt;
