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

CREATE TABLE IF NOT EXISTS `{{ project_id_src }}.{{ k9_datasets_processing }}.calendar_date_dim`
(
  Date DATE,
  DateInt INT64,
  DateStr STRING,
  DateStr2 STRING,
  CalYear INT64,
  CalSemester INT64,
  CalQuarter INT64,
  CalMonth INT64,
  CalWeek INT64,
  CalYearStr STRING,
  CalSemesterStr STRING,
  CalSemesterStr2 STRING,
  CalQuarterStr STRING,
  CalQuarterStr2 STRING,
  CalMonthLongStr STRING,
  CalMonthShortStr STRING,
  CalWeekStr STRING,
  DayNameLong STRING,
  DayNameShort STRING,
  DayOfWeek INT64,
  DayOfMonth INT64,
  DayOfQuarter INT64,
  DayOfSemester INT64,
  DayOfYear INT64,
  YearSemester STRING,
  YearQuarter STRING,
  YearMonth STRING,
  YearMonth2 STRING,
  YearWeek STRING,
  IsFirstDayOfYear BOOL,
  IsLastDayOfYear BOOL,
  IsFirstDayOfSemester BOOL,
  IsLastDayOfSemester BOOL,
  IsFirstDayOfQuarter BOOL,
  IsLastDayOfQuarter BOOL,
  IsFirstDayOfMonth BOOL,
  IsLastDayOfMonth BOOL,
  IsFirstDayOfWeek BOOL,
  IsLastDayOfWeek BOOL,
  IsLeapYear BOOL,
  IsWeekDay BOOL,
  IsWeekEnd BOOL,
  WeekStartDate DATE,
  WeekEndDate DATE,
  MonthStartDate DATE,
  MonthEndDate DATE,
  Has53Weeks BOOL
);
