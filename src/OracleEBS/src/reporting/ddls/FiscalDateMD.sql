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

-- FiscalDateMD dimension.

SELECT
  PeriodMap.ACCOUNTING_DATE AS FISCAL_DATE,
  Periods.PERIOD_TYPE,
  Periods.PERIOD_SET_NAME,
  Periods.PERIOD_NAME,
  Periods.PERIOD_NUM,
  Periods.QUARTER_NUM,
  Periods.PERIOD_YEAR AS YEAR_NUM
FROM
  `{{ project_id_src }}.{{ oracle_ebs_datasets_cdc }}.GL_PERIODS` AS Periods
INNER JOIN
  `{{ project_id_src }}.{{ oracle_ebs_datasets_cdc }}.GL_DATE_PERIOD_MAP` AS PeriodMap
  ON
    PeriodMap.ACCOUNTING_DATE BETWEEN Periods.START_DATE AND Periods.END_DATE
    AND Periods.PERIOD_SET_NAME = PeriodMap.PERIOD_SET_NAME
    AND Periods.PERIOD_NAME = PeriodMap.PERIOD_NAME
