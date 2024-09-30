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

# Fully update (refresh) the monthly inventory aggregation intermediate table.
CREATE OR REPLACE PROCEDURE
`{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.AggregateMonthlyInventory` ()
BEGIN
  CREATE OR REPLACE TABLE
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.monthly_inventory_aggregation`
  PARTITION BY
  DATE_TRUNC(month_end_date, MONTH) AS
  SELECT
    src.mandt,
    src.werks,
    src.matnr,
    src.charg,
    src.lgort,
    src.bukrs,
    src.bwart,
    src.insmk,
    src.sobkz,
    src.shkzg,
    datedim.calyear AS cal_year,
    datedim.calmonth AS cal_month,
    src.meins,
    src.waers,
    datedim.monthenddate AS month_end_date,
    SUM(IF(src.shkzg = 'H', (src.dmbtr * -1), src.dmbtr)) AS total_monthly_movement_amount,
    SUM(IF(src.shkzg = 'H', (src.menge * -1), src.menge)) AS total_monthly_movement_quantity
  FROM
    `{{ project_id_src }}.{{ dataset_cdc_processed }}.mseg` AS src
  LEFT JOIN
    `{{ project_id_src }}.{{ k9_datasets_processing }}.calendar_date_dim` AS datedim
    ON
      src.budat_mkpf = datedim.date
  WHERE
    src.mandt = '{{ mandt }}'
  GROUP BY
    src.mandt,
    src.werks,
    src.matnr,
    src.charg,
    src.meins,
    src.waers,
    src.lgort,
    src.bukrs,
    src.bwart,
    src.insmk,
    src.sobkz,
    src.shkzg,
    datedim.calyear,
    datedim.monthenddate,
    datedim.calmonth;
END;
