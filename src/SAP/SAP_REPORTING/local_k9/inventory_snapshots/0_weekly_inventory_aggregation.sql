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

## CORTEX-CUSTOMER: These procedures need to execute for inventory views to work.
## please check the ERD linked in the README for dependencies. The procedures
## can be scheduled with Cloud Composer with the provided templates or ported
## into the scheduling tool of choice. These DAGs will be executed from a different
## directory structure in future releases.
## PREVIEW

CREATE OR REPLACE PROCEDURE
`{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.AggregateWeeklyInventory`()
BEGIN
  CREATE OR REPLACE TABLE
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.weekly_inventory_aggregation` AS
  SELECT
    src.mandt,
    src.werks,
    src.matnr,
    src.charg,
    src.lgort,
    src.bukrs,
    src.meins,
    src.waers,
    StockCharacteristicsConfig.StockCharacteristic as stock_characteristic,
    EXTRACT(YEAR FROM datedim.weekEndDate) AS cal_year,
    EXTRACT(WEEK FROM datedim.weekEndDate) AS cal_week,
    datedim.WeekEndDate AS week_end_date,
    {% if sql_flavour == 'ecc' -%}
    SUM(IF(src.shkzg = 'H', (src.dmbtr * -1), src.dmbtr)) AS total_weekly_movement_amount,
    SUM(IF(src.shkzg = 'H', (src.menge * -1), src.menge)) AS total_weekly_movement_quantity
    {% else -%}
    SUM(src.dmbtr_stock) AS total_weekly_movement_amount,
    SUM(src.stock_qty) AS total_weekly_movement_quantity
    {% endif -%}
  FROM
    {% if sql_flavour == 'ecc' -%}
    `{{ project_id_src }}.{{ dataset_cdc_processed }}.mseg` AS src
    {% else -%}
    `{{ project_id_src }}.{{ dataset_cdc_processed }}.matdoc` AS src
    {% endif -%}
  LEFT JOIN
    `{{ project_id_src }}.{{ k9_datasets_processing }}.calendar_date_dim` AS datedim
    ON
      {% if sql_flavour == 'ecc' -%}
      src.budat_mkpf = datedim.date
      {% else -%}
      src.budat = datedim.Date
      {% endif -%}
  LEFT JOIN
    `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.StockCharacteristicsConfig` AS StockCharacteristicsConfig
    ON
      src.mandt = StockCharacteristicsConfig.Client_MANDT
      AND src.sobkz = StockCharacteristicsConfig.SpecialStockIndicator_SOBKZ
      {% if sql_flavour == 'ecc' -%}
      AND src.bwart = StockCharacteristicsConfig.MovementType_BWART
      AND src.shkzg = StockCharacteristicsConfig.Debit_CreditIndicator_SHKZG
      AND src.insmk = StockCharacteristicsConfig.StockType_INSMK
      {% else -%}
      AND src.bstaus_sg = StockCharacteristicsConfig.StockCharacteristic_BSTAUS_SG
      {% endif -%}
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
    StockCharacteristicsConfig.StockCharacteristic,
    datedim.WeekEndDate;

END;
