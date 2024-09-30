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

CREATE OR REPLACE TABLE
`{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.stock_weekly_snapshots`(
  mandt STRING,
  werks STRING,
  matnr STRING,
  charg STRING,
  lgort STRING,
  bukrs STRING,
  cal_year INTEGER,
  cal_week INTEGER,
  meins STRING,
  waers STRING,
  stock_characteristic STRING,
  week_end_date DATE,
  total_weekly_movement_quantity NUMERIC,
  total_weekly_movement_amount NUMERIC,
  amount_weekly_cumulative NUMERIC,
  quantity_weekly_cumulative NUMERIC);

CALL `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.AggregateWeeklyInventory`();

CALL `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.UpdateStockWeeklySnapshots`(
{% if sql_flavour == 'ecc' -%}
  (SELECT MIN(budat_mkpf) FROM `{{ project_id_src }}.{{ dataset_cdc_processed }}.mseg`),
{% else -%}
  (SELECT MIN(budat) FROM `{{ project_id_src }}.{{ dataset_cdc_processed }}.matdoc`),
{% endif -%}
  LAST_DAY(CURRENT_DATE, WEEK));
