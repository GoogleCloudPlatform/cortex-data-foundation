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

CREATE OR REPLACE TABLE `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.StockCharacteristicsConfig` AS
SELECT DISTINCT
  mandt AS Client_MANDT,
  {% if sql_flavour == 'ecc' -%}
  insmk AS StockType_INSMK,
  shkzg AS Debit_CreditIndicator_SHKZG,
  sobkz AS SpecialStockIndicator_SOBKZ,
  bwart AS MovementType_BWART,
  -- ## CORTEX-CUSTOMER: Consider adding relevant movement types ('bwart')
  -- for different stock if applicable
  CASE
    WHEN (insmk = 'F' OR Insmk = '') AND bwart IN('322', '323', '344', '341') AND shkzg = 'H' THEN 'Unrestricted'
    WHEN (insmk = 'F' OR Insmk = '') AND bwart IN('321', '324', '343', '342') AND shkzg = 'S'THEN 'Unrestricted'
    WHEN (insmk = 'F' OR Insmk = '') AND bwart IN('322', '323', '349') AND shkzg = 'S' THEN 'QualityInspection'
    WHEN (insmk = 'F' OR Insmk = '') AND bwart IN('321', '324', '350') AND shkzg = 'H' THEN 'QualityInspection'
    WHEN (insmk = 'F' OR Insmk = '') AND bwart IN('344', '350') AND shkzg = 'S' THEN 'Blocked'
    WHEN (insmk = 'F' OR Insmk = '') AND bwart IN('343', '349') AND shkzg = 'H' THEN 'Blocked'
    WHEN (insmk = 'F' OR Insmk = '') AND bwart IN('651') AND shkzg = 'S'THEN 'BlockedReturns'
    WHEN (insmk = 'F' OR Insmk = '') AND bwart IN('351') AND shkzg = 'S' THEN 'InTransit'
    WHEN (insmk = 'F' OR Insmk = '') AND bwart IN('352') AND shkzg = 'H' THEN 'InTransit'
    WHEN (insmk = 'F' OR Insmk = '') AND bwart IN('341') AND shkzg = 'S' THEN 'Restricted'
    WHEN (insmk = 'F' OR Insmk = '') AND bwart IN('342') AND shkzg = 'H' THEN 'Restricted'
    WHEN (insmk = 'F' OR Insmk = '') AND sobkz = 'K' THEN 'VendorManaged'
    WHEN (insmk = '3' OR Insmk = 'S') THEN 'Blocked'
    WHEN (insmk = 'X' OR Insmk = '2') THEN 'QualityInspection'
    ELSE 'Unrestricted'
  END AS StockCharacteristic
FROM
  `{{ project_id_src }}.{{ dataset_cdc_processed }}.mseg`
  {% else -%}
  bstaus_sg AS StockCharacteristic_BSTAUS_SG,
  sobkz AS SpecialStockIndicator_SOBKZ,
  CASE
    WHEN sobkz = 'K' AND bstaus_sg = 'A' THEN 'VendorManaged'
    WHEN sobkz != 'K' AND bstaus_sg = 'A' THEN 'Unrestricted'
    WHEN bstaus_sg = 'B' THEN 'QualityInspection'
    WHEN bstaus_sg = 'D' THEN 'Blocked'
    WHEN bstaus_sg = 'H' THEN 'InTransit'
    WHEN bstaus_sg = 'E' THEN 'Restricted'
    WHEN bstaus_sg = 'C' THEN 'BlockedReturns'
    WHEN bstaus_sg = 'F' THEN 'StockTransfer'
    WHEN bstaus_sg = 'Q' THEN 'Unrestricted Use Material Provided To Vendor'
    WHEN bstaus_sg = 'K' THEN 'Customer Consigment, Unrestricted Use'
  END AS StockCharacteristic
FROM
  `{{ project_id_src }}.{{ dataset_cdc_processed }}.matdoc`
  {% endif -%}
WHERE
    mandt = '{{ mandt }}';
