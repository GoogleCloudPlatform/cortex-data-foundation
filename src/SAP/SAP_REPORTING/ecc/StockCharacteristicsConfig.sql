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

SELECT DISTINCT
  mandt AS Client_MANDT,
  insmk AS StockType_INSMK,
  shkzg AS Debit_CreditIndicator_SHKZG,
  sobkz AS SpecialStockIndicator_SOBKZ,
  bwart AS MovementType_BWART,
  -- ## CORTEX-CUSTOMER: Consider adding relevant movement types ('bwart')
  -- for different stock if applicable
  CASE
    WHEN (insmk = 'F' OR Insmk = '') AND bwart IN ('322', '323', '344', '341') AND shkzg = 'H' THEN 'Unrestricted'
    WHEN (insmk = 'F' OR Insmk = '') AND bwart IN ('321', '324', '343', '342') AND shkzg = 'S' THEN 'Unrestricted'
    WHEN (insmk = 'F' OR Insmk = '') AND bwart IN ('322', '323', '349') AND shkzg = 'S' THEN 'QualityInspection'
    WHEN (insmk = 'F' OR Insmk = '') AND bwart IN ('321', '324', '350') AND shkzg = 'H' THEN 'QualityInspection'
    WHEN (insmk = 'F' OR Insmk = '') AND bwart IN ('344', '350') AND shkzg = 'S' THEN 'Blocked'
    WHEN (insmk = 'F' OR Insmk = '') AND bwart IN ('343', '349') AND shkzg = 'H' THEN 'Blocked'
    WHEN (insmk = 'F' OR Insmk = '') AND bwart IN ('651') AND shkzg = 'S' THEN 'BlockedReturns'
    WHEN (insmk = 'F' OR Insmk = '') AND bwart IN ('351') AND shkzg = 'S' THEN 'InTransit'
    WHEN (insmk = 'F' OR Insmk = '') AND bwart IN ('352') AND shkzg = 'H' THEN 'InTransit'
    WHEN (insmk = 'F' OR Insmk = '') AND bwart IN ('341') AND shkzg = 'S' THEN 'Restricted'
    WHEN (insmk = 'F' OR Insmk = '') AND bwart IN ('342') AND shkzg = 'H' THEN 'Restricted'
    WHEN (insmk = 'F' OR Insmk = '') AND sobkz = 'K' THEN 'VendorManaged'
    WHEN (insmk = '3' OR Insmk = 'S') THEN 'Blocked'
    WHEN (insmk = 'X' OR Insmk = '2') THEN 'QualityInspection'
    ELSE 'Unrestricted'
  END AS StockCharacteristic
FROM
  `{{ project_id_src }}.{{ dataset_cdc_processed }}.mseg`
WHERE
  mandt = '{{ mandt }}'
