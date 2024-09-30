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
WHERE
  mandt = '{{ mandt }}'
