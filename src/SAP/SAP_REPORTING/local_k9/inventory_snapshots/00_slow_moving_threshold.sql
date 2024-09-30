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
`{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.SlowMovingThreshold` (
  Client_MANDT STRING,
  MaterialType_MTART STRING,
  ThresholdValue NUMERIC);

INSERT INTO
`{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.SlowMovingThreshold` (
  Client_MANDT,
  MaterialType_MTART,
  ThresholdValue) (
  SELECT DISTINCT
    mandt as Client_MANDT,
    mtart as MaterialType_MTART,
    -- ## CORTEX-CUSTOMER: Consider customizing these thresholds for different
    -- ## material types
    CASE
      WHEN mara.mandt = '{{ mandt }}' AND mara.mtart = 'FERT' THEN 50
      WHEN mara.mandt = '{{ mandt }}' AND mara.mtart = 'ROH' THEN 60
      WHEN mara.mandt = '{{ mandt }}' AND mara.mtart = 'HIBE' THEN 60
      WHEN mara.mandt = '{{ mandt }}' AND mara.mtart = 'HALB' THEN 60
      ELSE 0
    END AS ThresholdValue
  FROM `{{ project_id_src }}.{{ dataset_cdc_processed }}.mara` AS mara
  WHERE
    mara.mandt = '{{ mandt }}'
);
