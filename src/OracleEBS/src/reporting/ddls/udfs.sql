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

-- UDFs for Oracle EBS

-- If source and target currency is same, returns source amount, else converts the source amount
-- to the target currency.
--
-- @param source_curr Source currency type that the event used
-- @param target_curr Target currency conversion used for reporting
-- @param conversion_rate Conversion rate if the currencies are different
-- @param source_amount Transaction amount in base currency
-- @return transaction amount converted to target currency or same amount if src and tgt currency
--   is same
CREATE OR REPLACE FUNCTION `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.GetConvertedAmount`(
  source_curr STRING, target_curr STRING,
  conversion_rate FLOAT64, source_amount FLOAT64) -- noqa: LT02
RETURNS FLOAT64
AS (
  IF(source_curr = target_curr, source_amount, conversion_rate * source_amount)
);

-- Checks if the action was performed within the agreed time
--
-- @param target_date Date an event should have occurred
-- @param target_date Date an event actually occurred
-- @return `true` if the action was performed within agreed time, `null` when the target date is
--   still in the future and `false` if the target date has passed and event either didn't
--   happen or happened after the target date
CREATE OR REPLACE FUNCTION `{{ project_id_tgt }}.{{ oracle_ebs_datasets_reporting }}.IsOnTime`(
  target_date DATE, actual_date DATE) -- noqa: LT02
RETURNS BOOL
AS (
  CASE
    WHEN actual_date <= target_date THEN TRUE
    WHEN CURRENT_DATE < target_date THEN NULL
    ELSE FALSE
  END
);
