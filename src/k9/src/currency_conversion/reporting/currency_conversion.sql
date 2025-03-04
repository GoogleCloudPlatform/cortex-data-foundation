#-- Copyright 2025 Google LLC
#--
#-- Licensed under the Apache License, Version 2.0 (the "License");
#-- you may not use this file except in compliance with the License.
#-- You may obtain a copy of the License at
#--
#--   https://www.apache.org/licenses/LICENSE-2.0
#--
#-- Unless required by applicable law or agreed to in writing, software
#-- distributed under the License is distributed on an "AS IS" BASIS,
#-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#-- See the License for the specific language governing permissions and
#-- limitations under the License.

CREATE TABLE IF NOT EXISTS `{{ project_id_tgt }}.{{ k9_datasets_reporting }}.currency_conversion`
(
  conversion_date DATE,
  source_system STRING,
  rate_type STRING,
  from_currency STRING,
  to_currency STRING,
  exchange_rate NUMERIC
)
PARTITION BY conversion_date;

-- If source and target currency is same, returns source amount, else converts the source amount
-- to the target currency.
--
-- @param source_curr Source currency type that the event used
-- @param target_curr Target currency conversion used for reporting
-- @param conversion_rate Conversion rate if the currencies are different
-- @param source_amount Transaction amount in base currency
-- @return transaction amount converted to target currency or same amount if src and tgt currency
--   is same
CREATE OR REPLACE FUNCTION `{{ project_id_tgt }}.{{ k9_datasets_reporting }}.GetConvertedAmount`(
  source_curr STRING, target_curr STRING,
  conversion_rate NUMERIC, source_amount FLOAT64) -- noqa: LT02
RETURNS FLOAT64
AS (
  IF(source_curr = target_curr, source_amount, conversion_rate * source_amount)
);