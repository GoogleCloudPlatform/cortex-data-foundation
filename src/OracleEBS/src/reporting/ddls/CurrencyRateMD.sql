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

-- CurrencyRateMD dimension.

SELECT
  CONVERSION_DATE,
  FROM_CURRENCY,
  TO_CURRENCY,
  CONVERSION_TYPE,
  CONVERSION_RATE
FROM
  `{{ project_id_src }}.{{ oracle_ebs_datasets_cdc }}.GL_DAILY_RATES`
WHERE
  TO_CURRENCY IN UNNEST({{ oracle_ebs_currency_conversion_targets }})
  AND CONVERSION_TYPE = '{{ oracle_ebs_currency_conversion_type }}'