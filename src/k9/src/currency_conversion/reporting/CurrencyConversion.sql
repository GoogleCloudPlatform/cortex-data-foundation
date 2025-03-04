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

SELECT
  conversion_date AS ConversionDate,
  from_currency AS FromCurrency,
  to_currency AS ToCurrency,
  exchange_rate AS ExchangeRate
FROM
  `{{ project_id_tgt }}.{{ k9_datasets_reporting }}.currency_conversion`
WHERE
  ## CORTEX-CUSTOMER: Update if you have customized source system name or rate type beyond what is
  ## supported through updating the config.
  source_system = '{{ k9_currency_conversion_data_source_type }}'
  AND rate_type = '{{ k9_currency_conversion_rate_type }}'
