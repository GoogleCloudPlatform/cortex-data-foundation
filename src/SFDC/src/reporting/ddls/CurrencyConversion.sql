#-- Copyright 2023 Google LLC
#--
#-- Licensed under the Apache License, Version 2.0 (the "License");
#-- you may not use this file except in compliance with the License.
#-- You may obtain a copy of the License at
#--
#--     https://www.apache.org/licenses/LICENSE-2.0
#--
#-- Unless required by applicable law or agreed to in writing, software
#-- distributed under the License is distributed on an "AS IS" BASIS,
#-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#-- See the License for the specific language governing permissions and
#-- limitations under the License.

--## CORTEX-CUSTOMER Currency Conversion feature requires Advanced Currency Management
--## to be enabled within your source Salesforce system to show correct data.
--## See https://help.salesforce.com/s/articleView?id=sf.administration_about_advanced_currency_management.htm

WITH
  SourceCurrency AS (
    SELECT
      IsoCode
    FROM
      `{{ project_id_tgt }}.{{ sfdc_datasets_reporting }}.CurrencyTypesMD` -- noqa: L057
    WHERE
      IsCorporate
  ),
  CurrencyTemp AS (
    SELECT
      IsoCode AS TargetCurrency,
      ConversionRate AS ConversionRate,
      IF(StartDate = DATE('0001-01-01'),
        DATE_SUB(CURRENT_DATE(), INTERVAL 20 YEAR),
        StartDate) AS StartDate,
      IF(NextStartDate = DATE('9999-12-31'),
        DATE_ADD(CURRENT_DATE(), INTERVAL 20 YEAR),
        DATE_SUB(NextStartDate, INTERVAL 1 DAY)) AS EndDate
    FROM
      `{{ project_id_tgt }}.{{ sfdc_datasets_reporting }}.DatedConversionRatesMD` -- noqa: L057
  ),
  CurrencyConversion AS (
    SELECT
      SourceCurrency.IsoCode AS SourceCurrency,
      CurrencyTemp.TargetCurrency,
      CurrencyTemp.ConversionRate,
      CurrencyTemp.StartDate,
      CurrencyTemp.EndDate
    FROM
      CurrencyTemp AS CurrencyTemp
    CROSS JOIN SourceCurrency AS SourceCurrency
  )
SELECT
  C.SourceCurrency,
  C.TargetCurrency,
  ConversionDate,
  C.ConversionRate,
  C.StartDate,
  C.EndDate
FROM
  CurrencyConversion AS C,
  UNNEST(GENERATE_DATE_ARRAY(DATE(StartDate), DATE(EndDate) - 1)) AS ConversionDate
