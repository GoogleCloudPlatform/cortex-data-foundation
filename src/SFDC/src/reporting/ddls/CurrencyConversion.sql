#-- Copyright 2025 Google LLC
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

/*
* This view provides source -> target currency conversion rates for all currencies supported
* in the system.
*
* All currencies are available as both source and target currency fields.
*
* Currency conversion results may be of limited accuracy due to limited decimal precision of
* SFDC conversion rates.
*/

--## CORTEX-CUSTOMER Currency Conversion feature requires Advanced Currency Management
--## to be enabled within your source Salesforce system to show correct data.
--## See https://help.salesforce.com/s/articleView?id=sf.administration_about_advanced_currency_management.htm

WITH
  AllCurrencies AS (
    SELECT DISTINCT
      IsoCode,
      IsCorporate
    FROM
      `{{ project_id_tgt }}.{{ sfdc_datasets_reporting }}.CurrencyTypesMD` -- noqa: L057
  ),
  CorrectedRates AS (
    SELECT
      IsoCode,
      ConversionRate,
      IF(
        StartDate = DATE('0001-01-01'),
        DATE_SUB(CURRENT_DATE(), INTERVAL 20 YEAR),
        StartDate
      ) AS StartDate,
      IF(
        NextStartDate = DATE('9999-12-31'),
        DATE_ADD(CURRENT_DATE(), INTERVAL 20 YEAR),
        DATE_SUB(NextStartDate, INTERVAL 1 DAY)
      ) AS EndDate
    FROM
      `{{ project_id_tgt }}.{{ sfdc_datasets_reporting }}.DatedConversionRatesMD` -- noqa: L057
  ),
  RateForAllDates AS (
    SELECT *
    FROM CorrectedRates, UNNEST(GENERATE_DATE_ARRAY(StartDate, EndDate - 1)) AS ConversionDate
  )
SELECT
  SourceCurrencies.IsoCode AS SourceCurrency,
  TargetCurrencies.IsoCode AS TargetCurrency,
  SourceCurrencies.IsCorporate AS SourceCurrencyIsCorporate,
  SourceCurrencyRates.ConversionDate,
  -- We first calculate Source->Corp Rate = 1 / Corp->Source Rate.
  -- Then we retrieve Target->Corp Rate.
  -- Lastly, Source->Target Rate = Source->Corp Rate * Corp->Target Rate.
  SAFE_DIVIDE(1, SourceCurrencyRates.ConversionRate) * TargetCurrencyRates.ConversionRate
    AS ConversionRate,
  SourceCurrencyRates.StartDate,
  SourceCurrencyRates.EndDate
FROM
  AllCurrencies AS SourceCurrencies
CROSS JOIN AllCurrencies AS TargetCurrencies
LEFT JOIN RateForAllDates AS SourceCurrencyRates
  ON SourceCurrencies.IsoCode = SourceCurrencyRates.IsoCode
LEFT JOIN RateForAllDates AS TargetCurrencyRates
  ON TargetCurrencies.IsoCode = TargetCurrencyRates.IsoCode
    AND SourceCurrencyRates.ConversionDate = TargetCurrencyRates.ConversionDate
