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

WITH
  CurrencyConversion AS (
    SELECT
      TargetCurrency,
      SourceCurrency,
      ConversionRate AS CurrencyExchangeRate,
      ConversionDate AS CurrencyConversionDate
    FROM `{{ project_id_tgt }}.{{ sfdc_datasets_reporting }}.CurrencyConversion`
    WHERE
      TargetCurrency IN UNNEST({{ sfdc_currencies }})
  )
SELECT
  Opportunities.OpportunityId,
  -- Total sale amount that a sales representative has achieved
  Opportunities.Amount AS TotalSaleAmount,
  Opportunities.CloseDate AS OpportunityCloseDate,
  Opportunities.Name AS OpportunityName,
  Opportunities.Probability AS OpportunityProbability,
  Opportunities.StageName AS OpportunityStageName,
  Opportunities.OwnerId AS OpportunityOwnerId,
  Opportunities.IsClosed AS IsOpportunityClosed,
  Opportunities.IsWon AS IsOpportunityWon,
  Opportunities.LastActivityDate AS OpportunityLastActivityDate,
  Opportunities.CreatedDatestamp AS OpportunityCreatedDatestamp,
  Opportunities.RecordTypeId AS OpportunityRecordTypeId,
  AccountsMD.AccountId,
  AccountsMD.Name AS AccountName,
  AccountsMD.OwnerId AS AccountOwnerId,
  AccountsMD.Industry AS AccountIndustry,
  AccountsMD.BillingCountry AS AccountBillingCountry,
  AccountsMD.ShippingCountry AS AccountShippingCountry,
  AccountsMD.CreatedDatestamp AS AccountCreatedDatestamp,
  UsersMD.Name AS OpportunityOwnerName,
  RecordTypesMD.RecordTypeName AS OpportunityRecordTypeName,
  CurrencyConversion.TargetCurrency,
  --## CORTEX-CUSTOMER Consider adding other dimensions from the CalendarDateDimension table as per your requirement
  Opportunities.OpportunityCreatedDate AS OpportunityCreatedDate,
  Opportunities.OpportunityCreatedWeek AS OpportunityCreatedWeek,
  Opportunities.OpportunityCreatedMonth AS OpportunityCreatedMonth,
  Opportunities.OpportunityCreatedQuarter AS OpportunityCreatedQuarter,
  Opportunities.OpportunityCreatedYear AS OpportunityCreatedYear,
  Opportunities.OpportunityClosedDate AS OpportunityClosedDate,
  Opportunities.OpportunityClosedWeek AS OpportunityClosedWeek,
  Opportunities.OpportunityClosedMonth AS OpportunityClosedMonth,
  Opportunities.OpportunityClosedQuarter AS OpportunityClosedQuarter,
  Opportunities.OpportunityClosedYear AS OpportunityClosedYear,
  CurrencyConversion.SourceCurrency,
  CurrencyConversion.CurrencyExchangeRate,
  CurrencyConversion.CurrencyConversionDate,
  Opportunities.Amount * (Opportunities.Probability / 100) AS OpportunityExpectedValue,
  (Opportunities.Amount * (Opportunities.Probability / 100)) * CurrencyConversion.CurrencyExchangeRate AS OpportunityExpectedValueInTargetCurrency,
  (Opportunities.Amount * CurrencyConversion.CurrencyExchangeRate) AS TotalSaleAmountInTargetCurrency
FROM `{{ project_id_tgt }}.{{ sfdc_datasets_reporting }}.Opportunities` AS Opportunities
LEFT JOIN
  `{{ project_id_tgt }}.{{ sfdc_datasets_reporting }}.AccountsMD` AS AccountsMD
  ON AccountsMD.AccountId = Opportunities.AccountId
LEFT JOIN
  `{{ project_id_tgt }}.{{ sfdc_datasets_reporting }}.UsersMD` AS UsersMD
  ON Opportunities.OwnerId = UsersMD.UserId
LEFT JOIN
  `{{ project_id_tgt }}.{{ sfdc_datasets_reporting }}.RecordTypesMD` AS RecordTypesMD
  ON Opportunities.RecordTypeId = RecordTypesMD.RecordTypeId
LEFT JOIN
  CurrencyConversion
  ON
    Opportunities.CloseDate = CurrencyConversion.CurrencyConversionDate
