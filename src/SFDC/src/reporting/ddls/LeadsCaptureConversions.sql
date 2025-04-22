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
  LeadsFirstResponseDates AS (
    SELECT LeadId, MIN(CreatedDatestamp) AS LeadFirstResponseDatestamp
    FROM (
      SELECT WhoId AS LeadId, MIN(CreatedDatestamp) AS CreatedDatestamp
      FROM `{{ project_id_tgt }}.{{ sfdc_datasets_reporting }}.Events`
      GROUP BY 1
      UNION ALL
      SELECT WhoId AS LeadId, MIN(CreatedDatestamp) AS CreatedDatestamp
      FROM `{{ project_id_tgt }}.{{ sfdc_datasets_reporting }}.Tasks`
      GROUP BY 1
    )
    GROUP BY 1
  ),
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
  Leads.LeadId AS LeadId,
  Leads.Name AS LeadName,
  Leads.FirstName AS LeadFirstName,
  Leads.LastName AS LeadLastName,
  Leads.Leadsource AS LeadSource,
  Leads.OwnerId AS LeadOwnerId,
  Leads.Industry AS LeadIndustry,
  Leads.Country AS LeadCountry,
  Leads.ConvertedDate AS LeadConvertedDate,
  Leads.CreatedDatestamp AS LeadCreatedDatestamp,
  Leads.IsConverted AS IsLeadConverted,
  UsersMD.Name AS LeadOwnerName,
  Opportunities.OpportunityId AS OpportunityId,
  Opportunities.Amount AS TotalSaleAmount,
  Opportunities.OwnerId AS OpportunityOwnerId,
  Opportunities.CloseDate AS OpportunityCloseDate,
  Opportunities.CreatedDatestamp AS OpportunityCreatedDatestamp,
  Opportunities.Name AS OpportunityName,
  Leads.Status AS LeadStatus,
  CurrencyConversion.TargetCurrency,
  CurrencyConversion.SourceCurrency,
  CurrencyConversion.CurrencyExchangeRate,
  CurrencyConversion.CurrencyConversionDate,
  LeadsFirstResponseDates.LeadFirstResponseDatestamp AS LeadFirstResponseDatestamp,
  (Opportunities.Amount * CurrencyConversion.CurrencyExchangeRate) AS TotalSaleAmountInTargetCurrency,
  --## CORTEX-CUSTOMER Consider adding other dimensions from the CalendarDateDimension table as per your requirement
  Leads.LeadCreatedDate AS LeadCreatedDate,
  Leads.LeadCreatedWeek AS LeadCreatedWeek,
  Leads.LeadCreatedMonth AS LeadCreatedMonth,
  Leads.LeadCreatedQuarter AS LeadCreatedQuarter,
  Leads.LeadCreatedYear AS LeadCreatedYear
FROM
  `{{ project_id_tgt }}.{{ sfdc_datasets_reporting }}.Leads` AS Leads
LEFT JOIN
  `{{ project_id_tgt }}.{{ sfdc_datasets_reporting }}.UsersMD` AS UsersMD
  ON Leads.OwnerId = UsersMD.UserId
LEFT JOIN
  `{{ project_id_tgt }}.{{ sfdc_datasets_reporting }}.Opportunities` AS Opportunities
  ON Leads.ConvertedOpportunityId = Opportunities.OpportunityId
LEFT JOIN
  LeadsFirstResponseDates
  ON Leads.LeadId = LeadsFirstResponseDates.LeadId
LEFT JOIN
  CurrencyConversion
  ON
    COALESCE(Opportunities.CloseDate, CURRENT_DATE()) = CurrencyConversion.CurrencyConversionDate
