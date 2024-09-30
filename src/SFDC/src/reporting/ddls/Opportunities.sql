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


SELECT
  Opportunities.OpportunityId,
  Opportunities.AccountId,
  Opportunities.RecordTypeId,
  Opportunities.Name,
  Opportunities.Amount,
  Opportunities.CampaignId,
  Opportunities.CloseDate,
  Opportunities.ContactId,
  Opportunities.CreatedById,
  Opportunities.CreatedDatestamp,
  Opportunities.Description,
  Opportunities.Fiscal,
  Opportunities.FiscalQuarter,
  Opportunities.FiscalYear,
  Opportunities.ForecastCategory,
  Opportunities.ForecastCategoryName,
  Opportunities.HasOpenActivity,
  Opportunities.HasOpportunityLineItem,
  Opportunities.HasOverDueTask,
  Opportunities.IsClosed,
  Opportunities.LastReferencedDatestamp,
  Opportunities.IsWon,
  Opportunities.LastActivityDate,
  Opportunities.LastAmountChangedHistoryId,
  Opportunities.LastCloseDateChangedHistoryId,
  Opportunities.LeadSource,
  Opportunities.LastViewedDatestamp,
  Opportunities.NextStep,
  Opportunities.OwnerId,
  Opportunities.Pricebook2Id,
  Opportunities.Probability,
  Opportunities.StageName,
  Opportunities.Type,
  Opportunities.LastModifiedById,
  --## CORTEX-CUSTOMER Consider adding other dimensions from the CalendarDateDimension table as per your requirement
  DateDimensionCreatedDate.Date AS OpportunityCreatedDate,
  DateDimensionCreatedDate.CalWeek AS OpportunityCreatedWeek,
  DateDimensionCreatedDate.CalMonth AS OpportunityCreatedMonth,
  DateDimensionCreatedDate.CalQuarterStr2 AS OpportunityCreatedQuarter,
  DateDimensionCreatedDate.CalYear AS OpportunityCreatedYear,
  DateDimensionClosedDate.Date AS OpportunityClosedDate,
  DateDimensionClosedDate.CalWeek AS OpportunityClosedWeek,
  DateDimensionClosedDate.CalMonth AS OpportunityClosedMonth,
  DateDimensionClosedDate.CalQuarterStr2 AS OpportunityClosedQuarter,
  DateDimensionClosedDate.CalYear AS OpportunityClosedYear,
  Opportunities.LastModifiedDatestamp
FROM
  `{{ project_id_src }}.{{ sfdc_datasets_cdc }}.opportunities` AS Opportunities
LEFT JOIN
  `{{ project_id_src }}.{{ k9_datasets_processing }}.calendar_date_dim` AS DateDimensionCreatedDate
  ON
    DATE(Opportunities.CreatedDatestamp) = DateDimensionCreatedDate.Date
LEFT JOIN
  `{{ project_id_src }}.{{ k9_datasets_processing }}.calendar_date_dim` AS DateDimensionClosedDate
  ON
    Opportunities.CloseDate = DateDimensionClosedDate.Date
