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

/*
* This view is intended to showcase how KPIs from SalesActivities view are meant to be calculated
* without the accompanying Looker visualizations.
*
* Please note that this is view is INFORMATIONAL ONLY and may be subject to change without
* notice in upcoming Cortex Data Foundation releases.
*/


WITH
  SalesActivities AS (
    SELECT
      LeadOwnerId,
      OpportunityOwnerId,
      OpportunityId,
      ActivityId,
      IsOpportunityClosed,
      IsOpportunityWon
    FROM
      `{{ project_id_tgt }}.{{ sfdc_datasets_reporting }}.SalesActivities`
    WHERE
      OpportunityId IS NOT NULL
      AND ActivityStatus = 'Completed'
  )
SELECT
  'Opportunity' AS SourceList,
  OpportunityPipeline.OpportunityCreatedDatestamp AS CreatedDatestamp,
  OpportunityPipeline.AccountBillingCountry AS Country,
  OpportunityPipeline.AccountIndustry AS Industry,
  OpportunityPipeline.OpportunityOwnerName AS OwnerName,
  MAX(SalesActivities.OpportunityOwnerId) AS OpportunityOwnerId,
  MAX(SalesActivities.LeadOwnerId) AS LeadOwnerId,
  --## CORTEX-CUSTOMER Consider adding other dimensions from the CalendarDateDimension table as per your requirement
  OpportunityPipeline.OpportunityCreatedDate AS CreatedDate,
  OpportunityPipeline.OpportunityCreatedWeek AS CreatedWeek,
  OpportunityPipeline.OpportunityCreatedMonth AS CreatedMonth,
  OpportunityPipeline.OpportunityCreatedQuarter AS CreatedQuarter,
  OpportunityPipeline.OpportunityCreatedYear AS CreatedYear,
  COUNT(DISTINCT OpportunityPipeline.OpportunityId) AS NumOfOpportunities,
  COUNT(DISTINCT SalesActivities.OpportunityId) AS NumOfOpportunitiesWithActivities,
  COUNT(DISTINCT SalesActivities.ActivityId) AS NumofOpportunityActivities,
  COUNT(DISTINCT
    IF(NOT SalesActivities.IsOpportunityClosed, SalesActivities.ActivityId, NULL)) AS NumOfActivitiesOnOpenOpportunities,
  COUNT(DISTINCT
    IF(NOT SalesActivities.IsOpportunityClosed, SalesActivities.OpportunityId, NULL)) AS NumOfOpenOpportunities,
  COUNT(DISTINCT IF(
      SalesActivities.IsOpportunityClosed AND SalesActivities.IsOpportunityWon,
      SalesActivities.ActivityId,
      NULL)) AS NumOfActivitiesOnWonOpportunities,
  COUNT(DISTINCT IF(
      SalesActivities.IsOpportunityClosed AND SalesActivities.IsOpportunityWon,
      SalesActivities.OpportunityId,
      NULL)) AS NumOfWonOpportunities,
  COUNT(DISTINCT IF(
      SalesActivities.IsOpportunityClosed AND NOT SalesActivities.IsOpportunityWon,
      SalesActivities.ActivityId,
      NULL)) AS NumOfActivitiesOnLostOpportunities,
  COUNT(DISTINCT IF(
      SalesActivities.IsOpportunityClosed AND NOT SalesActivities.IsOpportunityWon,
      SalesActivities.OpportunityId,
      NULL)) AS NumOfLostOpportunities,
  COUNT(DISTINCT IF(
      NOT OpportunityPipeline.IsOpportunityClosed
      AND (
        (DATE_DIFF(CURRENT_DATE(), OpportunityPipeline.OpportunityLastActivityDate, DAY) > 7)
        OR (DATE_DIFF(CURRENT_DATE(), DATE(OpportunityPipeline.OpportunityCreatedDatestamp), DAY) > 7)),
      OpportunityPipeline.OpportunityId,
      NULL)) AS NumOfNeglectedOpportunities,
  COUNT(DISTINCT IF(
      NOT OpportunityPipeline.IsOpportunityClosed
      AND DATE_DIFF(CURRENT_DATE(), OpportunityPipeline.OpportunityCloseDate, DAY) > 0,
      OpportunityPipeline.OpportunityId,
      NULL)) AS NumOfOverdueOpportunities,
  NULL AS NumOfLeadActivities,
  NULL AS NumOfLeads,
  NULL AS NumOfConvertedLeads,
  NULL AS NumOfActivitiesForConvertedLeads,
  NULL AS NumOfUnqualifiedLeads,
  NULL AS NumOfActivitiesForUnqualifiedLeads
FROM
  `{{ project_id_tgt }}.{{ sfdc_datasets_reporting }}.OpportunityPipeline` AS OpportunityPipeline
LEFT JOIN
  SalesActivities
  ON
    OpportunityPipeline.OpportunityId = SalesActivities.OpportunityId

GROUP BY
  OpportunityPipeline.OpportunityCreatedDatestamp,
  OpportunityPipeline.AccountBillingCountry,
  OpportunityPipeline.AccountIndustry,
  OpportunityPipeline.OpportunityOwnerName,
  --## CORTEX-CUSTOMER Consider adding other dimensions from the CalendarDateDimension table as per your requirement
  OpportunityPipeline.OpportunityCreatedDate,
  OpportunityPipeline.OpportunityCreatedWeek,
  OpportunityPipeline.OpportunityCreatedMonth,
  OpportunityPipeline.OpportunityCreatedQuarter,
  OpportunityPipeline.OpportunityCreatedYear
UNION ALL
SELECT
  'Lead' AS SourceList,
  LeadCreatedDatestamp AS CreatedDatestamp,
  LeadCountry AS Country,
  LeadIndustry AS Industry,
  LeadOwnerName AS OpportunityOwnerName,
  NULL AS OpportunityOwnerId,
  MAX(LeadOwnerId) AS LeadOwnerId,
  --## CORTEX-CUSTOMER Consider adding other dimensions from the CalendarDateDimension table as per your requirement
  LeadCreatedDate AS CreatedDate,
  LeadCreatedWeek AS CreatedWeek,
  LeadCreatedMonth AS CreatedMonth,
  LeadCreatedQuarter AS CreatedQuarter,
  LeadCreatedYear AS CreatedYear,
  NULL AS NumOfOpportunities,
  NULL AS NumOfOpportunitiesWithActivities,
  NULL AS NumofOpportunityActivities,
  NULL AS NumOfActivitiesOnOpenOpportunities,
  NULL AS NumOfOpenOpportunities,
  NULL AS NumOfActivitiesOnWonOpportunities,
  NULL AS NumOfWonOpportunities,
  NULL AS NumOfActivitiesOnLostOpportunities,
  NULL AS NumOfLostOpportunities,
  NULL AS NumOfNeglectedOpportunities,
  NULL AS NumOfOverdueOpportunities,
  COUNT(DISTINCT ActivityId) AS NumOfLeadActivities,
  COUNT(DISTINCT LeadId) AS NumOfLeads,
  COUNT(DISTINCT IF(IsLeadConverted, LeadId, NULL)) AS NumOfConvertedLeads,
  COUNT(DISTINCT IF(IsLeadConverted, ActivityId, NULL)) AS NumOfActivitiesForConvertedLeads,
  COUNT(DISTINCT IF(LeadStatus = 'Unqualified', LeadId, NULL)) AS NumOfUnqualifiedLeads,
  COUNT(DISTINCT IF(LeadStatus = 'Unqualified', ActivityId, NULL)) AS NumOfActivitiesForUnqualifiedLeads
FROM
  `{{ project_id_tgt }}.{{ sfdc_datasets_reporting }}.SalesActivities`
WHERE
  LeadId IS NOT NULL
  AND OpportunityId IS NULL
  AND ActivityStatus = 'Completed'
GROUP BY
  LeadCreatedDatestamp,
  LeadCountry,
  LeadIndustry,
  LeadOwnerName,
  --## CORTEX-CUSTOMER Consider adding other dimensions from the CalendarDateDimension table as per your requirement
  LeadCreatedDate,
  LeadCreatedWeek,
  LeadCreatedMonth,
  LeadCreatedQuarter,
  LeadCreatedYear

