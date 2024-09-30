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
* This view is intended to showcase how KPIs from CaseManagement view are meant to be calculated
* without the accompanying Looker visualizations.
*
* Please note that this is view is INFORMATIONAL ONLY and may be subject to change without
* notice in upcoming Cortex Data Foundation releases.
*/

SELECT
  CaseOrigin,
  CasePriority,
  CaseStatus,
  CaseType,
  CaseOwnerId,
  CaseOwnerName,
  AccountId,
  AccountName,
  AccountIndustry,
  AccountBillingCountry,
  CaseCreatedDatestamp,
  CaseClosedDatestamp,
  --## CORTEX-CUSTOMER Consider adding other dimensions from the CalendarDateDimension table as per your requirement
  CaseCreatedDate,
  CaseCreatedMonth,
  CaseCreatedQuarter,
  CaseCreatedYear,
  CaseClosedDate,
  CaseClosedMonth,
  CaseClosedQuarter,
  CaseClosedYear,
  IF(IsAgentAssigned, CaseOwnerId, NULL) AS CaseOwnerAgentId,
  COUNT(CaseId) AS NumOfCreatedCases,
  COUNTIF(IsAgentAssigned) AS NumOfAssignedCases,
  COUNTIF(NOT IsCaseClosed) AS NumOfOpenCases,
  COUNTIF(IsCaseClosed) AS NumOfClosedCases,
  COUNTIF(NOT IsCaseClosed AND CaseStatus = 'Escalated') AS NumOfOpenEscalatedCases,
  COUNTIF(NOT IsCaseClosed AND CasePriority = 'High') AS NumOfOpenHighPriorityCases,
  COUNTIF(NOT IsCaseClosed AND CasePriority = 'Medium') AS NumOfOpenMediumPriorityCases,
  COUNTIF(NOT IsCaseClosed AND CasePriority = 'Low') AS NumOfOpenLowPriorityCases,
  COUNTIF(NOT IsCaseClosed AND NOT IsAgentAssigned) AS NumOfOpenUnassignedCases,
  COUNTIF(IsCaseClosed AND CasePriority = 'High') AS NumOfClosedHighPriorityCases,
  COUNTIF(IsCaseClosed AND CasePriority = 'Medium') AS NumOfClosedMediumPriorityCases,
  COUNTIF(IsCaseClosed AND CasePriority = 'Low') AS NumOfClosedLowPriorityCases,
  SUM(
    IF(
      NOT IsCaseClosed, DATE_DIFF(DATE(CURRENT_TIMESTAMP()), DATE(CaseCreatedDatestamp), DAY), 0
    )) AS TotalCaseAge,
  SUM(DATE_DIFF(DATE(CaseClosedDatestamp), DATE(CaseCreatedDatestamp), DAY)) AS TotalCaseResolutionTime,
  SUM(
    IF(
      IsCaseClosed AND CasePriority = 'High',
      DATE_DIFF(DATE(CaseClosedDatestamp), DATE(CaseCreatedDatestamp), DAY),
      0)) AS TotalHighPriorityCaseResolutionTime
FROM `{{ project_id_tgt }}.{{ sfdc_datasets_reporting }}.CaseManagement`
GROUP BY
  CaseOrigin,
  CasePriority,
  CaseStatus,
  CaseType,
  CaseOwnerId,
  CaseOwnerName,
  CaseOwnerAgentId,
  AccountId,
  AccountName,
  AccountIndustry,
  --## CORTEX-CUSTOMER Consider adding other dimensions from the CalendarDateDimension table as per your requirement
  CaseCreatedDate,
  CaseCreatedMonth,
  CaseCreatedQuarter,
  CaseCreatedYear,
  CaseClosedDate,
  CaseClosedMonth,
  CaseClosedQuarter,
  CaseClosedYear,
  AccountBillingCountry,
  CaseCreatedDatestamp,
  CaseClosedDatestamp
