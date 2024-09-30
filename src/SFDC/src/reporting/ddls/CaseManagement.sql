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
    Cases.CaseId AS CaseId,
    Cases.Origin AS CaseOrigin,
    Cases.Priority AS CasePriority,
    Cases.Status AS CaseStatus,
    Cases.OwnerId AS CaseOwnerId,
    Cases.ClosedDatestamp AS CaseClosedDatestamp,
    Cases.CaseNumber AS CaseNumber,
    Cases.CreatedDatestamp AS CaseCreatedDatestamp,
    Cases.IsClosed AS IsCaseClosed,
    Cases.Type AS CaseType,
    Cases.Subject AS CaseSubject,
    AccountsMD.AccountId AS AccountId,
    AccountsMD.Name AS AccountName,
    AccountsMD.Type AS AccountType,
    AccountsMD.Phone AS AccountPhone,
    AccountsMD.OwnerId AS AccountOwnerId,
    AccountOwner.Name AS AccountOwnerName,
    AccountsMD.Industry AS AccountIndustry,
    AccountsMD.BillingCountry AS AccountBillingCountry,
    AccountsMD.ShippingCountry AS AccountShippingCountry,
    AccountsMD.CreatedDatestamp AS AccountCreatedDatestamp,
    CaseOwner.Name AS CaseOwnerName,
    --## Agents who have been assigned to Cases
    (Cases.OwnerId NOT LIKE '00G%') AS IsAgentAssigned,
    --## CORTEX-CUSTOMER Consider adding other dimensions from the CalendarDateDimension table as per your requirement
    Cases.CaseCreatedDate AS CaseCreatedDate,
    Cases.CaseCreatedWeek AS CaseCreatedWeek,
    Cases.CaseCreatedMonth AS CaseCreatedMonth,
    Cases.CaseCreatedQuarter AS CaseCreatedQuarter,
    Cases.CaseCreatedYear AS CaseCreatedYear,
    Cases.CaseClosedDate AS CaseClosedDate,
    Cases.CaseClosedWeek AS CaseClosedWeek,
    Cases.CaseClosedMonth AS CaseClosedMonth,
    Cases.CaseClosedQuarter AS CaseClosedQuarter,
    Cases.CaseClosedYear AS CaseClosedYear
  FROM
    `{{ project_id_tgt }}.{{ sfdc_datasets_reporting }}.Cases` AS Cases
  LEFT JOIN
    `{{ project_id_tgt }}.{{ sfdc_datasets_reporting }}.AccountsMD` AS AccountsMD
    ON
      Cases.AccountId = AccountsMD.AccountId
  LEFT JOIN
    `{{ project_id_tgt }}.{{ sfdc_datasets_reporting }}.UsersMD` AS CaseOwner
    ON
      Cases.OwnerId = CaseOwner.UserId
  LEFT JOIN
    `{{ project_id_tgt }}.{{ sfdc_datasets_reporting }}.UsersMD` AS AccountOwner
    ON
      AccountsMD.OwnerId = AccountOwner.UserId
