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
  Cases.CaseId,
  Cases.AccountId,
  Cases.CaseNumber,
  Cases.ClosedDatestamp,
  Cases.Comments,
  Cases.ContactEmail,
  Cases.ContactFax,
  Cases.ContactId,
  Cases.ContactMobile,
  Cases.ContactPhone,
  Cases.CreatedById,
  Cases.CreatedDatestamp,
  Cases.Description,
  Cases.EntitlementId,
  Cases.Isclosed,
  Cases.IsEscalated,
  Cases.Language,
  Cases.MasterRecordId,
  Cases.Origin,
  Cases.OwnerId,
  Cases.ParentId,
  Cases.Priority,
  Cases.Reason,
  Cases.RecordTypeId,
  Cases.Status,
  Cases.Subject,
  Cases.SuppliedCompany,
  Cases.SuppliedEmail,
  Cases.SuppliedName,
  Cases.SuppliedPhone,
  Cases.Type,
  Cases.LastModifiedById,
  Cases.LastModifiedDatestamp,
  Cases.LastReferencedDatestamp,
  Cases.LastViewedDatestamp,
  Cases.SlaExitDatestamp,
  Cases.SlaStartDatestamp,
  --## CORTEX-CUSTOMER Consider adding other dimensions from the CalendarDateDimension table as per your requirement
  DateDimensionCreatedDate.Date AS CaseCreatedDate,
  DateDimensionCreatedDate.CalWeek AS CaseCreatedWeek,
  DateDimensionCreatedDate.CalMonth AS CaseCreatedMonth,
  DateDimensionCreatedDate.CalQuarterStr2 AS CaseCreatedQuarter,
  DateDimensionCreatedDate.CalYear AS CaseCreatedYear,
  DateDimensionClosedDate.Date AS CaseClosedDate,
  DateDimensionClosedDate.CalWeek AS CaseClosedWeek,
  DateDimensionClosedDate.CalMonth AS CaseClosedMonth,
  DateDimensionClosedDate.CalQuarterStr2 AS CaseClosedQuarter,
  DateDimensionClosedDate.CalYear AS CaseClosedYear
FROM
  `{{ project_id_src }}.{{ sfdc_datasets_cdc }}.cases` AS Cases
LEFT JOIN
  `{{ project_id_src }}.{{ k9_datasets_processing }}.calendar_date_dim` AS DateDimensionCreatedDate
  ON
    DATE(Cases.CreatedDatestamp) = DateDimensionCreatedDate.Date
LEFT JOIN
  `{{ project_id_src }}.{{ k9_datasets_processing }}.calendar_date_dim` AS DateDimensionClosedDate
  ON
    DATE(Cases.ClosedDatestamp) = DateDimensionClosedDate.Date
