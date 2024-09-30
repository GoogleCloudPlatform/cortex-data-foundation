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
  Tasks.TaskId,
  Tasks.AccountId,
  Tasks.ActivityDate,
  Tasks.CallDisposition,
  Tasks.CallDurationInSeconds,
  Tasks.CallObject,
  Tasks.CallType,
  Tasks.CompletedDateTimestamp,
  Tasks.CreatedById,
  Tasks.CreatedDatestamp,
  Tasks.Description,
  Tasks.IsClosed,
  Tasks.IsHighPriority,
  Tasks.IsRecurrence,
  Tasks.IsReminderSet,
  Tasks.LastModifiedById,
  Tasks.LastModifiedDatestamp,
  Tasks.OwnerId,
  Tasks.Priority,
  Tasks.RecurrenceActivityId,
  Tasks.RecurrenceDayOfMonth,
  Tasks.RecurrenceDayOfWeekMask,
  Tasks.RecurrenceEndDateOnly,
  Tasks.RecurrenceInstance,
  Tasks.RecurrenceInterval,
  Tasks.RecurrenceMonthOfYear,
  Tasks.RecurrenceRegeneratedType,
  Tasks.RecurrenceStartDateOnly,
  Tasks.RecurrenceTimeZoneSidKey,
  Tasks.RecurrenceType,
  Tasks.ReminderDateTimestamp,
  Tasks.Status,
  Tasks.Subject,
  Tasks.TaskSubtype,
  Tasks.WhatId,
  Tasks.WhoId,
  --## CORTEX-CUSTOMER Consider adding other dimensions from the CalendarDateDimension table as per your requirement
  DateDimensionCreatedDate.Date AS TaskCreatedDate,
  DateDimensionCreatedDate.CalWeek AS TaskCreatedWeek,
  DateDimensionCreatedDate.CalMonth AS TaskCreatedMonth,
  DateDimensionCreatedDate.CalQuarterStr2 AS TaskCreatedQuarter,
  DateDimensionCreatedDate.CalYear AS TaskCreatedYear
FROM
  `{{ project_id_src }}.{{ sfdc_datasets_cdc }}.tasks` AS Tasks
LEFT JOIN
  `{{ project_id_src }}.{{ k9_datasets_processing }}.calendar_date_dim` AS DateDimensionCreatedDate
  ON
    DATE(Tasks.CreatedDatestamp) = DateDimensionCreatedDate.Date

