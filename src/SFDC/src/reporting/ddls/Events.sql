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
  Events.EventId,
  Events.AccountId,
  Events.ActivityDate,
  Events.ActivityDateTimestamp,
  Events.CreatedById,
  Events.CreatedDatestamp,
  Events.Description,
  Events.DurationInMinutes,
  Events.EndDate,
  Events.EndDateTimestamp,
  Events.EventSubtype,
  Events.GroupEventType,
  Events.IsAllDayEvent,
  Events.IsChild,
  Events.IsGroupEvent,
  Events.IsPrivate,
  Events.IsRecurrence,
  Events.IsRecurrence2,
  Events.IsRecurrence2Exception,
  Events.IsRecurrence2Exclusion,
  Events.IsReminderSet,
  Events.LastModifiedById,
  Events.LastModifiedDatestamp,
  Events.Location,
  Events.OwnerId,
  Events.Recurrence2PatternStartDatestamp,
  Events.Recurrence2PatternText,
  Events.Recurrence2PatternTimeZone,
  Events.Recurrence2PatternVersion,
  Events.RecurrenceActivityId,
  Events.RecurrenceDayOfMonth,
  Events.RecurrenceDayOfWeekMask,
  Events.RecurrenceEndDateOnly,
  Events.RecurrenceInstance,
  Events.RecurrenceInterval,
  Events.RecurrenceMonthOfYear,
  Events.RecurrenceStartDateTimestamp,
  Events.RecurrenceTimeZoneSidKey,
  Events.RecurrenceType,
  Events.ReminderDateTimestamp,
  Events.ShowAs,
  Events.StartDateTimestamp,
  Events.Subject,
  Events.WhatId,
  Events.WhoId,
  --## CORTEX-CUSTOMER Consider adding other dimensions from the CalendarDateDimension table as per your requirement
  DateDimensionCreatedDate.Date AS EventCreatedDate,
  DateDimensionCreatedDate.CalWeek AS EventCreatedWeek,
  DateDimensionCreatedDate.CalMonth AS EventCreatedMonth,
  DateDimensionCreatedDate.CalQuarterStr2 AS EventCreatedQuarter,
  DateDimensionCreatedDate.CalYear AS EventCreatedYear
FROM
  `{{ project_id_src }}.{{ sfdc_datasets_cdc }}.events` AS Events
LEFT JOIN
  `{{ project_id_src }}.{{ k9_datasets_processing }}.calendar_date_dim` AS DateDimensionCreatedDate
  ON
    DATE(Events.CreatedDatestamp) = DateDimensionCreatedDate.Date
