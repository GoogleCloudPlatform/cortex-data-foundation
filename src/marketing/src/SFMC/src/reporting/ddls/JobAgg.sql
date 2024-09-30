# -- Copyright 2024 Google LLC
# --
# -- Licensed under the Apache License, Version 2.0 (the "License");
# -- you may not use this file except in compliance with the License.
# -- You may obtain a copy of the License at
# --
# --      https://www.apache.org/licenses/LICENSE-2.0
# --
# -- Unless required by applicable law or agreed to in writing, software
# -- distributed under the License is distributed on an "AS IS" BASIS,
# -- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# -- See the License for the specific language governing permissions and
# -- limitations under the License.

/* This table shows job level aggregate measures.*/

WITH
  Sents AS (
    SELECT
      JobID,
      COUNT(JobID) AS TotalSent
    FROM `{{ project_id_tgt }}.{{ marketing_sfmc_datasets_reporting }}.Sent`
    GROUP BY JobID
  ),
  Bounces AS (
    SELECT
      JobID,
      COUNT(JobID) AS TotalBounce
    FROM `{{ project_id_tgt }}.{{ marketing_sfmc_datasets_reporting }}.Bounce`
    GROUP BY JobID
  ),
  Unsubscribes AS (
    SELECT
      JobID,
      COUNT(JobID) AS TotalUnsubscribe,
      COUNTIF(IsUnique) AS TotalUniqueUnsubscribe
    FROM `{{ project_id_tgt }}.{{ marketing_sfmc_datasets_reporting }}.Unsubscribe`
    GROUP BY JobID
  ),
  Opens AS (
    SELECT
      JobID,
      COUNT(JobID) AS TotalOpen,
      COUNTIF(IsUnique) AS TotalUniqueOpen
    FROM `{{ project_id_tgt }}.{{ marketing_sfmc_datasets_reporting }}.Open`
    GROUP BY JobID
  ),
  Clicks AS (
    SELECT
      JobID,
      COUNT(JobID) AS TotalClick,
      COUNTIF(IsUnique) AS TotalUniqueClick
    FROM `{{ project_id_tgt }}.{{ marketing_sfmc_datasets_reporting }}.Click`
    GROUP BY JobID
  )
SELECT
  Jobs.JobID,
  Jobs.EmailName,
  -- SFMC uses Central Standard Time (CST) but do not observe Daylight Saving Time (DST).
  DATE(Jobs.SchedTime, 'Etc/GMT+6') AS SchedDate,
  Jobs.AccountID,
  Jobs.AccountUserID,
  Jobs.FromName,
  Jobs.FromEmail,
  Jobs.DeliveredTime,
  Jobs.CreatedDate,
  Jobs.ModifiedDate,
  Jobs.IsMultipart,
  Jobs.JobStatus,
  Jobs.EmailSubject,
  Jobs.Category,
  Jobs.ResolveLinksWithCurrentData,
  Jobs.SendType,
  Jobs.TriggeredSendCustomerKey,
  Jobs.ModifiedBy,
  Jobs.JobType,
  Jobs.SendClassification,
  Jobs.SendClassificationType,
  COALESCE(Sents.TotalSent, 0) AS TotalSent,
  COALESCE(Opens.TotalOpen, 0) AS TotalOpen,
  COALESCE(Opens.TotalUniqueOpen, 0) AS TotalUniqueOpen,
  COALESCE(Clicks.TotalClick, 0) AS TotalClick,
  COALESCE(Clicks.TotalUniqueClick, 0) AS TotalUniqueClick,
  COALESCE(Bounces.TotalBounce, 0) AS TotalBounce,
  COALESCE(Unsubscribes.TotalUnsubscribe, 0) AS TotalUnsubscribe,
  COALESCE(Unsubscribes.TotalUniqueUnsubscribe, 0) AS TotalUniqueUnsubscribe,
  -- TotalDelivered can not be negative value.
  GREATEST(0, COALESCE(Sents.TotalSent, 0) - COALESCE(Bounces.TotalBounce, 0)) AS TotalDelivered
FROM `{{ project_id_tgt }}.{{ marketing_sfmc_datasets_reporting }}.Job` AS Jobs
LEFT JOIN Sents
  USING (JobID)
LEFT JOIN Bounces
  USING (JobID)
LEFT JOIN Unsubscribes
  USING (JobID)
LEFT JOIN Opens
  USING (JobID)
LEFT JOIN Clicks
  USING (JobID)
