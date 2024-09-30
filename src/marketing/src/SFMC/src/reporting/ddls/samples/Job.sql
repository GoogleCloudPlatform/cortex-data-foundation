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

/* This is a sample SQL script showing how to use the JobAgg table to report
* simple as well as derived measures.
*
* JobAgg table contains base measures which are additive in nature.
* Here, we use them to calculate rate measures which are non-additive in nature.
*
* This or similar SQL can be used to generate BI report to show delivery rates,
* click rates etc for email campaigns.
*/

SELECT
  CONCAT(EmailName, ' ', JobID) AS EmailBroadcast,
  SchedDate,
  TotalSent,
  TotalOpen,
  TotalUniqueOpen,
  TotalClick,
  TotalUniqueClick,
  TotalBounce,
  TotalUnsubscribe,
  TotalDelivered,
  SAFE_DIVIDE(TotalSent - TotalBounce, TotalSent) * 100 AS DeliveryRate,
  SAFE_DIVIDE(TotalUniqueClick, TotalDelivered) * 100 AS ClickRate,
  SAFE_DIVIDE(TotalUniqueOpen, TotalDelivered) * 100 AS OpenRate,
  SAFE_DIVIDE(TotalBounce, TotalSent) * 100 AS BounceRate,
  SAFE_DIVIDE(TotalUnsubscribe, TotalSent) * 100 AS UnsubscribeRate
FROM `{{ project_id_tgt }}.{{ marketing_sfmc_datasets_reporting }}.JobAgg`
