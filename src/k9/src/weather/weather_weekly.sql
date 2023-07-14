#--  Copyright 2022 Google Inc.
#
#--  Licensed under the Apache License, Version 2.0 (the "License");
#--  you may not use this file except in compliance with the License.
#--  You may obtain a copy of the License at
#
#--      http://www.apache.org/licenses/LICENSE-2.0
#
#--  Unless required by applicable law or agreed to in writing, software
#--  distributed under the License is distributed on an "AS IS" BASIS,
#--  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#--  See the License for the specific language governing permissions and
#--  limitations under the License.

/* Creates a table with daily weather details with actual (for past) values or
 * with forecat (for future) values.
 *
 * This a weekly aggregate table, built from daily values.   This table is fully refreshed
 * at every run.
 *
 * @param project_id_src: Source Project Id - substitued using Jinja templating.
 * @param k9_datasets_processing: K9 processed dataset - substitued using Jinja templating.
 */

-- TODO: Convert to incremental merge statement to avoid full refresh.
CREATE OR REPLACE TABLE `{{ project_id_src }}.{{ k9_datasets_processing }}.weather_weekly`
AS
SELECT
  country,
  postcode,
  DATE_TRUNC(date, WEEK(MONDAY)) AS week_start_date,
  AVG(min_temp) AS avg_min_temp,
  AVG(max_temp) AS avg_max_temp,
  MIN(min_temp) AS min_temp,
  MAX(max_temp) AS max_temp
FROM `{{ project_id_src }}.{{ k9_datasets_processing }}.weather_daily`
GROUP BY country, postcode, week_start_date;
