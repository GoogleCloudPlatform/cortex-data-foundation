#-- Copyright 2024 Google LLC
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

## Following fiscal functions are deprecated, and will be removed in the next release.
## Use fiscal_date_dim table instead.

CREATE OR REPLACE FUNCTION `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.Fiscal_Case1`(
  Ip_Mandt STRING, Ip_Periv STRING, Ip_Date DATE) AS
(
  (
    SELECT
    CONCAT(EXTRACT(YEAR FROM Ip_Date), LPAD(CAST(EXTRACT(MONTH FROM Ip_Date) AS STRING), 3, '0')) AS Op_Period
  )
);
