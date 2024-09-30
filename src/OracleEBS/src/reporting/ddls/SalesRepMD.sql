-- Copyright 2024 Google LLC
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--      https://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- SalesRepMD dimension.

SELECT
  SALESREP_ID,
  ORG_ID AS BUSINESS_UNIT_ID,
  NAME
FROM
  `{{ project_id_src }}.{{ oracle_ebs_datasets_cdc }}.RA_SALESREPS_ALL`
