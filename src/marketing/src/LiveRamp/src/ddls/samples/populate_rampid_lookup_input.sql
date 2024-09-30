# -- Copyright 2024 Google LLC
# --
# -- Licensed under the Apache License, Version 2.0 (the "License");
# -- you may not use this file except in compliance with the License.
# -- You may obtain a copy of the License at
# --
# --     https://www.apache.org/licenses/LICENSE-2.0
# --
# -- Unless required by applicable law or agreed to in writing, software
# -- distributed under the License is distributed on an "AS IS" BASIS,
# -- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# -- See the License for the specific language governing permissions and
# -- limitations under the License.

/* Sample script to populate RampId Lookup Input table from
* Cortex SFDC Reporting dataset.
*/

INSERT INTO `{{ project_id_src }}.{{ marketing_liveramp_datasets_cdc }}.rampid_lookup_input`
(
  id, segment_name, source_system_name, name, email, phone_number, postal_code, is_processed,
  load_timestamp, processed_timestamp
)
/* Due to how LiveRamp API handles data, the input table should not contain duplicate PII
* information. This script performs a form of deduplication to make sure input records are
* successfully processed with LiveRamp APIs.
*/
WITH
  DeduplicatedLeads AS (
    SELECT
      MAX(LeadId) AS LeadId,
      Industry,
      Email,
      Name,
      Phone,
      PostalCode
    FROM `{{ project_id_tgt }}.{{ sfdc_datasets_reporting }}.Leads`
    -- Filter for relevant segments, or remove the WHERE clause to include every segment.
    WHERE
      Industry IN ('Media', 'Entertainment', 'Manufacturing')
    GROUP BY
      2, 3, 4, 5, 6
  )
SELECT
  LeadId AS id,
  Industry AS segment_name,
  'SalesForce' AS source_system_name,
  Name AS name,
  Email AS email,
  Phone AS phone_number,
  PostalCode AS postal_code,
  FALSE AS is_processed,
  CURRENT_TIMESTAMP() AS load_timestamp,
  CAST(NULL AS TIMESTAMP) AS processed_timestamp
FROM DeduplicatedLeads
