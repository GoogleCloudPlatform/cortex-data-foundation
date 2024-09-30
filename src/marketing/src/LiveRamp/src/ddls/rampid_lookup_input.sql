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

/* LiveRamp RampId Lookup Input table.
 * Table contains data from external CRM systems.
 */

CREATE TABLE `{{ project_id_src }}.{{ marketing_liveramp_datasets_cdc }}.rampid_lookup_input`
(
  id STRING NOT NULL,
  segment_name STRING NOT NULL,
  source_system_name STRING NOT NULL,
  name STRING,
  email STRING,
  phone_number STRING,
  postal_code STRING,
  is_processed BOOL NOT NULL,
  load_timestamp TIMESTAMP NOT NULL,
  processed_timestamp TIMESTAMP
)
CLUSTER BY segment_name
OPTIONS (description = 'This table contains PII data from the source CRM system.')
