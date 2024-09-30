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

/* LiveRamp RampId Lookup Output table.
 * Contains resolved RampIDs and segment names.
 */

CREATE TABLE `{{ project_id_src }}.{{ marketing_liveramp_datasets_cdc }}.rampid_lookup`
(
  segment_name STRING NOT NULL,
  ramp_id STRING NOT NULL,
  recordstamp TIMESTAMP NOT NULL
)
CLUSTER BY segment_name
OPTIONS (description = 'Table that contains RampID and segment names.')
