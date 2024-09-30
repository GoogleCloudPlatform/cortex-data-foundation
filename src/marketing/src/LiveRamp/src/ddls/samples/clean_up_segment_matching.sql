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

/* Sample script to prepare Cortex tables for reprocessing records.
*
* This script, for a given segment, removes all RampIDs from RampId Lookup
* (output) table.
*
* It also resets all the records in the input table for the segment and
* makes them ready to be re-processed.
*
*/


BEGIN TRANSACTION;

DELETE FROM `{{ project_id_src }}.{{ marketing_liveramp_datasets_cdc }}.rampid_lookup`
WHERE segment_name = 'Sample Segment';

UPDATE `{{ project_id_src }}.{{ marketing_liveramp_datasets_cdc }}.rampid_lookup_input`
SET is_processed = FALSE
WHERE segment_name = 'Sample Segment';

COMMIT TRANSACTION;
