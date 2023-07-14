#--  Copyright 2023 Google Inc.
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

-- Ads AdGroups that have mappings in the mapping spreadsheet
-- different from the most recent mapping run results stored in ads_to_sap_prod_hier_log.
-- This view is needed for ignoring all Ad Groups that were manually modified
-- in the spreadsheet.
--

CREATE OR REPLACE VIEW `{{ project_id_src }}.{{ dataset_k9_processing }}.ads_to_sap_modified_mappings` AS
(
  WITH
  ordered_log AS
  (
    SELECT campaign_id, ad_group_id, is_negative, prodh,
      RANK() OVER (ORDER BY mapping_timestamp DESC) as _rn
    FROM `{{ project_id_src }}.{{ dataset_k9_processing }}.ads_to_sap_prod_hier_log`
  ),

  last_log AS (
    SELECT campaign_id, ad_group_id,
      STRING_AGG( CONCAT(IF(is_negative, "-|||", "+|||"), prodh), "|||" ORDER BY ad_group_id, prodh) as combined_mappings
    FROM ordered_log
    WHERE _rn = 1
    GROUP BY campaign_id, ad_group_id
  ),
  combined_spreadsheet as (
    SELECT
      campaign_id, ad_group_id,
        STRING_AGG( CONCAT(IF(is_negative, "-|||", "+|||"), prodh), "|||" ORDER BY ad_group_id, prodh) as combined_mappings
    FROM `{{ project_id_src }}.{{ dataset_k9_processing }}.ads_to_sap_prod_hier`
    GROUP BY campaign_id, ad_group_id
  )

  SELECT combined_spreadsheet.ad_group_id
  FROM combined_spreadsheet
    LEFT JOIN last_log
    ON last_log.ad_group_id = combined_spreadsheet.ad_group_id
  WHERE last_log.combined_mappings != combined_spreadsheet.combined_mappings
      OR last_log.combined_mappings IS NULL
)
