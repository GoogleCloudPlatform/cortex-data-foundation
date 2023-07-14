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

-- Ads AdGroups to SAP Product Hierarchy mapping.
-- This view uses `ads_hier_mapping_sheet` which is an external table
-- connected to the mapping spreadsheet.
-- If this view is queried before the mapping spreadsheet is filled with data,
-- BigQuery will throw an error:
-- "Failed to read the spreadsheet. Error message: Unable to parse range: Mapping!A1:D"
--

CREATE OR REPLACE VIEW `{{ project_id_src }}.{{ dataset_k9_processing }}.ads_to_sap_prod_hier` AS
(
  WITH clean_mapping AS
  (
    SELECT TRIM(campaign_name) as m_campaign_name,
      TRIM(ad_group_name) AS m_ad_group_name,
      IF(add_remove = "━", TRUE, FALSE) AS is_negative,
      TRIM(sap_hier) as m_sap_hier
    FROM `{{ project_id_src }}.{{ dataset_k9_processing }}.ads_hier_mapping_sheet`
  )

  SELECT DISTINCT
    ad_groups.campaign_id, campaigns.campaign_name,
    ad_groups.ad_group_id, ad_groups.ad_group_name,
    is_negative, hier.prodh, hier.fullhier
  FROM `{{ project_id_src }}.{{ dataset_ads }}.ads_AdGroup_{{ ads_account }}` ad_groups
  INNER JOIN `{{ project_id_src }}.{{ dataset_ads }}.ads_Campaign_{{ ads_account }}` campaigns
    ON ad_groups.campaign_id = campaigns.campaign_id
  INNER JOIN clean_mapping
    ON clean_mapping.m_campaign_name = campaigns.campaign_name
      AND clean_mapping.m_ad_group_name = ad_groups.ad_group_name
  INNER JOIN `{{ project_id_src }}.{{ dataset_k9_processing }}.prod_hierarchy` hier
    ON m_sap_hier = hier.fullhier
  WHERE ad_groups._DATA_DATE = DATE_SUB(ad_groups._LATEST_DATE, INTERVAL 1 DAY)
  AND campaigns._DATA_DATE = DATE_SUB(campaigns._LATEST_DATE, INTERVAL 1 DAY)
  ORDER BY ad_groups.campaign_id, ad_groups.ad_group_id
)
