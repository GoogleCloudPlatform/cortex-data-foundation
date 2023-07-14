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

-- Ads AdGroups to SAP Product Hierarchy mapping log.
--

CREATE TABLE IF NOT EXISTS `{{ project_id_src }}.{{ dataset_k9_processing }}.ads_to_sap_prod_hier_log`
(
  mapping_timestamp TIMESTAMP,
  campaign_id INT64,
  ad_group_id INT64,
  is_negative BOOL,
  prodh STRING
)
