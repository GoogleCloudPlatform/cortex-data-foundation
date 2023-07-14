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

#-- Ads daily stats grouped by product hierarchy and country.

CREATE OR REPLACE VIEW
  `{{ project_id_tgt }}.{{ dataset_k9_reporting }}.AdsDailyStatsByHierAndCountry`
AS
SELECT
  _DATA_DATE AS Date,
  prodh AS ProductHierarchy,
  fullhier AS ProductHierarchyText,
  geo_targets.Country_Code AS CountryCode,
  SUM(metrics_clicks) AS Clicks,
  SUM(metrics_cost_micros / 10e6) AS Cost,
  SUM(metrics_impressions) AS Impressions
FROM
  `{{ project_id_src }}.{{ dataset_ads }}.ads_GeoStats_{{ ads_account }}` geo_stats
INNER JOIN `{{ project_id_src }}.{{ dataset_k9_processing }}.ads_to_sap_prod_hier` mappings
  ON geo_stats.ad_group_id = mappings.ad_group_id
  AND mappings.is_negative IS FALSE
INNER JOIN `{{ project_id_src }}.{{ dataset_k9_processing }}.ads_geo_targets` geo_targets
  ON geographic_view_country_criterion_id = geo_targets.Criteria_ID
GROUP BY
  1, 2, 3, 4
