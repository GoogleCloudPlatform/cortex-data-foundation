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
#-- Weekly Sales and Ads Metrics grouped by Product Hierarchy and Country.

CREATE OR REPLACE VIEW
  `{{ project_id_tgt }}.{{ dataset_k9_reporting }}.WeeklySAPSalesAndAdsStatsByHierAndCountry`
AS
SELECT
  COALESCE(DATE_TRUNC(ads.Date, WEEK), DATE_TRUNC(sales.OrderDate, WEEK)) AS WeekDate,
  COALESCE(ads.ProductHierarchy, sales.ProductHierarchy) AS ProductHierarchy,
  COALESCE(ads.ProductHierarchyText, sales.ProductHierarchyText) AS ProductHierarchyText,
  COALESCE(ads.CountryCode, sales.CountryCode) AS CountryCode,
  SUM(IFNULL(Clicks, 0)) AS Clicks,
  SUM(IFNULL(Cost, 0)) AS Cost,
  SUM(IFNULL(Impressions, 0)) AS Impressions,
  SUM(IFNULL(TotalSalesValue, 0)) AS TotalSalesValue
FROM `{{ project_id_tgt }}.{{ dataset_k9_reporting }}.AdsDailyStatsByHierAndCountry` ads
FULL JOIN `{{ project_id_tgt }}.{{ dataset_k9_reporting }}.DailySalesByHierAndCountry` sales
ON
  DATE_TRUNC(sales.OrderDate, WEEK) = DATE_TRUNC(ads.Date, WEEK)
  AND sales.ProductHierarchy = ads.ProductHierarchy
  AND sales.CountryCode = ads.CountryCode
GROUP BY 1, 2, 3, 4
ORDER BY WeekDate, ProductHierarchy, CountryCode
