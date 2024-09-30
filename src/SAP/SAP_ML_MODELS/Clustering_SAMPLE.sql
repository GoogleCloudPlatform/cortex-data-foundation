#-- Copyright 2022 Google LLC
#--
#-- Licensed under the Apache License, Version 2.0 (the "License");
#-- you may not use this file except in compliance with the License.
#-- You may obtain a copy of the License at
#--
#--     https://www.apache.org/licenses/LICENSE-2.0
#--
#-- Unless required by applicable law or agreed to in writing, software
#-- distributed under the License is distributed on an "AS IS" BASIS,
#-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#-- See the License for the specific language governing permissions and
#-- limitations under the License.

CREATE OR REPLACE
MODEL
`{{ project_id_tgt }}.{{ dataset_models_tgt }}.clustering_nmodel`
OPTIONS (
  model_type = 'kmeans',
  num_clusters = 3)
AS
WITH
  orders_customer AS ( -- noqa: L045
    SELECT
      SoldtoParty_KUNNR AS vbap_kunnr,
      SalesDocument_VBELN,
      LanguageKey_SPRAS,
      CountryKey_LAND1,
      1 AS order_counter,
      avg(NetPrice_NETWR) AS avg_item,
      sum(counter) AS item_count
    FROM (
      SELECT
        SO.SoldtoParty_KUNNR,
        SO.NetPrice_NETWR,
        SO.SalesDocument_VBELN,
        CM.CountryKey_LAND1,
        CM.LanguageKey_SPRAS,
        1 AS counter
      FROM
        `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.SalesOrders` AS SO
      INNER JOIN `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.CustomersMD` AS CM
        ON
          SO.Client_MANDT = CM.Client_MANDT
          AND SO.SoldtoParty_KUNNR = CM.CustomerNumber_KUNNR
    )
    GROUP BY vbap_kunnr, SalesDocument_VBELN, LanguageKey_SPRAS, CountryKey_LAND1
    ORDER BY SalesDocument_VBELN
  ),

  CUSTOMER_STATS AS ( -- noqa: L045
    SELECT
      vbap_kunnr,
      LanguageKey_SPRAS,
      CountryKey_LAND1,
      avg(avg_item) AS avg_item_price,
      avg(item_count) AS avg_item_count,
      sum(order_counter) AS order_count
    FROM orders_customer
    GROUP BY vbap_kunnr, LanguageKey_SPRAS, CountryKey_LAND1
  )

SELECT * EXCEPT (vbap_kunnr) -- noqa: L044
FROM customer_stats
