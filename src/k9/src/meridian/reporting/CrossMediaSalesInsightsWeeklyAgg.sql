# -- Copyright 2025 Google LLC
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

/* Cross-Media transformation view for Meridian input. */

WITH
  XMEDIA_PIVOT AS (
    SELECT * FROM (
      SELECT
        ReportDate,
        EXTRACT(YEAR FROM ReportDate) AS ReportYear,
        CountryCode,
        CountryName,
        SourceSystem,
        TotalImpressions,
        TotalClicks,
        TotalCostInTargetCurrency,
        TargetCurrency
      FROM `{{ project_id_tgt }}.{{ k9_datasets_reporting }}.CrossMediaCampaignDailyAgg`
      WHERE TargetCurrency = 'USD'
    )
    PIVOT (
      SUM(TotalImpressions) AS impressions,
      SUM(TotalClicks) AS clicks,
      SUM(TotalCostInTargetCurrency) AS cost
      FOR SourceSystem IN ('DV360' AS YouTube, 'Meta', 'TikTok', 'GoogleAds')
    )
  ),
  CENSUS_POPULATION AS (
    SELECT
      country_name AS country_name,
      midyear_population AS country_population,
      year AS year
    FROM `bigquery-public-data.census_bureau_international.midyear_population`
  )
  {%- if k9_meridian_sales_data_source_type == 'SAP' -%},
  SALES AS (
    SELECT
      so.DocumentDate_AUDAT AS order_date,
      cu.CountryKey_LAND1 AS order_country,
      COUNT(so.SalesDocument_VBELN) AS order_numbers,
      SUM(so.CumulativeOrderQuantity_KWMENG * so.NetPrice_NETPR * cc.ExchangeRate_UKURS) AS order_revenue
    FROM
      `{{ project_id_tgt }}.{{ k9_meridian_sales_dataset_id }}.SalesOrders_V2` AS so
    LEFT JOIN
      `{{ project_id_tgt }}.{{ k9_meridian_sales_dataset_id }}.CustomersMD` AS cu
      ON
        so.Client_MANDT = cu.Client_MANDT
        AND so.SoldToParty_KUNNR = cu.CustomerNumber_KUNNR
    LEFT JOIN
      `{{ project_id_tgt }}.{{ k9_meridian_sales_dataset_id }}.CurrencyConversion` AS cc
      ON
        so.Client_MANDT = cc.Client_MANDT
        AND so.Currency_WAERK = cc.FromCurrency_FCURR
        AND so.DocumentDate_AUDAT = cc.ConvDate
        AND cc.ExchangeRateType_KURST = 'M'
    WHERE cc.ToCurrency_TCURR = 'USD'
    GROUP BY 1, 2
  )
  {% endif -%}
  {%- if k9_meridian_sales_data_source_type == 'OracleEBS' -%},
  SALES AS (
    SELECT
      ORDERED_DATE AS order_date,
      SOLD_TO_CUSTOMER_COUNTRY AS order_country,
      NUM_ORDERS AS order_numbers,
      SUM(a.TOTAL_ORDERED) AS order_revenue
    FROM
      `{{ project_id_tgt }}.{{ k9_meridian_sales_dataset_id }}.SalesOrdersDailyAgg`,
      UNNEST(LINES) AS l,
      UNNEST(l.AMOUNTS) AS a
    GROUP BY 1, 2, 3
  )
  {% endif -%}
  {%- if k9_meridian_sales_data_source_type == 'BYOD' -%},
  -- ## CORTEX-CUSTOMER: Please join with sales data table here as required for your specific meridian run use case. The following is a sample code only.
  SALES AS (
    SELECT
      CURRENT_DATE AS order_date,
      'AA' AS order_country,
      0 AS order_numbers,
      0 AS order_revenue
  )
  {% endif -%}
SELECT
  xp.CountryCode AS geo,
  DATE_TRUNC(xp.ReportDate, WEEK (MONDAY)) AS time,
  xp.TargetCurrency AS target_currency,
  SUM(COALESCE(xp.impressions_TikTok, 0)) AS Tiktok_impression,
  SUM(COALESCE(xp.impressions_Meta, 0)) AS Meta_impression,
  SUM(COALESCE(xp.impressions_YouTube, 0)) AS YouTube_impression,
  SUM(COALESCE(xp.impressions_GoogleAds, 0)) AS GoogleAds_impression,
  ROUND(SUM(COALESCE(xp.cost_TikTok, 0)), 3) AS Tiktok_spend,
  ROUND(SUM(COALESCE(xp.cost_Meta, 0)), 3) AS Meta_spend,
  ROUND(SUM(COALESCE(xp.cost_YouTube, 0)), 3) AS YouTube_spend,
  ROUND(SUM(COALESCE(xp.cost_GoogleAds, 0)), 3) AS GoogleAds_spend,
  -- ## CORTEX-CUSTOMER: Please add conversions data here if required for your specific meridian run use case.
  0 AS conversions,
  {%- if ( k9_meridian_sales_data_source_type == 'OracleEBS' or k9_meridian_sales_data_source_type == 'SAP' ) %}
  SUM(COALESCE(sa.order_numbers, 0)) AS number_of_sales_orders,
  ROUND(AVG(COALESCE(SAFE_DIVIDE(sa.order_revenue, sa.order_numbers), 0)), 2) AS average_revenue_per_sales_order,
  {% endif -%}
  {%- if k9_meridian_sales_data_source_type == 'BYOD' %}
  -- ## CORTEX-CUSTOMER: Please add sales data here if required for your specific meridian run use case. number_of_sales_orders maps to column kpi and average_revenue_per_sales_order maps to column revenue_per_kpi in meridian model input schema.
  0 AS number_of_sales_orders,
  0 AS average_revenue_per_sales_order,
  {% endif -%}
  AVG(cs.country_population) AS population
FROM
  XMEDIA_PIVOT AS xp
LEFT JOIN
  CENSUS_POPULATION AS cs
  ON
    xp.ReportYear = cs.year
    AND xp.CountryName = cs.country_name
{% if ( k9_meridian_sales_data_source_type == 'SAP' ) -%}
LEFT JOIN
  SALES AS sa
  ON
    xp.ReportDate = sa.order_date
    AND xp.CountryCode = sa.order_country
{% endif -%}
{% if ( k9_meridian_sales_data_source_type == 'OracleEBS' ) -%}
LEFT JOIN
  SALES AS sa
  ON
    xp.ReportDate = sa.order_date
    AND xp.CountryName = sa.order_country
{% endif -%}
{% if ( k9_meridian_sales_data_source_type == 'BYOD' ) -%}
-- ## CORTEX-CUSTOMER: Please check if your sales data has been joined with cross-media data. Also, Modify the join condition on code (use xp.CountryCode = sa.order_country) or name (use xp.CountryName = sa.order_country) as present in you base table)
LEFT JOIN
  SALES AS sa
  ON
    xp.ReportDate = sa.order_date
    AND xp.CountryCode = sa.order_country
{% endif -%}
GROUP BY
  1, 2, 3
ORDER BY
  1, 2