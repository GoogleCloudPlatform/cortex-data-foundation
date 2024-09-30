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

CREATE OR REPLACE VIEW `{{ project_id_tgt }}.{{ dataset_models_tgt }}.retail_recommender`
AS
SELECT 'See instructions in code' AS Material
FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.MaterialsMD` LIMIT 1

-- ##CORTEX-CUSTOMER
-- -- The sample dataset bigquery-public-data.google_analytics_sample.ga_sessions* is only available in the US.
-- -- You can make a manual copy into your region if needed. Uncomment and re-execute after making the dataset available.
-- SELECT
--   CONCAT(fullVisitorID, '-', CAST(visitNumber AS STRING), '-', CAST(hitNumber AS STRING))
--   AS visitorId_session_hit,
--   LEAD(time, 1)
--   OVER (
--     PARTITION BY CONCAT(fullVisitorID, '-', CAST(visitNumber AS STRING))
--     ORDER BY
--       time ASC
--   )
--   - time AS pageview_duration
-- FROM
--   `bigquery-public-data.google_analytics_sample.ga_sessions*`,  --noqa: L057
--   UNNEST(hits) AS hit
-- -- This model requires Training Matrix Factorization reservations to be available and a Google Analytics dataset.
-- -- Enable the reservation for your project and deploy the following code:
-- CREATE OR REPLACE MODEL `{{ project_id_tgt }}.{{ dataset_models_tgt }}.retail_recommender`
-- OPTIONS(model_type='matrix_factorization',
--         user_col='visitorId',
--         item_col='itemId',
--         rating_col='session_duration',
--         feedback_type='implicit'
--         )
-- AS
-- select * from (
--      WITH
--     durations AS (
--       --calculate pageview durations
--       SELECT
--         CONCAT(fullVisitorID,'-',
--              CAST(visitNumber AS STRING),'-',
--              CAST(hitNumber AS STRING) ) AS visitorId_session_hit,
--         LEAD(time, 1) OVER (
--           PARTITION BY CONCAT(fullVisitorID,'-',CAST(visitNumber AS STRING))
--           ORDER BY
--           time ASC ) - time AS pageview_duration
--       FROM
--         `bigquery-public-data.google_analytics_sample.ga_sessions*`,
--         UNNEST(hits) AS hit
--     ),
--     prodview_durations AS (
--       --filter for product detail pages only
--       SELECT
--         CONCAT(fullVisitorID,'-',CAST(visitNumber AS STRING)) AS visitorId,
--         productSKU AS itemId,
--         IFNULL(dur.pageview_duration,
--           1) AS pageview_duration,
--       FROM
--         `bigquery-public-data.google_analytics_sample.ga_sessions_2016*` t,
--         UNNEST(hits) AS hits,
--         UNNEST(hits.product) AS hits_product
--       JOIN
--         durations dur
--       ON
--         CONCAT(fullVisitorID,'-',
--                CAST(visitNumber AS STRING),'-',
--                CAST(hitNumber AS STRING)) = dur.visitorId_session_hit
--       WHERE
--       #action_type: Product detail views = 2
--       eCommerceAction.action_type = "2"
--     ),
--     aggregate_web_stats AS(
--       --sum pageview durations by visitorId, itemId
--       SELECT
--         p.visitorId,
--         p.itemId,
--         -- m.MaterialText_MAKTX,
--         SUM(p.pageview_duration) AS session_duration
--       FROM
--         prodview_durations as p
--       -- JOIN `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.MaterialsMD` as m
--       -- ON p.itemId = m.MaterialNumber_MATNR
--       -- WHERE m.Client_MANDT = '{{ mandt }}'
--       -- AND m.Language_SPRAS = 'E'
--       GROUP BY
--         visitorId,
--         itemId )
--     SELECT
--       *
--     FROM
--       aggregate_web_stats )
