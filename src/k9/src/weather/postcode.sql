#--  Copyright 2022 Google Inc.
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


/* Creates a table with all postcodes with centroid informatiun.
 *
 * This script fully refreshes the table.
 *
 * @param project_id_src: Source Project Id - substitued using Jinja templating.
 * @param k9_datasets_processing: K9 processed dataset - substitued using Jinja templating.
 * @param dataset_cdc_processed: SAP CDC processed dataset - substitued using Jinja templating.
 *
 */

-- TODO: Convert to incremental merge statement to avoid full refresh.
CREATE OR REPLACE TABLE `{{ project_id_src }}.{{ k9_datasets_processing }}.postcode`
AS
WITH
  Countries AS (
    SELECT
      (SELECT value FROM layers.all_tags WHERE key = 'ISO3166-1') as country,
      geometry
    ## --CORTEX-CUSTOMER: Replace all occurrences of the dataset below if using a copy or a different name
    FROM `bigquery-public-data.geo_openstreetmap.planet_layers` AS layers
    WHERE layer_class = 'boundary'
      AND layer_name = 'national'
      AND EXISTS (SELECT 1 FROM layers.all_tags WHERE key = 'ISO3166-1')
  ),
  PostCodes AS
  (
    SELECT
      (
        SELECT value
        FROM UNNEST(all_tags)
        WHERE key IN ('addr:postcode', 'postcode', 'postal_code')
        LIMIT 1
      ) AS postcode,
      ST_CENTROID(geometry) AS centroid,
    FROM `bigquery-public-data.geo_openstreetmap.planet_layers` AS layers
    WHERE EXISTS (
      SELECT 1
      FROM layers.all_tags
      WHERE key IN ('addr:postcode', 'postcode', 'postal_code'))
  ),
  PostCodesByCountry AS
  (
    SELECT
      C.country, P.postcode, P.centroid,
      -- Highly possible to have more than one entry in OSM
      -- with the same post code, with same or different centroid.
      ROW_NUMBER() OVER (PARTITION BY C.country, P.postcode) AS rn
    FROM Countries AS C
      INNER JOIN PostCodes AS P ON (ST_WITHIN(P.centroid, C.geometry))
  ),
  -- We need only one entry for a given post code.
  UniquePostCodesByCountry AS
  (
    SELECT country, postcode, centroid
    FROM PostCodesByCountry
    WHERE rn = 1
  ),
  -- Find Postcodes present in Customer table.
  -- Useful to later limit out postcode table by only data which we care for.
  AllCustPostCodes AS (
    SELECT DISTINCT pstlz AS postcode, land1 AS country
    FROM `{{ project_id_src }}.{{ dataset_cdc_processed }}.kna1`
    WHERE NULLIF(pstlz, '') IS NOT NULL
    AND NULLIF(land1, '') IS NOT NULL
  )
-- Although we don't really need to store centroid as text, this is done to
-- facilitate testing.
SELECT P.country, P.postcode, ST_ASTEXT(P.centroid) AS centroid,
FROM UniquePostCodesByCountry AS P
INNER JOIN AllCustPostCodes AS C USING (postcode, country);
