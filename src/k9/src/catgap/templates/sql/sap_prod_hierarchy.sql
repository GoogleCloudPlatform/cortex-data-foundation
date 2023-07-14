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

-- This view contains expanded product hierarchies of all levels up to max_stufe [3, 9]

CREATE OR REPLACE VIEW `{{ project_id_src }}.{{ dataset_k9_processing }}.prod_hierarchy` AS
(
WITH
Hier AS
(
  SELECT mandt, prodh, CAST(stufe AS INT64) AS stufe FROM `{{ project_id_src }}.{{ dataset_cdc_processed }}.t179`
    WHERE
      stufe IS NOT NULL
      AND CAST(stufe AS INT64) <= GREATEST(3, {{ max_stufe }})
      AND prodh IS NOT NULL
      AND mandt = "{{ mandt }}"
),

Texts AS
(
  SELECT mandt, prodh, vtext FROM `{{ project_id_src }}.{{ dataset_cdc_processed }}.t179t`
    WHERE vtext IS NOT NULL
    AND prodh IS NOT NULL
    AND mandt = "{{ mandt }}"
    AND spras = "E" -- TODO: multiple languages support
),

Combined AS
(
  SELECT DISTINCT
    Texts.mandt as mandt, Texts.prodh as prodh,
    Hier.stufe as stufe, Texts.vtext as vtext,
  FROM Texts INNER JOIN Hier
    ON Hier.mandt = Texts.mandt
    AND Hier.prodh = Texts.prodh
),

Extended AS
(
SELECT
  Combined.mandt AS mandt, Combined.prodh AS prodh,
  Combined.stufe AS stufe, Combined.vtext AS vtext,
  IF(Combined.stufe = 1, "" , CombinedP.prodh) AS parentprodh,
  IF(Combined.stufe = 1, "" , CombinedP.vtext) AS parentvtext,
  Combined1.prodh as prodh1, Combined2.prodh as prodh2, Combined3.prodh as prodh3,
  Combined4.prodh as prodh4, Combined5.prodh as prodh5, Combined6.prodh as prodh6,
  Combined7.prodh as prodh7, Combined8.prodh as prodh8, Combined9.prodh as prodh9,
  Combined1.vtext as vtext1, Combined2.vtext as vtext2, Combined3.vtext as vtext3,
  Combined4.vtext as vtext4, Combined5.vtext as vtext5, Combined6.vtext as vtext6,
  Combined7.vtext as vtext7, Combined8.vtext as vtext8, Combined9.vtext as vtext9
FROM Combined
  LEFT JOIN Combined AS CombinedP
    ON Combined.mandt = CombinedP.mandt AND (CombinedP.stufe = Combined.stufe - 1) AND Combined.prodh LIKE CONCAT(CombinedP.prodh, "%")
  LEFT JOIN Combined AS Combined1
    ON Combined.mandt = Combined1.mandt AND (Combined1.stufe = 1) AND Combined.prodh LIKE CONCAT(Combined1.prodh, "%")
  LEFT JOIN Combined AS Combined2
    ON Combined.mandt = Combined2.mandt AND (Combined2.stufe = 2) AND Combined.prodh LIKE CONCAT(Combined2.prodh, "%")
  LEFT JOIN Combined AS Combined3
    ON Combined.mandt = Combined3.mandt AND (Combined3.stufe = 3) AND Combined.prodh LIKE CONCAT(Combined3.prodh, "%")
  LEFT JOIN Combined AS Combined4
    ON Combined.mandt = Combined4.mandt AND (Combined4.stufe = 4) AND Combined.prodh LIKE CONCAT(Combined4.prodh, "%")
  LEFT JOIN Combined AS Combined5
    ON Combined.mandt = Combined5.mandt AND (Combined5.stufe = 5) AND Combined.prodh LIKE CONCAT(Combined5.prodh, "%")
  LEFT JOIN Combined AS Combined6
    ON Combined.mandt = Combined6.mandt AND (Combined6.stufe = 6) AND Combined.prodh LIKE CONCAT(Combined6.prodh, "%")
  LEFT JOIN Combined AS Combined7
    ON Combined.mandt = Combined7.mandt AND (Combined7.stufe = 7) AND Combined.prodh LIKE CONCAT(Combined7.prodh, "%")
  LEFT JOIN Combined AS Combined8
    ON Combined.mandt = Combined8.mandt AND (Combined8.stufe = 8) AND Combined.prodh LIKE CONCAT(Combined8.prodh, "%")
  LEFT JOIN Combined AS Combined9
    ON Combined.mandt = Combined9.mandt AND (Combined9.stufe = 9) AND Combined.prodh LIKE CONCAT(Combined9.prodh, "%")
)

SELECT
    mandt, prodh, stufe,
    parentprodh, vtext, parentvtext,
    prodh1, prodh2, prodh3,
    prodh4, prodh5, prodh6,
    prodh7, prodh8, prodh9,
    vtext1, vtext2, vtext3,
    vtext4, vtext5, vtext6,
    vtext7, vtext8, vtext9,
    REPLACE(
      RTRIM(
        CONCAT(
          IFNULL(vtext1, ""), "/", IFNULL(vtext2, ""), "/",
          IFNULL(vtext3, ""), "/", IFNULL(vtext4, ""), "/",
          IFNULL(vtext5, ""), "/", IFNULL(vtext6, ""), "/",
          IFNULL(vtext7, ""), "/", IFNULL(vtext8, ""), "/",
          IFNULL(vtext9, "")),
          "/ "),
      "//", "/")
    AS fullhier
  FROM Extended
)
