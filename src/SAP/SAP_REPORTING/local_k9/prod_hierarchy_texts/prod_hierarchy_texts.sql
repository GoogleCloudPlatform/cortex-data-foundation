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

CREATE TABLE IF NOT EXISTS `{{ project_id_tgt  }}.{{ dataset_reporting_tgt }}.prod_hierarchy_texts` (
    mandt	STRING,
    prodh1	STRING,
    prodh2	STRING,
    prodh3	STRING,
    HierText1	STRING,
    prodh4	STRING,
    HierText2	STRING
);

MERGE `{{ project_id_tgt  }}.{{ dataset_reporting_tgt }}.prod_hierarchy_texts` T
USING(
    WITH h1_h2 AS (
      SELECT h1.mandt,
        h1.prodh AS prodh1,
        h2.prodh AS prodh2
      FROM `{{ project_id_src  }}.{{ dataset_cdc_processed }}.t179` AS h1
      LEFT OUTER JOIN `{{ project_id_src  }}.{{ dataset_cdc_processed }}.t179` AS h2
        ON starts_with(h2.prodh, h1.prodh) AND h1.mandt = h2.mandt
      WHERE h1.stufe = '1'
        AND h2.stufe = '2'
    ),
    h1_h2_h3 AS (
      SELECT h1_h2.mandt,
        h1_h2.prodh1 AS prodh1,
        h1_h2.prodh2 AS prodh2,
        h3.prodh AS prodh3,
        mara.prdha AS prodh4
      FROM h1_h2 AS h1_h2
      LEFT OUTER JOIN `{{ project_id_src  }}.{{ dataset_cdc_processed }}.t179` AS h3
        ON starts_with(h3.prodh, h1_h2.prodh2) AND h1_h2.mandt = h3.mandt
      INNER JOIN `{{ project_id_src  }}.{{ dataset_cdc_processed }}.mara` AS mara
    ## CORTEX-CUSTOMER: Modify the left clause to match the number of starting characters
    ## --so that the hierarchy in the material master matches the higher level one
    ## -- Alternatively, use starts_with as in the example above
        ON left(mara.prdha, 5 ) = h3.prodh
          AND mara.mandt = h3.mandt
      WHERE h3.stufe = '3'
      ORDER BY mara.prdha
    )
    SELECT DISTINCT
      hier.mandt,
      hier.prodh1,
      hier.prodh2,
      hier.prodh3,
      txt.vtext AS HierText1,
      hier.prodh4,
      txt2.vtext AS HierText2
    FROM h1_h2_h3 AS hier
    INNER JOIN `{{ project_id_src  }}.{{ dataset_cdc_processed }}.t179t` AS txt
      ON hier.mandt = txt.mandt
        AND hier.prodh3 = txt.prodh
    INNER JOIN `{{ project_id_src  }}.{{ dataset_cdc_processed }}.t179t` AS txt2
      ON hier.mandt = txt2.mandt
        AND hier.prodh4 = txt2.prodh
    WHERE txt.spras = 'E'
      AND txt2.spras = 'E' ) AS S
  ON S.mandt = T.mandt AND S.prodh1 = T.prodh1 AND S.prodh2 = T.prodh2 AND S.prodh3 = T.prodh3 AND S.prodh4 = T.prodh4
WHEN MATCHED THEN UPDATE SET T.mandt = S.mandt, T.prodh1 = S.prodh1, T.prodh2 = S.prodh2, T.prodh3 = S.prodh3, T.HierText1 = S.HierText1, T.prodh4 = S.prodh4, T.HierText2 = S.HierText2
WHEN NOT MATCHED THEN INSERT (mandt, prodh1, prodh2, prodh3, HierText1, prodh4, HierText2)
    VALUES (mandt, prodh1, prodh2, prodh3, HierText1, prodh4, HierText2)
