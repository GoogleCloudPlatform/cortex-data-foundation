(WITH h1_h2 AS (
  SELECT h1.prodh AS prodh1, h2.prodh AS prodh2
  FROM `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.t179` AS h1
  LEFT OUTER JOIN `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.t179` AS h2
    ON starts_with(h2.prodh, h1.prodh)
  WHERE h1.stufe = '1'
    AND h2.stufe = '2'
)
SELECT h1_h2.prodh1 AS prodh1,
  h1_h2.prodh2 AS prodh2,
  h3.prodh AS prodh3
FROM h1_h2 AS h1_h2
LEFT OUTER JOIN `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.t179` AS h3
  ON starts_with(h3.prodh, h1_h2.prodh2)
WHERE h3.stufe = '3'
)
