SELECT
  TSPA.mandt AS Client_MANDT,
  TSPA.spart AS Division_SPART,
  TSPAT.spras AS LanguageKey_SPRAS,
  TSPAT.vtext AS DivisionName_VTEXT
FROM
  `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.tspa` AS TSPA
LEFT JOIN
  `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.tspat` AS TSPAT
  ON
    TSPA.MANDT = TSPAT.MANDT
    AND TSPA.SPART = TSPAT.SPART
