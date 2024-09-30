SELECT
  T005S.MANDT AS Client_MANDT,
  T005S.LAND1 AS CountryKey_LAND1,
  T005S.BLAND AS Region_BLAND,
  T005S.FPRCD AS ProvincialTaxCode_FPRCD,
  T005S.HERBL AS StateOfManufacture_HERBL
FROM
  `{{ project_id_src }}.{{ dataset_cdc_processed_s4 }}.t005s` AS T005S

