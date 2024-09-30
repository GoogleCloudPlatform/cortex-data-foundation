SELECT
  t148t.MANDT AS Client_MANDT,
  t148t.SPRAS AS LanguageKey_SPRAS,
  t148t.SOBKZ AS SpecialStockIndicator_SOBKZ,
  t148t.SOTXT AS DescriptionOfSpecialStock_SOTXT
FROM `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.t148t` AS t148t
