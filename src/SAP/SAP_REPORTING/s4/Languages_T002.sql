SELECT
  T002.SPRAS AS LanguageKey_SPRAS,
  T002.LASPEZ AS LanguageSpecifications_LASPEZ,
  T002.LAHQ AS DegreeOfTranslationOfLanguage_LAHQ,
  T002.LAISO AS TwoCharacterSapLanguageCode_LAISO
FROM `{{ project_id_src }}.{{ dataset_cdc_processed_s4 }}.t002` AS T002
