SELECT
  T005K.MANDT AS Client_MANDT,
  T005K.LAND1 AS CountryKey_LAND1,
  T005K.TELEFFROM AS InternationalDialingCodeForTelephonefax_TELEFFROM,
  T005K.TELEFTO AS CountryTelephonefaxDiallingCode_TELEFTO,
  T005K.TELEFRM AS DigitToBeDeletedForCallsFromAbroad_TELEFRM,
  T005K.TELEXFROM AS ForeignDiallingCodeForTelex_TELEXFROM,
  T005K.TELEXTO AS ForeignDiallingCodeForTelex_TELEXTO,
  T005K.MOBILE_SMS AS Indicator_MobileTelephonesAreSmsEnabledByDefault_MOBILE_SMS
FROM `{{ project_id_src }}.{{ dataset_cdc_processed_s4 }}.t005k` AS T005K
