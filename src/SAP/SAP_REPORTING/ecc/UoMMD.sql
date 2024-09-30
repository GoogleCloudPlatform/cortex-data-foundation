SELECT
  T006.MANDT AS Client_MANDT,
  T006.MSEHI AS UnitOfMeasurement_MSEHI,
  T006.KZEX3 AS ThreeCharIndicatorForExternalUnitOfMeasurement_KZEX3,
  T006.KZEX6 AS SixCharIdForExternalUnitOfMeasurement_KZEX6,
  T006T.SPRAS AS LanguageKey_SPRAS, T006T.TXDIM AS DimensionText_TXDIM,
  T006A.MSEHT AS UnitOfMeasurementText__maximum10Characters___MSEHT,
  T006A.MSEHL AS UnitOfMeasurementText__maximum30Characters___MSEHL
#T006.ANDEC AS NoOfDecimalPlacesForRounding_ANDEC,  T006.KZKEH AS CommercialMeasurementUnitId_KZKEH,  T006.KZWOB AS ValueBasedCommitmentIndicator_KZWOB,  T006.KZ1EH AS Indicator__1__Unit__indicatorNotYetDefined___KZ1EH,
#T006.KZ2EH AS Indicator__2__Unit__indicatorNotYetDefined___KZ2EH,  T006.DIMID AS DimensionKey_DIMID,  T006.ZAEHL AS NumeratorForConversionToSiUnit_ZAEHL,  T006.NENNR AS DenominatorForConversionIntoSiUnit_NENNR,
#T006.EXP10 AS BaseTenExponentForConversionToSiUnit_EXP10,  T006.ADDKO AS AdditiveConstantForConversionToSiUnit_ADDKO,  T006.EXPON AS BaseTenExponentForFloatingPointDisplay_EXPON,
#T006.DECAN AS NumberOfDecimalPlacesForNumberDisplay_DECAN,  T006.ISOCODE AS IsoCodeForUnitOfMeasurement_ISOCODE,  T006.PRIMARY AS SelectionFieldForConversionFromIsoCodeToIntCode_PRIMARY,  
#T006.TEMP_VALUE AS Temperature_TEMP_VALUE,
#T006.TEMP_UNIT AS TemperatureUnit_TEMP_UNIT, T006.FAMUNIT AS UnitOfMeasurementFamily_FAMUNIT, T006.PRESS_VAL AS PressureValue_PRESS_VAL, T006.PRESS_UNIT AS UnitOfPressure_PRESS_UNIT
FROM `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.t006` AS T006
INNER JOIN `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.t006t` AS T006T
  ON T006.MANDT = T006T.MANDT
    AND T006.DIMID = T006T.DIMID
INNER JOIN `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.t006a` AS T006A
  ON T006.MANDT = T006A.MANDT
    AND T006A.SPRAS = T006T.SPRAS
    AND T006A.MSEHI = T006.MSEHI
