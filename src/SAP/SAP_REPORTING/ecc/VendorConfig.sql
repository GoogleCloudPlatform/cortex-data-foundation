SELECT
  MANDT AS Client_MANDT,
  NAME AS NameOfVariantVariable_NAME,
  -- TYPE AS TypeOfSelection_TYPE,
  -- NUMB AS CurrentSelecionNumber_NUMB,
  -- SIGN AS ID_I_E_SIGN,
  -- OPTI AS SelectionOption_OPTI,
  LOW AS LowField_LOW,
  HIGH AS HighField_HIGH
  -- CLIE_INDEP AS CLIE_INDEP
FROM `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.tvarvc`
