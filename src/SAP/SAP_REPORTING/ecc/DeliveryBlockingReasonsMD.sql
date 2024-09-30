SELECT
  TVLST.MANDT AS Client_MANDT,
  TVLST.SPRAS AS LanguageKey_SPRAS,
  TVLST.LIFSP AS DefaultDeliveryBlock_LIFSP,
  TVLST.VTEXT AS DeliveryBlockReason_VTEXT,
FROM `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.tvlst` AS TVLST
