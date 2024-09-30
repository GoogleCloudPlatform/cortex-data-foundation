SELECT
 TVFST.MANDT AS Client_MANDT,
 TVFST.SPRAS AS LanguageKey_SPRAS,
 TVFST.FAKSP AS Block_FAKSP,
 TVFST.VTEXT AS BillingBlockReason_VTEXT,
FROM `{{ project_id_src }}.{{ dataset_cdc_processed_s4 }}.tvfst` AS TVFST