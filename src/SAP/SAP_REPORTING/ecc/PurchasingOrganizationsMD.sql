SELECT
  t024e.MANDT AS Client_MANDT,
  t024e.EKORG AS PurchasingOrganization_EKORG,
  t024e.EKOTX AS PurchasingOrganizationText_EKOTX,
  t024e.BUKRS AS CompanyCode_BUKRS
-- t024e.TXADR AS SenderLineText_TXADR,
-- t024e.TXKOP AS LetterHeadingText_TXKOP,
-- t024e.TXFUS AS FooterLinesText_TXFUS,
-- t024e.TXGRU AS ComplimentaryCloseText_TXGRU,
-- t024e.KALSE AS GroupCalculationSchema_KALSE,
-- t024e.MKALS AS CalculationSchemaMarketPrice_MKALS,
-- t024e.BPEFF AS EffectivePrice_BPEFF,
-- t024e.BUKRS_NTR AS CompanyCodeSubsequentSettlement_BUKRS_NTR
FROM
  `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.t024e` AS t024e
