SELECT
  VBAP.MANDT AS Client_MANDT,
  VBAP.VBELN AS SDDocumentNumber_VBELN,
  VBAP.POSNR AS ItemNumberOfTheSdDocument_POSNR,
  VBAP.RFSTA AS ReferenceStatus_RFSTA,
  VBAP.RFGSA AS OverallStatusOfReference_RFGSA,
  VBAP.BESTA AS ConfirmationStatusOfDocumentItem_BESTA,
  VBAP.LFSTA AS DeliveryStatus_LFSTA,
  VBAP.LFGSA AS OverallDeliveryStatusOfTheItem_LFGSA,
  VBAP.WBSTA AS GoodsMovementStatus_WBSTA,
  LIPS.FKSTA AS BillingStatusOfDelivery_FKSTA,
  VBAP.FKSAA AS BillingStatusForOrder_FKSAA,
  VBAP.ABSTA AS RejectionStatusForSdItem_ABSTA,
  VBAP.GBSTA AS OverallProcessingStatusOfTheSdDocumentItem_GBSTA,
  LIPS.KOSTA AS PickingStatusputawayStatus_KOSTA,
  LIPS.LVSTA AS StatusOfWarehouseManagementActivities_LVSTA,
  VBAP.UVALL AS GeneralIncompletionStatusOfItem_UVALL,
  VBAP.UVVLK AS IncompletionStatusOfTheItemWithRegardToDelivery_UVVLK,
  VBAP.UVFAK AS ItemIncompletionStatusWithRespectToBilling_UVFAK,
  VBAP.UVPRS AS PricingForItemIsIncomplete_UVPRS,
  LIPS.FKIVP AS IntercompanyBillingStatus_FKIVP,
  VBAP.UVP01 AS CustomerReserves1_ItemStatus_UVP01,
  VBAP.UVP02 AS CustomerReserves2_ItemStatus_UVP02,
  VBAP.UVP03 AS ItemReserves3_ItemStatus_UVP03,
  VBAP.UVP04 AS ItemReserves4_ItemStatus_UVP04,
  VBAP.UVP05 AS CustomerReserves5_ItemStatus_UVP05,
  LIPS.PKSTA AS PackingStatusOfItem_PKSTA,
  LIPS.KOQUA AS ConfirmationStatusOfPickingputaway_KOQUA,
  VBAP.CMPPI AS StatusOfCreditCheckAgainstFinancialDocument_CMPPI,
  VBAP.CMPPJ AS StatusOfCreditCheckAgainstExportCreditInsurance_CMPPJ,
  LIPS.UVPIK AS IncompleteStatusOfItemForPickingputaway_UVPIK,
  LIPS.UVPAK AS IncompleteStatusOfItemForPackaging_UVPAK,
  LIPS.UVWAK AS IncompleteStatusOfItemRegardingGoodsIssue_UVWAK,
  VBAP.DCSTA AS DelayStatus_DCSTA,
  NULL AS RevenueDeterminationStatus_RRSTA,
  LIPS.VLSTP AS DecentralizedWhseProcessing_VLSTP,
  VBAP.FSSTA AS BillingBlockStatusForItems_FSSTA,
  VBAP.LSSTA AS DeliveryBlockStatusForItem_LSSTA,
  LIPS.PDSTA AS PodStatusOnItemLevel_PDSTA,
  VBAP.MANEK AS ManualCompletionOfContract_MANEK,
  LIPS.HDALL AS InboundDeliveryItemNotYetComplete__onHold___HDALL,
  NULL AS Indicator_StockableTypeSwitchedIntoStandardProduct_LTSPS,
  NULL AS AllocationStatusOfASalesDocumentItem_FSH_AR_STAT_ITM,
  NULL AS StatusOfSalesOrderItem_MILL_VS_VSSTA,
  ( CASE LIPS.FKSTA
    WHEN 'A' THEN 'Not Yet Processed'
    WHEN 'B' THEN 'Partially Processed'
    WHEN 'C' THEN 'Completely Processed'
    END ) AS Billing_Status,
  ( CASE VBAP.LFSTA
      WHEN 'A' THEN 'Not Yet Processed'
      WHEN 'B' THEN 'Partially Processed'
      WHEN 'C' THEN 'Completely Processed'
    END ) AS Delivery_Status
FROM
  `{{ project_id_src }}.{{ dataset_cdc_processed_s4 }}.vbap` AS VBAP
LEFT JOIN
  `{{ project_id_src }}.{{ dataset_cdc_processed_s4 }}.lips` AS LIPS
  ON
    VBAP.VBELN = LIPS.VGBEL
    AND VBAP.POSNR = LIPS.VGPOS
    AND VBAP.MANDT = LIPS.MANDT
