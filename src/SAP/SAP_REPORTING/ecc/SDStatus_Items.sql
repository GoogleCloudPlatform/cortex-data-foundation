SELECT
  VBUP.MANDT AS Client_MANDT,
  VBUP.VBELN AS SDDocumentNumber_VBELN,
  VBUP.POSNR AS ItemNumberOfTheSdDocument_POSNR,
  VBUP.RFSTA AS ReferenceStatus_RFSTA,
  VBUP.RFGSA AS OverallStatusOfReference_RFGSA,
  VBUP.BESTA AS ConfirmationStatusOfDocumentItem_BESTA,
  VBUP.LFSTA AS DeliveryStatus_LFSTA,
  VBUP.LFGSA AS OverallDeliveryStatusOfTheItem_LFGSA,
  VBUP.WBSTA AS GoodsMovementStatus_WBSTA,
  VBUP.FKSTA AS BillingStatusOfDelivery_FKSTA,
  VBUP.FKSAA AS BillingStatusForOrder_FKSAA,
  VBUP.ABSTA AS RejectionStatusForSdItem_ABSTA,
  VBUP.GBSTA AS OverallProcessingStatusOfTheSdDocumentItem_GBSTA,
  VBUP.KOSTA AS PickingStatusputawayStatus_KOSTA,
  VBUP.LVSTA AS StatusOfWarehouseManagementActivities_LVSTA,
  VBUP.UVALL AS GeneralIncompletionStatusOfItem_UVALL,
  VBUP.UVVLK AS IncompletionStatusOfTheItemWithRegardToDelivery_UVVLK,
  VBUP.UVFAK AS ItemIncompletionStatusWithRespectToBilling_UVFAK,
  VBUP.UVPRS AS PricingForItemIsIncomplete_UVPRS,
  VBUP.FKIVP AS IntercompanyBillingStatus_FKIVP,
  VBUP.UVP01 AS CustomerReserves1_ItemStatus_UVP01,
  VBUP.UVP02 AS CustomerReserves2_ItemStatus_UVP02,
  VBUP.UVP03 AS ItemReserves3_ItemStatus_UVP03,
  VBUP.UVP04 AS ItemReserves4_ItemStatus_UVP04,
  VBUP.UVP05 AS CustomerReserves5_ItemStatus_UVP05,
  VBUP.PKSTA AS PackingStatusOfItem_PKSTA,
  VBUP.KOQUA AS ConfirmationStatusOfPickingputaway_KOQUA,
  VBUP.CMPPI AS StatusOfCreditCheckAgainstFinancialDocument_CMPPI,
  VBUP.CMPPJ AS StatusOfCreditCheckAgainstExportCreditInsurance_CMPPJ,
  VBUP.UVPIK AS IncompleteStatusOfItemForPickingputaway_UVPIK,
  VBUP.UVPAK AS IncompleteStatusOfItemForPackaging_UVPAK,
  VBUP.UVWAK AS IncompleteStatusOfItemRegardingGoodsIssue_UVWAK,
  VBUP.DCSTA AS DelayStatus_DCSTA,
  VBUP.RRSTA AS RevenueDeterminationStatus_RRSTA,
  VBUP.VLSTP AS DecentralizedWhseProcessing_VLSTP,
  VBUP.FSSTA AS BillingBlockStatusForItems_FSSTA,
  VBUP.LSSTA AS DeliveryBlockStatusForItem_LSSTA,
  VBUP.PDSTA AS PodStatusOnItemLevel_PDSTA,
  VBUP.MANEK AS ManualCompletionOfContract_MANEK,
  VBUP.HDALL AS InboundDeliveryItemNotYetComplete__onHold___HDALL,
  VBUP.LTSPS AS Indicator_StockableTypeSwitchedIntoStandardProduct_LTSPS,
  VBUP.FSH_AR_STAT_ITM AS AllocationStatusOfASalesDocumentItem_FSH_AR_STAT_ITM,
  VBUP.MILL_VS_VSSTA AS StatusOfSalesOrderItem_MILL_VS_VSSTA,
  ( CASE VBUP.FKSTA
    WHEN 'A' THEN 'Not Yet Processed'
    WHEN 'B' THEN 'Partially Processed'
    WHEN 'C' THEN 'Completely Processed'
    END ) AS Billing_Status,
  ( CASE VBUP.LFSTA
      WHEN 'A' THEN 'Not Yet Processed'
      WHEN 'B' THEN 'Partially Processed'
      WHEN 'C' THEN 'Completely Processed'
    END ) AS Delivery_Status
FROM `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.vbup` AS VBUP
