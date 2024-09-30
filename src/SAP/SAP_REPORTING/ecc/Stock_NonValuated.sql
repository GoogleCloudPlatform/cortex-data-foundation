SELECT
  MARD.MANDT AS Client_MANDT,
  MARD.MATNR AS MaterialNumber_MATNR,
  mara.MaterialText_MAKTX, MARD.WERKS AS Plant_WERKS,
  t001w.Name_NAME1 AS Plant_Name, MARD.LGORT AS StorageLocation_LGORT,
  mara.BaseUnitOfMeasure_MEINS,
  mara.Language_SPRAS,
  mara.MaterialType_MTART,
  mara.MaterialGroup_MATKL,
  mara.ProductHierarchy_PRDHA,
  t001w.Region_County__REGIO AS Plant_Region,
  t001w.CountryKey_LAND1 AS Plant_Country,
  SUM(MARD.LABST) AS ValuatedUnrestrictedUseStock
# MARD.PSTAT AS MaintenanceStatus_PSTAT, MARD.LVORM AS FlagMaterialForDeletionAtStorageLocationLevel_LVORM,  MARD.LFGJA AS FiscalYearOfCurrentPeriod_LFGJA,  MARD.LFMON AS CurrentPeriod_postingPeriod_LFMON,
#MARD.SPERR AS PhysicalInventoryBlockingIndicator_SPERR,    MARD.UMLME AS StockInTransfer__fromOneStorageLocationToAnother___UMLME,
#MARD.INSME AS StockInQualityInspection_INSME,  MARD.EINME AS TotalStockOfAllRestrictedBatches_EINME,  MARD.SPEME AS BlockedStock_SPEME,  MARD.RETME AS BlockedStockReturns_RETME,
#MARD.VMLAB AS ValuatedUnrestrictedUseStockInPreviousPeriod_VMLAB,  MARD.VMUML AS StockInTransferInPreviousPeriod_VMUML,  MARD.VMINS AS StockInQualityInspectionInPreviousPeriod_VMINS,
#MARD.VMEIN AS RestrictedUseStockInPreviousPeriod_VMEIN,  MARD.VMSPE AS BlockedStockOfPreviousPeriod_VMSPE,  MARD.VMRET AS BlockedStockReturnsInPreviousPeriod_VMRET,
#MARD.KZILL AS PhysicalInventoryIndicatorForWhseStockInCurrentYear_KZILL,  MARD.KZILQ AS PhysInventoryInd_KZILQ,  MARD.KZILE AS PhysicalInventoryIndicatorForRestrictedUseStock_KZILE,
#MARD.KZILS AS PhysicalInventoryIndicatorForBlockedStock_KZILS,  MARD.KZVLL AS PhysicalInventoryIndicatorForStockInPreviousYear_KZVLL,  MARD.KZVLQ AS PhysInventoryIndInPrevPeriod_KZVLQ,
#MARD.KZVLE AS PhysicalInventoryIndForRestrictedUseStockPrevPd_KZVLE,  MARD.KZVLS AS PhysInventoryIndicatorForBlockedStockInPrevPeriod_KZVLS,  MARD.DISKZ AS StorageLocationMrpIndicator_DISKZ,
#MARD.LSOBS AS SpecialProcurementTypeAtStorageLocationLevel_LSOBS,  MARD.LMINB AS ReorderPointForStorageLocationMrp_LMINB,  MARD.LBSTF AS ReplenishmentQuantityForStorageLocationMrp_LBSTF,
#MARD.HERKL AS CountryOfOriginOfMaterial_nonPreferentialOrigin___HERKL,  MARD.EXPPG AS PreferenceIndicator__deactivated___EXPPG,  MARD.EXVER AS ExportIndicator__deactivated___EXVER,  MARD.LGPBE AS StorageBin_LGPBE,
#MARD.KLABS AS UnrestrictedUseConsignmentStock_KLABS,  MARD.KINSM AS ConsignmentStockInQualityInspection_KINSM,  MARD.KEINM AS RestrictedUseConsignmentStock_KEINM,  MARD.KSPEM AS BlockedConsignmentStock_KSPEM,
#MARD.DLINL AS DateOfLastPostedCount_DLINL,  MARD.PRCTL AS ProfitCenter_PRCTL,  MARD.ERSDA AS CreatedOn_ERSDA,  MARD.VKLAB AS StockValueOfAValueOnlyMaterialAtSalesPrice_VKLAB,
#MARD.VKUML AS SalesValueInStockTransfer__slocToSloc___VKUML,  MARD.LWMKB AS PickingAreaForLeanWm_LWMKB,  MARD.BSKRF AS InventoryCorrectionFactor_BSKRF,
#MARD.MDRUE AS MardhRecAlreadyExistsForPerBeforeLastOfMardPer_MDRUE,  MARD.MDJIN AS FiscalYearOfCurrentPhysicalInventoryIndicator_MDJIN,  MARD.FSH_SALLOC_QTY_S AS AllocatedStockQuantity_FSH_SALLOC_QTY_S,
FROM `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.mard` AS MARD
INNER JOIN `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.MaterialsMD` AS mara
  ON mara.Client_MANDT = MARD.MANDT
    AND mara.MaterialNumber_MATNR = MARD.MATNR
INNER JOIN `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.PlantsMD` AS t001w
  ON t001w.Client_MANDT = MARD.MANDT
    AND t001w.Plant_WERKS = MARD.WERKS
    AND t001w.Language_SPRAS = mara.Language_SPRAS
WHERE mara.Language_SPRAS IN UNNEST({{ sap_languages }})
GROUP BY
  MARD.MANDT, MARD.MATNR, mara.MaterialText_MAKTX, MARD.WERKS,
  t001w.Name_NAME1, MARD.LGORT, mara.BaseUnitOfMeasure_MEINS,
  mara.Language_SPRAS, mara.MaterialType_MTART,
  mara.MaterialGroup_MATKL, mara.ProductHierarchy_PRDHA,
  t001w.Region_County__REGIO, t001w.CountryKey_LAND1
