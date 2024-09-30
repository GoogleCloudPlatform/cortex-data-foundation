SELECT
  mbew.MANDT AS Client_MANDT,
  mbew.MATNR AS MaterialNumber_MATNR,
  mbew.BWTAR AS ValuationType_BWTAR,
  mbew.BWKEY AS ValuationArea_BWKEY,
  mbew.LVORM AS DeletionFlagForAllMaterialDataOfValuationType_LVORM,
  mbew.BWTTY AS ValuationCategory_BWTTY,
  mbew.LBKUM AS TotalValuatedStock_LBKUM,
  mbew.SALK3 AS ValueOfTotalValuatedStock_SALK3,
  mbew.VPRSV AS PriceControlIndicator_VPRSV,
  mbew.PEINH AS PriceUnit_PEINH,
  mbew.LAEPR AS DateOfTheLastPriceChange_LAEPR,
  mbew.ZKDAT AS DateAsOfWhichThePriceIsValid_ZKDAT,
  mbewh.LFGJA AS FiscalYearOfCurrentPeriod_LFGJA,
  mbewh.LFMON AS CurrentPeriod_LFMON,
  mbewh.BKLAS AS ValuationClass_BKLAS,
  -- mbew.SALKV AS ValueBasedOnMovingAveragePrice_SALKV,
  -- mbew.VMKUM AS TotalValuatedStockInPreviousPeriod_VMKUM,
  -- mbew.VMSAL AS ValueOfTotalValuatedStockInPreviousPeriod_VMSAL,
  -- mbew.VMVPR AS PriceControlIndicatorInPreviousPeriod_VMVPR,
  -- mbew.VMVER AS PeriodicUnitPriceInPreviousPeriod_VMVER,
  -- mbew.VMSTP AS StandardPriceInThePreviousPeriod_VMSTP,
  -- mbew.VMPEI AS PriceUnitOfPreviousPeriod_VMPEI,
  -- mbew.VMBKL AS ValuationClassInPreviousPeriod_VMBKL,
  -- mbew.VMSAV AS ValueBasedOnMovingAveragePricePreviousPeriod_VMSAV,
  -- mbew.VJKUM AS TotalValuatedStockInPreviousYear_VJKUM,
  -- mbew.VJSAL AS ValueOfTotalValuatedStockInPreviousYear_VJSAL,
  -- mbew.VJVPR AS PriceControlIndicatorInPreviousYear_VJVPR,
  -- mbew.VJVER AS PeriodicUnitPriceInPreviousYear_VJVER,
  -- mbew.VJSTP AS StandardPriceInPreviousYear_VJSTP,
  -- mbew.VJPEI AS PriceUnitOfPreviousYear_VJPEI,
  -- mbew.VJBKL AS ValuationClassInPreviousYear_VJBKL,
  -- mbew.VJSAV AS ValueBasedOnMovingAveragePricePreviousYear_VJSAV,
  -- mbew.BWPRS AS ValuationPriceBasedOnTaxLawLevel1_BWPRS,
  -- mbew.BWPRH AS ValuationPriceBasedOnCommercialLawLevel1_BWPRH,
  -- mbew.VJBWS AS ValuationPriceBasedOnTaxLawLevel3_VJBWS,
  -- mbew.VJBWH AS ValuationPriceBasedOnCommercialLawLevel3_VJBWH,
  -- mbew.VVJSL AS ValueOfTotalValuatedStockInYearBeforeLast_VVJSL,
  -- mbew.VVJLB AS TotalValuatedStockInYearBeforeLast_VVJLB,
  -- mbew.VVMLB AS TotalValuatedStockInPeriodBeforeLast_VVMLB,
  -- mbew.VVSAL AS ValueOfTotalValuatedStockInPeriodBeforeLast_VVSAL,
  -- mbew.ZPLPR AS FuturePlannedPrice_ZPLPR,
  -- mbew.ZPLP1 AS FuturePlannedPrice1_ZPLP1,
  -- mbew.ZPLP2 AS FuturePlannedPrice2,
  -- mbew.ZPLP3 AS FuturePlannedPrice3,
  -- mbew.ZPLD1 AS DateFromWhichFuturePlannedPrice1IsValid_ZPLD1,
  -- mbew.ZPLD2 AS DateFromWhichFuturePlannedPrice2IsValid_ZPLD2,
  -- mbew.ZPLD3 AS DateFromWhichFuturePlannedPrice3IsValid_ZPLD3,
  -- mbew.PPERZ AS PeriodForFutureStandardCostEstimateDeactivated_PPERZ,
  -- mbew.PPERL AS PeriodForCurrentStandardCostEstimateDeactivated_PPERL,
  -- mbew.PPERV AS PeriodForPreviousStandardCostEstimateDeactivated_PPERV,
  -- mbew.KALKZ AS IndicatorStandardCostEstimateForFuturePeriod_KALKZ,
  -- mbew.KALKL AS StandardCostEstimateForCurrentPeriod_KALKL,
  -- mbew.KALKV AS IndicatorStandardCostEstimateForPreviousPeriod_KALKV,
  -- mbew.KALSC AS OverheadKeyDeactivated_KALSC,
  -- mbew.XLIFO AS LIFOFIFORelevant_XLIFO,
  -- mbew.MYPOL AS PoolNumberForLIFOValuation_MYPOL,
  -- mbew.BWPH1 AS ValuationPriceBasedOnCommercialLawLevel2_BWPH1,
  -- mbew.BWPS1 AS ValuationPriceBasedOnTaxLawLevel2_BWPS1,
  -- mbew.ABWKZ AS LowestValueDevaluationIndicator_ABWKZ,
  -- mbew.PSTAT AS MaintenanceStatus_PSTAT,
  -- mbew.KALN1 AS CostEstimateNumberProductCosting_KALN1,
  -- mbew.KALNR AS CostEstimateNumberForCostEst_QtyStructure_KALNR,
  -- mbew.BWVA1 AS ValuationVariantForFutureStandardCostEstimate_BWVA1,
  -- mbew.BWVA2 AS ValuationVariantForCurrentStandardCostEstimate_BWVA2,
  -- mbew.BWVA3 AS ValuationVariantForPreviousStandardCostEstimate_BWVA3,
  -- mbew.VERS1 AS CostingVersionOfFutureStandardCostEstimate_VERS1,
  -- mbew.VERS2 AS CostingVersionOfCurrentStandardCostEstimate_VERS2,
  -- mbew.VERS3 AS CostingVersionOfPreviousStandardCostEstimate_VERS3,
  -- mbew.HRKFT AS OriginGroupAsSubdivisionOfCostElement_HRKFT,
  -- mbew.KOSGR AS CostingOverheadGroup_KOSGR,
  -- mbew.PPRDZ AS PeriodofFutureStandardCostEstimate_PPRDZ,
  -- mbew.PPRDL AS PeriodOfCurrentStandardCostEstimate_PPRDL,
  -- mbew.PPRDV AS PeriodOfPreviousStandardCostEstimate_PPRDV,
  -- mbew.PDATZ AS FiscalYearOfFutureStandardCostEstimate_PDATZ,
  -- mbew.PDATL AS FiscalYearOfCurrentStandardCostEstimate_PDATL,
  -- mbew.PDATV AS FiscalYearOfPreviousStandardCostEstimate_PDATV,
  -- mbew.EKALR AS MaterialIsCostedWithQuantityStructure_EKLAR,
  -- mbew.VPLPR AS PreviousPlannedPrice_VPLPR,
  -- mbew.MLMAA AS MaterialLedgerActivatedAtMaterialLevel_MLMAA,
  -- mbew.MLAST AS MaterialPriceDetermination_Control_MLAST,
  -- mbew.LPLPR AS CurrentPlannedPrice_LPLPR,
  -- mbew.VKSAL AS ValueOfTotalValuatedStockAtSalesPrice_VKSAL,
  -- mbew.HKMAT AS Material_RelatedOrigin_HKMAT,
  -- mbew.SPERW AS PhysicalInventoryBlockingIndicator_SPERW,
  -- mbew.KZIWL AS Phys_InventoryIndicatorForValue_OnlyMaterial_KZIWL,
  -- mbew.WLINL AS DateOfLastPostedCount_WLINL,
  -- mbew.ABCIW AS PhysicalInventoryIndicatorForCycleCounting_ABCIW,
  -- mbew.BWSPA AS ValuationMargin_BWSPA,
  -- mbew.LPLPX AS FixedPortionOfCurrentPlannedPrice_LPLPX,
  -- mbew.VPLPX AS FixedPortionOfPreviousPlannedPrice_VPLPX,
  -- mbew.FPLPX AS FixedPortionOfFuturePlannedPrice_FPLPX,
  -- mbew.LBWST AS Val_StratForCurrentPlanPriceSalesOrderProj_Stock_LBWST,
  -- mbew.VBWST AS ValuationStrategyForPreviousPlannedPriceSpecialStock_VBWST,
  -- mbew.FBWST AS ValuationStrategyForFuturePlannedPriceSpecialStock_FBWST,
  -- mbew.EKLAS AS ValuationClassForSalesOrderStock_EKLAS,
  -- mbew.QKLAS AS ValuationClassForProjectStock_QKLAS,
  -- mbew.MTUSE AS UsageOfTheMaterial_MTUSE,
  -- mbew.MTORG AS OriginOfTheMaterial_MTORG,
  -- mbew.OWNPR AS ProducedInHouse_OWNPR,
  -- mbew.XBEWM AS ValuationBasedOnTheBatchSpecificUnitOfMeasure_XBEWM,
  -- mbew.BWPEI AS PriceUnitForValuationPricesBasedOnTaxCommercialLaw_BWPEI,
  -- mbew.MBRUE AS MBEWH_Rec_AlreadyExistsForPerBeforeLastOfMBEWPer_MBRUE,
  -- mbew.OKLAS AS ValuationClassForSpecialStockAtTheVendor_OKLAS,
  -- mbew.OIPPINV AS PrepaidInventoryFlagForMaterialValuationTypeSegment_OIPPINV,
  t001.waers AS CurrencyKey_WAERS,
  --##CORTEX-CUSTOMER Consider adding other dimensions from the calendar_date_dim table as per your requirement
  CalendarDateDimension_LAEPR.CalYear AS YearOfDateOfTheLastPriceChange_LAEPR,
  CalendarDateDimension_LAEPR.CalMonth AS MonthOfDateOfTheLastPriceChange_LAEPR,
  CalendarDateDimension_LAEPR.CalWeek AS WeekOfDateOfTheLastPriceChange_LAEPR,
  CalendarDateDimension_LAEPR.CalQuarter AS QuarterOfDateOfTheLastPriceChange_LAEPR,
  CalendarDateDimension_ZKDAT.CalYear AS YearOfDateAsOfWhichThePriceIsValid_ZKDAT,
  CalendarDateDimension_ZKDAT.CalMonth AS MonthOfDateAsOfWhichThePriceIsValid_ZKDAT,
  CalendarDateDimension_ZKDAT.CalWeek AS WeekOfDateAsOfWhichThePriceIsValid_ZKDAT,
  CalendarDateDimension_ZKDAT.CalQuarter AS QuarterOfDateAsOfWhichThePriceIsValid_ZKDAT,
  --##CORTEX-CUSTOMER If you prefer to use currency conversion, uncomment below
  -- currency_conversion.UKURS AS ExchangeRate_UKURS,
  -- currency_conversion.TCURR AS TargetCurrency_TCURR,
  -- currency_conversion.conv_date AS Conversion_date,
  -- mbewh.STPRS * currency_conversion.UKURS AS StandardPriceInTargetCurrency_STPRS,
  -- mbewh.VERPR * currency_conversion.UKURS AS PeriodicUnitPriceInTargetCurrency_VERPR,
  -- mbew.STPRS * currency_conversion.UKURS AS StandardCostInTargetCurrency_STPRS,
  -- mbew.VERPR * currency_conversion.UKURS AS MovingAveragePriceInTargetCurrency_VERPR,
  -- mbew.STPRV * currency_conversion.UKURS AS PreviousPriceInTargetCurrency_STPRV,
  -- mbew.ZKPRS * currency_conversion.UKURS AS FuturePriceInTargetCurrency_ZKPRS,
  COALESCE(mbew.STPRS * currency_decimal.CURRFIX, mbew.STPRS) AS StandardCost_STPRS,
  COALESCE(mbew.VERPR * currency_decimal.CURRFIX, mbew.VERPR) AS MovingAveragePrice_VERPR,
  COALESCE(mbew.STPRV * currency_decimal.CURRFIX, mbew.STPRV) AS PreviousPrice_STPRV,
  COALESCE(mbew.ZKPRS * currency_decimal.CURRFIX, mbew.ZKPRS) AS FuturePrice_ZKPRS,
  COALESCE(mbewh.STPRS * currency_decimal.CURRFIX, mbewh.STPRS) AS StandardPrice_STPRS,
  COALESCE(mbewh.VERPR * currency_decimal.CURRFIX, mbewh.VERPR) AS PeriodicUnitPrice_VERPR
FROM
  `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.mbew` AS mbew
LEFT JOIN
  `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.mbewh` AS mbewh
  ON
    mbew.MANDT = mbewh.MANDT
    AND mbew.MATNR = mbewh.MATNR
    AND mbew.BWKEY = mbewh.BWKEY
    AND mbew.BWTAR = mbewh.BWTAR
LEFT JOIN
  `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.t001k` AS t001k
  ON mbew.MANDT = t001k.MANDT
    AND mbew.BWKEY = t001k.BWKEY
LEFT JOIN
  `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.t001` AS t001
  ON t001.MANDT = t001k.MANDT
    AND t001.BUKRS = t001k.BUKRS
LEFT JOIN `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.currency_decimal` AS currency_decimal
  ON t001.WAERS = currency_decimal.CURRKEY
--##CORTEX-CUSTOMER If you prefer to use currency conversion, uncomment below
-- LEFT JOIN `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.currency_conversion` AS currency_conversion
--   ON mbew.MANDT = currency_conversion.MANDT
--     AND t001.WAERS = currency_conversion.FCURR
--     AND mbew.LAEPR = currency_conversion.conv_date
--     AND currency_conversion.TCURR IN UNNEST({{ sap_currencies }})
--     --##CORTEX-CUSTOMER Modify the exchange rate type based on your requirement
--     AND currency_conversion.KURST = 'M'
LEFT JOIN `{{ project_id_src }}.{{ k9_datasets_processing }}.calendar_date_dim` AS CalendarDateDimension_LAEPR
  ON CalendarDateDimension_LAEPR.Date = mbew.LAEPR
LEFT JOIN `{{ project_id_src }}.{{ k9_datasets_processing }}.calendar_date_dim` AS CalendarDateDimension_ZKDAT
  ON CalendarDateDimension_ZKDAT.Date = mbew.ZKDAT
