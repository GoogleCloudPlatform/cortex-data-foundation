SELECT
  PO.VendorAccountNumber_LIFNR,
  ekbe.MANDT AS Client_MANDT,
  ekbe.EBELN AS PurchasingDocumentNumber_EBELN,
  ekbe.EBELP AS ItemNumberOfPurchasingDocument_EBELP,
  ekbe.ZEKKN AS SequentialNumberOfAccountAssignment_ZEKKN,
  ekbe.VGABE AS TransactioneventType_VGABE,
  ekbe.GJAHR AS MaterialDocumentYear_GJAHR,
  ekbe.BELNR AS NumberOfMaterialDocument_BELNR,
  ekbe.BUZEI AS ItemInMaterialDocument_BUZEI,
  ekbe.BEWTP AS PurchaseOrderHistoryCategory_BEWTP,
  ekbe.BWART AS MovementType__inventoryManagement___BWART,
  ekbe.BUDAT AS PostingDateInTheDocument_BUDAT,
  ekbe.MENGE AS Quantity_MENGE,
  ekbe.BPMNG AS QuantityInPurchaseOrderPriceUnit_BPMNG,
  ekbe.WAERS AS CurrencyKey_WAERS,
  ekbe.WESBS AS GoodsReceiptBlockedStockInOrderUnit_WESBS,
  ekbe.BPWES AS QuantityInGrBlockedStockInOrderPriceUnit_BPWES,
  ekbe.SHKZG AS DebitcreditIndicator_SHKZG,
  ekbe.BWTAR AS ValuationType_BWTAR,
  ekbe.ELIKZ AS deliveryCompleted_ELIKZ,
  ekbe.XBLNR AS ReferenceDocumentNumber_XBLNR,
  ekbe.LFGJA AS FiscalYearOfAReferenceDocument_LFGJA,
  ekbe.LFBNR AS DocumentNoOfAReferenceDocument_LFBNR,
  ekbe.LFPOS AS ItemOfAReferenceDocument_LFPOS,
  ekbe.GRUND AS ReasonForMovement_GRUND,
  ekbe.CPUDT AS DayOnWhichAccountingDocumentWasEntered_CPUDT,
  ekbe.CPUTM AS TimeOfEntry_CPUTM,
  ekbe.EVERE AS ComplianceWithShippingInstructions_EVERE,
  ekbe.REFWR AS InvoiceValueInForeignCurrency_REFWR,
  ekbe.MATNR AS MaterialNumber_MATNR,
  ekbe.WERKS AS Plant_WERKS,
  ekbe.XWSBR AS ReversalOfGrAllowedForGrBasedIvDespiteInvoice_XWSBR,
  ekbe.ETENS AS SequentialNumberOfVendorConfirmation_ETENS,
  ekbe.KNUMV AS NumberOfTheDocumentCondition_KNUMV,
  ekbe.MWSKZ AS TaxOnSalespurchasesCode_MWSKZ,
  ekbe.LSMNG AS QuantityInUnitOfMeasureFromDeliveryNote_LSMNG,
  ekbe.LSMEH AS UnitOfMeasureFromDeliveryNote_LSMEH,
  ekbe.EMATN AS MaterialNumber_EMATN,
  ekbe.HSWAE AS LocalCurrencyKey_HSWAE,
  ekbe.BAMNG AS Quantity_BAMNG,
  ekbe.CHARG AS BatchNumber_CHARG,
  ekbe.BLDAT AS DocumentDateInDocument_BLDAT,
  ekbe.XWOFF AS CalculationOfValOpen_XWOFF,
  ekbe.XUNPL AS UnplannedAccountAssignmentFromInvoiceVerification_XUNPL,
  ekbe.ERNAM AS NameOfPersonWhoCreatedTheObject_ERNAM,
  ekbe.SRVPOS AS ServiceNumber_SRVPOS,
  ekbe.PACKNO AS PackageNumberOfService_PACKNO,
  ekbe.INTROW AS LineNumberOfService_INTROW,
  ekbe.BEKKN AS NumberOfPoAccountAssignment_BEKKN,
  ekbe.LEMIN AS ReturnsIndicator_LEMIN,
  ekbe.AREWB AS ClearingValueOnGrirAccountInPoCurrency_AREWB,
  ekbe.REWRB AS InvoiceAmountInPoCurrency_REWRB,
  ekbe.SAPRL AS SapRelease_SAPRL,
  ekbe.MENGE_POP AS Quantity_MENGE_POP,
  ekbe.BPMNG_POP AS QuantityInPurchaseOrderPriceUnit_BPMNG_POP,
  ekbe.DMBTR_POP AS AmountInLocalCurrency_DMBTR_POP,
  ekbe.WRBTR_POP AS AmountInDocumentCurrency_WRBTR_POP,
  ekbe.WESBB AS ValuatedGoodsReceiptBlockedStockInOrderUnit_WESBB,
  ekbe.BPWEB AS QuantityInValuatedGrBlockedStockInOrderPriceUnit_BPWEB,
  ekbe.WEORA AS AcceptanceAtOrigin_WEORA,
  ekbe.AREWR_POP AS GrirAccountClearingValueInLocalCurrency_AREWR_POP,
  ekbe.KUDIF AS ExchangeRateDifferenceAmount_KUDIF,
  ekbe.RETAMT_FC AS RetentionAmountInDocumentCurrency_RETAMT_FC,
  ekbe.RETAMT_LC AS RetentionAmountInCompanyCodeCurrency_RETAMT_LC,
  ekbe.RETAMTP_FC AS PostedRetentionAmountInDocumentCurrency_RETAMTP_FC,
  ekbe.RETAMTP_LC AS PostedSecurityRetentionAmountInCompanyCodeCurrency_RETAMTP_LC,
  ekbe.XMACC AS MultipleAccountAssignment_XMACC,
  ekbe.WKURS AS ExchangeRate_WKURS,
  ekbe.INV_ITEM_ORIGIN AS OriginOfAnInvoiceItem_INV_ITEM_ORIGIN,
  ekbe.VBELN_ST AS Delivery_VBELN_ST,
  ekbe.VBELP_ST AS DeliveryItem_VBELP_ST,
  ekbe.SGT_SCAT AS StockSegment_SGT_SCAT,
  ekbe.ET_UPD AS ProcedureForUpdatingTheScheduleLineQuantity_ET_UPD,
  ekbe.J_SC_DIE_COMP_F AS DepreciationCompletionFlag_J_SC_DIE_COMP_F,
  -- ekbe.FSH_SEASON_YEAR AS SeasonYear_FSH_SEASON_YEAR,
  -- ekbe.FSH_SEASON AS Season_FSH_SEASON,
  -- ekbe.FSH_COLLECTION AS FashionCollection_FSH_COLLECTION,
  -- ekbe.FSH_THEME AS FashionTheme_FSH_THEME,
  ekbe.WRF_CHARSTC1 AS CharacteristicValue1_WRF_CHARSTC1,
  ekbe.WRF_CHARSTC2 AS CharacteristicValue2_WRF_CHARSTC2,
  ekbe.WRF_CHARSTC3 AS CharacteristicValue3_WRF_CHARSTC3,
  --##CORTEX-CUSTOMER Consider adding other dimensions from the calendar_date_dim table as per your requirement
  CalendarDateDimension_BUDAT.CalYear AS YearOfPostingDateInTheDocument_BUDAT,
  CalendarDateDimension_BUDAT.CalMonth AS MonthOfPostingDateInTheDocument_BUDAT,
  CalendarDateDimension_BUDAT.CalWeek AS WeekOfPostingDateInTheDocument_BUDAT,
  CalendarDateDimension_BUDAT.CalQuarter AS QuarterOfPostingDateInTheDocument_BUDAT,
  CalendarDateDimension_BLDAT.CalYear AS YearOfDocumentDateInDocument_BLDAT,
  CalendarDateDimension_BLDAT.CalMonth AS MonthOfDocumentDateInDocument_BLDAT,
  CalendarDateDimension_BLDAT.CalWeek AS WeekOfDocumentDateInDocument_BLDAT,
  CalendarDateDimension_BLDAT.CalQuarter AS QuarterOfDocumentDateInDocument_BLDAT,
  --##CORTEX-CUSTOMER If you prefer to use currency conversion, uncomment below
  -- currency_conversion.UKURS AS ExchangeRate_UKURS,
  -- currency_conversion.TCURR AS TargetCurrency_TCURR,
  -- currency_conversion.conv_date AS Conversion_date,
  -- ekbe.DMBTR * currency_conversion.UKURS AS AmountInTargetCurrency_DMBTR,
  -- ekbe.WRBTR * currency_conversion.UKURS AS AmountInTargetCurrency_WRBTR,
  -- ekbe.AREWR * currency_conversion.UKURS AS GrirAccountClearingValueInTargetCurrency_AREWR,
  -- ekbe.REEWR * currency_conversion.UKURS AS InvoiceValueEntered__InTargetCurrency___REEWR,
  -- ekbe.AREWW * currency_conversion.UKURS AS ClearingValueOnGrirClearingAccount__InTargetCurrency___AREWW,
  COALESCE(ekbe.DMBTR * currency_decimal.CURRFIX, ekbe.DMBTR) AS AmountInLocalCurrency_DMBTR,
  COALESCE(ekbe.WRBTR * currency_decimal.CURRFIX, ekbe.WRBTR) AS AmountInDocumentCurrency_WRBTR,
  COALESCE(ekbe.AREWR * currency_decimal.CURRFIX, ekbe.AREWR) AS GrirAccountClearingValueInLocalCurrency_AREWR,
  COALESCE(ekbe.REEWR * currency_decimal.CURRFIX, ekbe.REEWR) AS InvoiceValueEntered__inLocalCurrency___REEWR,
  COALESCE(ekbe.AREWW * currency_decimal.CURRFIX, ekbe.AREWW) AS ClearingValueOnGrirClearingAccount__transacCurrency___AREWW

FROM
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.PurchaseDocuments` AS PO
INNER JOIN
  `{{ project_id_src }}.{{ dataset_cdc_processed_s4 }}.ekbe` AS ekbe
  ON PO.Client_MANDT = ekbe.MANDT
    AND PO.DocumentNumber_EBELN = ekbe.EBELN
    AND PO.Item_EBELP = ekbe.EBELP
LEFT JOIN `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.currency_decimal` AS currency_decimal
  ON PO.CurrencyKey_WAERS = currency_decimal.CURRKEY
--##CORTEX-CUSTOMER If you prefer to use currency conversion, uncomment below
-- LEFT JOIN
--   `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.currency_conversion` AS currency_conversion
--   ON PO.Client_MANDT = currency_conversion.MANDT
--     AND PO.CurrencyKey_WAERS = currency_conversion.FCURR
--     AND PO.ChangeDate_AEDAT = currency_conversion.conv_date
--     AND currency_conversion.TCURR IN UNNEST({{ sap_currencies }})
--##CORTEX-CUSTOMER Modify the exchange rate type based on your requirement
--     AND currency_conversion.KURST = 'M'
LEFT JOIN `{{ project_id_src }}.{{ k9_datasets_processing }}.calendar_date_dim` AS CalendarDateDimension_BUDAT
  ON CalendarDateDimension_BUDAT.Date = ekbe.BUDAT
LEFT JOIN `{{ project_id_src }}.{{ k9_datasets_processing }}.calendar_date_dim` AS CalendarDateDimension_BLDAT
  ON CalendarDateDimension_BLDAT.Date = ekbe.BLDAT
--vgabe='1' for Goods Receipt and vgabe='2' for Invoice Receipt
WHERE ekbe.VGABE IN ('1', '2')
