SELECT
  konv.MANDT AS Client_MANDT,
  konv.KNUMV AS NumberOfTheDocumentCondition_KNUMV,
  konv.KPOSN AS ConditionItemNumber_KPOSN,
  konv.STUNR AS StepNumber_STUNR,
  konv.ZAEHK AS ConditionCounter_ZAEHK,
  konv.KAPPL AS Application_KAPPL,
  konv.KSCHL AS ConditionType_KSCHL,
  konv.KRECH AS CalculationTypeForCondition_KRECH,
  konv.KAWRT AS Checkbox_KAWRT,
  konv.KBETR AS ConditionAmountOrPercentage_KBETR,
  konv.KKURS AS ConditionExchangeRateForConversionToLocalCurrency_KKURS,
  konv.KPEIN AS ConditionPricingUnit_KPEIN,
  konv.KMEIN AS ConditionUnitInTheDocument_KMEIN,
  konv.KUMZA AS NumeratorForConvertingConditionUnitsToBaseUnits_KUMZA,
  konv.KUMNE AS DenominatorForConvertingConditionUnitsToBaseUnits_KUMNE,
  konv.KNTYP AS ConditionCategory_KNTYP,
  konv.KSTAT AS ConditionIsUsedForStatistics_KSTAT,
  konv.KNPRS AS ScaleType_KNPRS,
  konv.KRUEK AS ConditionIsRelevantForAccrual_KRUEK,
  konv.KRELI AS ConditionForInvoiceList_KRELI,
  konv.KHERK AS OriginOfTheCondition_KHERK,
  konv.KGRPE AS GroupCondition_KGRPE,
  konv.KOUPD AS ConditionUpdate_KOUPD,
  konv.KOLNR AS AccessSequenceAccessNumber_KOLNR,
  konv.KNUMH AS NumberOfConditionRecordFromBatchDetermination_KNUMH,
  konv.KOPOS AS SequentialNumberOfTheCondition_KOPOS,
  konv.KVSL1 AS AccountKey_KVSL1,
  konv.SAKN1 AS GLAccountNumber_SAKN1,
  konv.MWSK1 AS TaxOnSalesPurchasesCode_MWSK1,
  konv.KVSL2 AS AccountKeyAccrualsProvisions_KVSL2,
  konv.SAKN2 AS GLAccountNumber_SAKN2,
  konv.MWSK2 AS WithholdingTaxCode_MWSK2,
  konv.LIFNR AS AccountNumberOfVendorORCreditor_LIFNR,
  konv.KUNNR AS CustomerNumber_KUNNR,
  konv.KDIFF AS RoundingOffDifferenceOfTheCondition_KDIFF,
  konv.KSTEU AS ConditionControl_KSTEU,
  konv.KINAK AS ConditionIsInactive_KINAK,
  konv.KOAID AS ConditionClass_KOAID,
  konv.ZAEKO AS ConditionCounter_ZAEKO,
  konv.KMXAW AS IndicatorForMaximumConditionBaseValue_KMXAW,
  konv.KMXWR AS IndicatorForMaximumConditionAmount_KMXWR,
  konv.KFAKTOR AS FactorForConditionBaseValue_KFAKTOR,
  konv.KDUPL AS StructureCondition_KDUPL,
  KONV.KFAKTOR1 AS FactorForConditionBasis_KFAKTOR1,
  konv.KZBZG AS ScaleBasisIndicator_KZBZG,
  konv.KSTBS AS ScaleBaseValueOfTheCondition_KSTBS,
  konv.KONMS AS ConditionScaleUnitOfMeasure_KONMS,
  konv.KONWS AS ScaleCurrency_KONWS,
  konv.KAWRT_K AS UpdatedInformationInRelatedUserDataField_KAWRT_K,
  konv.KWAEH AS ConditionCurrency_KWAEH,
  konv.KWERT_K AS ConditionValue_KWERT_K,
  konv.KFKIV AS ConditionForInterCompanyBilling_KFKIV,
  konv.KVARC AS VariantCond_KVARC,
  konv.KMPRS AS ConditionChangedManually_KMPRS,
  konv.PRSQU AS PriceSource_PRSQU,
  konv.VARCOND AS VariantCondition_VARCOND,
  konv.KTREL AS RelevanceForAccountAssignment_KTREL,
  konv.MDFLG AS IndicatorMatrixMaintenance_MDFLG,
  konv.TXJLV AS TaxJurisdictionCodeLevel_TXJLV,
  konv.KBFLAG AS BitEncryptedFlagsInPricing_KBFLAG,
  konv.CPF_GUID AS IdentifierOfCPFFormulaInDocument_CPF_GUID,
  konv.KAQTY AS AdjustedQuantity_KAQTY,
  --##CORTEX-CUSTOMER Consider adding other dimensions from the calendar_date_dim table as per your requirement
  CalendarDateDimension_KDATU.CalYear AS YearOfChangeDate_KDATU,
  CalendarDateDimension_KDATU.CalMonth AS MonthOfChangeDate_KDATU,
  CalendarDateDimension_KDATU.CalWeek AS WeekOfChangeDate_KDATU,
  CalendarDateDimension_KDATU.CalQuarter AS QuarterOfChangeDate_KDATU,
  konv.KDATU AS Checkbox_KDATU,
  konv.STUFE AS Level_STUFE,
  konv.WEGXX AS Path_WEGXX,
  konv.KOLNR3 AS AccessSequenceAccessNumber_KOLNR3,
  --##CORTEX-CUSTOMER If you prefer to use currency conversion, uncomment below
  -- currency_conversion.UKURS AS ExchangeRate_UKURS,
  -- currency_conversion.conv_date AS Conversion_date,
  -- currency_conversion.TCURR AS TargetCurrency_TCURR,
  -- konv.KWERT * currency_conversion.UKURS AS ConditionValueInTargetCurrency_KWERT,
  CAST(NULL AS STRING) AS ProcessConditionsWithValueEqualToZero_VAL_ZERO,
  CAST(NULL AS STRING) AS StatisticalAndRelevantForAccountDetermination_IS_ACCT_DETN_RELEVANT,
  CAST(NULL AS STRING) AS TaxReportingCountry_TAX_COUNTRY,
  CAST(NULL AS STRING) AS SDDocumentCurrency_WAERK,
  CAST(NULL AS DATE) AS DataFilterValueForDataAging_DATAAGING,
  COALESCE(konv.WAERS, '') AS CurrencyKey_WAERS,
  -- decimal place of amounts for non-decimal-based currencies
  COALESCE(konv.KWERT * currency_decimal.CURRFIX, konv.KWERT) AS ConditionValue_KWERT
FROM
  `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.konv` AS konv
LEFT JOIN
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.currency_decimal` AS currency_decimal
  ON
    COALESCE(konv.WAERS, '') = currency_decimal.CURRKEY
--##CORTEX-CUSTOMER If you prefer to use currency conversion, uncomment below
-- LEFT JOIN
--   `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.currency_conversion` AS currency_conversion
-- ON
--   konv.MANDT = currency_conversion.MANDT
--   AND COALESCE(konv.WAERS, '') = currency_conversion.FCURR
--   AND konv.KDATU = currency_conversion.conv_date
--   AND currency_conversion.TCURR IN UNNEST({{ sap_currencies }})
--##CORTEX-CUSTOMER Modify the exchange rate type based on your requirement
--   AND currency_conversion.KURST = 'M'
LEFT JOIN
  `{{ project_id_src }}.{{ k9_datasets_processing }}.calendar_date_dim` AS CalendarDateDimension_KDATU
  ON
    CalendarDateDimension_KDATU.Date = konv.KDATU
