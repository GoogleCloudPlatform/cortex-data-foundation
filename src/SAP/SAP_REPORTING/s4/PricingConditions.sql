SELECT
  prcd_elements.CLIENT AS Client_MANDT,
  prcd_elements.KNUMV AS NumberOfTheDocumentCondition_KNUMV,
  prcd_elements.KPOSN AS ConditionItemNumber_KPOSN,
  prcd_elements.STUNR AS StepNumber_STUNR,
  prcd_elements.ZAEHK AS ConditionCounter_ZAEHK,
  prcd_elements.KAPPL AS Application_KAPPL,
  prcd_elements.KSCHL AS ConditionType_KSCHL,
  prcd_elements.KRECH AS CalculationTypeForCondition_KRECH,
  prcd_elements.KAWRT AS Checkbox_KAWRT,
  prcd_elements.KBETR AS ConditionAmountOrPercentage_KBETR,
  prcd_elements.KKURS AS ConditionExchangeRateForConversionToLocalCurrency_KKURS,
  prcd_elements.KPEIN AS ConditionPricingUnit_KPEIN,
  prcd_elements.KMEIN AS ConditionUnitInTheDocument_KMEIN,
  prcd_elements.KUMZA AS NumeratorForConvertingConditionUnitsToBaseUnits_KUMZA,
  prcd_elements.KUMNE AS DenominatorForConvertingConditionUnitsToBaseUnits_KUMNE,
  prcd_elements.KNTYP AS ConditionCategory_KNTYP,
  prcd_elements.KSTAT AS ConditionIsUsedForStatistics_KSTAT,
  prcd_elements.KNPRS AS ScaleType_KNPRS,
  prcd_elements.KRUEK AS ConditionIsRelevantForAccrual_KRUEK,
  prcd_elements.KRELI AS ConditionForInvoiceList_KRELI,
  prcd_elements.KHERK AS OriginOfTheCondition_KHERK,
  prcd_elements.KGRPE AS GroupCondition_KGRPE,
  prcd_elements.KOUPD AS ConditionUpdate_KOUPD,
  prcd_elements.KOLNR AS AccessSequenceAccessNumber_KOLNR,
  prcd_elements.KNUMH AS NumberOfConditionRecordFromBatchDetermination_KNUMH,
  prcd_elements.KOPOS AS SequentialNumberOfTheCondition_KOPOS,
  prcd_elements.KVSL1 AS AccountKey_KVSL1,
  prcd_elements.SAKN1 AS GLAccountNumber_SAKN1,
  prcd_elements.MWSK1 AS TaxOnSalesPurchasesCode_MWSK1,
  prcd_elements.KVSL2 AS AccountKeyAccrualsProvisions_KVSL2,
  prcd_elements.SAKN2 AS GLAccountNumber_SAKN2,
  prcd_elements.MWSK2 AS WithholdingTaxCode_MWSK2,
  prcd_elements.LIFNR AS AccountNumberOfVendorORCreditor_LIFNR,
  prcd_elements.KUNNR AS CustomerNumber_KUNNR,
  prcd_elements.KDIFF AS RoundingOffDifferenceOfTheCondition_KDIFF,
  prcd_elements.KSTEU AS ConditionControl_KSTEU,
  prcd_elements.KINAK AS ConditionIsInactive_KINAK,
  prcd_elements.KOAID AS ConditionClass_KOAID,
  prcd_elements.ZAEKO AS ConditionCounter_ZAEKO,
  prcd_elements.KMXAW AS IndicatorForMaximumConditionBaseValue_KMXAW,
  prcd_elements.KMXWR AS IndicatorForMaximumConditionAmount_KMXWR,
  prcd_elements.KFAKTOR AS FactorForConditionBaseValue_KFAKTOR,
  prcd_elements.KDUPL AS StructureCondition_KDUPL,
  prcd_elements.KFAKTOR1 AS FactorForConditionBasis_KFAKTOR1,
  prcd_elements.KZBZG AS ScaleBasisIndicator_KZBZG,
  prcd_elements.KSTBS AS ScaleBaseValueOfTheCondition_KSTBS,
  prcd_elements.KONMS AS ConditionScaleUnitOfMeasure_KONMS,
  prcd_elements.KONWS AS ScaleCurrency_KONWS,
  prcd_elements.KAWRT_K AS UpdatedInformationInRelatedUserDataField_KAWRT_K,
  prcd_elements.KWAEH AS ConditionCurrency_KWAEH,
  prcd_elements.KWERT_K AS ConditionValue_KWERT_K,
  prcd_elements.KFKIV AS ConditionForIntercompanyBilling_KFKIV,
  prcd_elements.KVARC AS VariantCond_KVARC,
  prcd_elements.KMPRS AS ConditionChangedManually_KMPRS,
  prcd_elements.PRSQU AS PriceSource_PRSQU,
  prcd_elements.VARCOND AS VariantCondition_VARCOND,
  prcd_elements.KTREL AS RelevanceForAccountAssignment_KTREL,
  prcd_elements.MDFLG AS IndicatorMatrixMaintenance_MDFLG,
  prcd_elements.TXJLV AS TaxJurisdictionCodeLevel_TXJLV,
  prcd_elements.KBFLAG AS BitEncryptedFlagsInPricing_KBFLAG,
  prcd_elements.CPF_GUID AS IdentifierOfCPFFormulaInDocument_CPF_GUID,
  prcd_elements.KAQTY AS AdjustedQuantity_KAQTY,
  --##CORTEX-CUSTOMER Consider adding other dimensions from the calendar_date_dim table as per your requirement
  CalendarDateDimension_KDATU.CalYear AS YearOfChangeDate_KDATU,
  CalendarDateDimension_KDATU.CalMonth AS MonthOfChangeDate_KDATU,
  CalendarDateDimension_KDATU.CalWeek AS WeekOfChangeDate_KDATU,
  CalendarDateDimension_KDATU.CalQuarter AS QuarterOfChangeDate_KDATU,
  PARSE_DATE('%Y%m%d', SUBSTRING(prcd_elements.KDATU, 1, 8)) AS Checkbox_KDATU,
  CAST(NULL AS NUMERIC) AS Level_STUFE,
  CAST(NULL AS NUMERIC) AS Path_WEGXX,
  CAST(NULL AS STRING) AS AccessSequenceAccessNumber_KOLNR3,
  --##CORTEX-CUSTOMER If you prefer to use currency conversion, uncomment below
  -- currency_conversion.UKURS AS ExchangeRate_UKURS,
  -- currency_conversion.conv_date AS Conversion_date,
  -- currency_conversion.TCURR AS TargetCurrency_TCURR,
  -- prcd_elements.KWERT * currency_conversion.UKURS AS ConditionValueInTargetCurrency_KWERT,
  prcd_elements.VAL_ZERO AS ProcessConditionsWithValueEqualToZero_VAL_ZERO,
  prcd_elements.IS_ACCT_DETN_RELEVANT AS StatisticalAndRelevantForAccountDetermination_IS_ACCT_DETN_RELEVANT,
  prcd_elements.TAX_COUNTRY AS TaxReportingCountry_TAX_COUNTRY,
  prcd_elements.WAERK AS SDDocumentCurrency_WAERK,
  prcd_elements.DATAAGING AS DataFilterValueForDataAging_DATAAGING,
  COALESCE(prcd_elements.WAERS, '') AS CurrencyKey_WAERS,
  -- decimal place of amounts for non-decimal-based currencies
  COALESCE(prcd_elements.KWERT * currency_decimal.CURRFIX, prcd_elements.KWERT) AS ConditionValue_KWERT
FROM
  `{{ project_id_src }}.{{ dataset_cdc_processed_s4 }}.prcd_elements` AS prcd_elements
LEFT JOIN
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.currency_decimal` AS currency_decimal
  ON
    COALESCE(prcd_elements.WAERS, '') = currency_decimal.CURRKEY
--##CORTEX-CUSTOMER If you prefer to use currency conversion, uncomment below
-- LEFT JOIN
--   `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.currency_conversion` AS currency_conversion
-- ON
--   prcd_elements.MANDT = currency_conversion.MANDT
--   AND COALESCE(prcd_elements.WAERS, '') = currency_conversion.FCURR
--   AND PARSE_DATE('%Y%m%d', SUBSTRING(prcd_elements.KDATU, 1, 8)) = currency_conversion.conv_date
--   AND currency_conversion.TCURR IN UNNEST({{ sap_currencies }})
--##CORTEX-CUSTOMER Modify the exchange rate type based on your requirement
--   AND currency_conversion.KURST = 'M'
LEFT JOIN
  `{{ project_id_src }}.{{ k9_datasets_processing }}.calendar_date_dim` AS CalendarDateDimension_KDATU
  ON
    CalendarDateDimension_KDATU.Date = PARSE_DATE('%Y%m%d', SUBSTRING(prcd_elements.KDATU, 1, 8))
