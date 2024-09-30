WITH TCURX AS (
  -- Joining to this table is necesssary to fix the decimal place of 
  -- amounts for non-decimal-bASed currencies. SAP stores these amounts 
  -- offset by a factor  of 1/100 within the system (FYI this gets 
  -- corrected when a user observes these in the GUI) Currencies w/ 
  -- decimals are unimpacted.
  --
  -- Example of impacted currencies JPY, IDR, KRW, TWD 
  -- Example of non-impacted currencies USD, GBP, EUR
  -- Example 1,000 JPY will appear AS 10.00 JPY
  SELECT DISTINCT
    CURRKEY,
    CAST(POWER(10, 2 - COALESCE(CURRDEC, 0)) AS NUMERIC) AS CURRFIX
  FROM
    `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.tcurx`
),

AGGKONV AS (
  SELECT
    KONV.KNUMV,
    KONV.KPOSN,
    KONV.MANDT,
    SUM(
      IF(KONV.KRECH = 'C' AND KONV.KOAID = 'B' AND KONV.KINAK IS NULL, KONV.KWERT, NULL)
    ) AS ListPrice,
    SUM(
      IF(KONV.KRECH = 'C' AND KONV.KOAID = 'B' AND KONV.KSCHL = 'PB00', KONV.KWERT, NULL)
    ) AS AdjustedPrice,
    SUM(IF(KONV.KOAID = 'A' AND KONV.KINAK IS NULL, KONV.KWERT, NULL)) AS Discount,
    SUM(
      IF(KONV.KFKIV = 'X' AND KONV.KOAID = 'B' AND KONV.KINAK IS NULL, KONV.KWERT, NULL)
    ) AS InterCompanyPrice
  FROM
    `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.konv` AS KONV
  GROUP BY KNUMV, KPOSN, MANDT
),

AGGVBEP AS (
  SELECT
    MANDT,
    VBELN,
    POSNR,
    SUM(BMENG) AS ConfirmedOrderQuantity_BMENG
  FROM `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.vbep`
  GROUP BY MANDT, VBELN, POSNR
),

AGGVBPAITEM AS (
  SELECT
    VBPA.Mandt,
    VBPA.Vbeln,
    VBPA.Posnr,
    MAX(IF((VBPA.PARVW = 'AG'), VBPA.KUNNR, NULL)) AS SoldToPartyItem_KUNNR,
    MAX(IF((VBPA.PARVW = 'AG'), KNA1.Name1, NULL)) AS SoldToPartyItemName_KUNNR,
    MAX( IF((VBPA.PARVW = 'WE'), VBPA.KUNNR, NULL)) AS ShipToPartyItem_KUNNR,
    MAX( IF((VBPA.PARVW = 'WE'), KNA1.Name1, NULL)) AS ShipToPartyItemName_KUNNR,
    MAX( IF((VBPA.PARVW = 'RE'), VBPA.KUNNR, NULL)) AS BillToPartyItem_KUNNR,
    MAX( IF((VBPA.PARVW = 'RE'), KNA1.Name1, NULL)) AS BillToPartyItemName_KUNNR,
    MAX( IF((VBPA.PARVW = 'RG'), VBPA.KUNNR, NULL)) AS PayerItem_KUNNR,
    MAX( IF((VBPA.PARVW = 'RG'), KNA1.Name1, NULL)) AS PayerItemName_KUNNR
  FROM `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.vbpa` AS VBPA
  INNER JOIN `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.kna1` AS KNA1
    ON
      VBPA.Mandt = KNA1.Mandt
      AND VBPA.Kunnr = KNA1.Kunnr
  GROUP BY VBPA.Mandt, VBPA.Vbeln, VBPA.Posnr
),

AGGVBPAHEADER AS (
  SELECT
    VBPA.Mandt,
    VBPA.Vbeln,
    VBPA.Posnr,
    MAX(IF((VBPA.PARVW = 'AG'), VBPA.KUNNR, NULL)) AS SoldToPartyHeader_KUNNR,
    MAX(IF((VBPA.PARVW = 'AG'), KNA1.Name1, NULL)) AS SoldToPartyHeaderName_KUNNR,
    MAX( IF((VBPA.PARVW = 'WE'), VBPA.KUNNR, NULL)) AS ShipToPartyHeader_KUNNR,
    MAX( IF((VBPA.PARVW = 'WE'), KNA1.Name1, NULL)) AS ShipToPartyHeaderName_KUNNR,
    MAX( IF((VBPA.PARVW = 'RE'), VBPA.KUNNR, NULL)) AS BillToPartyHeader_KUNNR,
    MAX( IF((VBPA.PARVW = 'RE'), KNA1.Name1, NULL)) AS BillToPartyHeaderName_KUNNR,
    MAX( IF((VBPA.PARVW = 'RG'), VBPA.KUNNR, NULL)) AS PayerHeader_KUNNR,
    MAX( IF((VBPA.PARVW = 'RG'), KNA1.Name1, NULL)) AS PayerHeaderName_KUNNR
  FROM `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.vbpa` AS VBPA
  INNER JOIN `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.kna1` AS KNA1
    ON
      VBPA.Mandt = KNA1.Mandt
      AND VBPA.Kunnr = KNA1.Kunnr
  -- join `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.vbap` VBAP
  -- ON
  -- VBAP.mandt=VBPA.mandt
  -- AND VBAP.vbeln=VBPA.vbeln
  -- AND VBPA.posnr is NULL or VBPA.posnr='000000'
  GROUP BY VBPA.Mandt, VBPA.Vbeln, VBPA.Posnr
)

SELECT
  VBAK.MANDT AS Client_MANDT,
  VBAK.VBELN AS SalesDocument_VBELN,
  VBAP.POSNR AS Item_POSNR,
  VBAP.MATNR AS MaterialNumber_MATNR,
  VBAP.ERDAT AS CreationDate_ERDAT,
  VBAK.ERZET AS CreationTime_ERZET,
  VBAK.ERNAM AS CreatedBy_ERNAM,
  VBAK.ANGDT AS QuotationDateFrom_ANGDT,
  VBAK.BNDDT AS QuotationDateTo_BNDDT,
  VBAK.AUDAT AS DocumentDate_AUDAT,
  VBAK.VBTYP AS DocumentCategory_VBTYP,
  VBAK.TRVOG AS TransactionGroup_TRVOG,
  VBAK.AUART AS SalesDocumentType_AUART,
  VBAK.AUGRU AS Reason_AUGRU,
  VBAK.GWLDT AS WarrantyDate_GWLDT,
  VBAK.SUBMI AS CollectiveNumber_SUBMI,
  VBAK.LIFSK AS DeliveryBlock_LIFSK,
  VBAK.FAKSK AS BillingBlock_FAKSK,
  VBAK.WAERK AS CurrencyHdr_WAERK,
  VBAK.VKORG AS SalesOrganization_VKORG,
  VBAK.VTWEG AS DistributionChannel_VTWEG,
  VBAK.SPART AS DivisionHdr_SPART,
  VBAK.VKGRP AS SalesGroup_VKGRP,
  VBAK.VKBUR AS SalesOffice_VKBUR,
  VBAK.GSBER AS BusinessAreaHdr_GSBER,
  VBAK.GSKST AS CostCtrBusinessArea_GSKST,
  VBAK.GUEBG AS AgreementValidFrom_GUEBG,
  VBAK.GUEEN AS AgreementValidTo_GUEEN,
  VBAK.KNUMV AS ConditionNumber_KNUMV,
  VBAK.VDATU AS RequestedDeliveryDate_VDATU,
  VBAK.VPRGR AS ProposedDateType_VPRGR,
  VBAK.AUTLF AS CompleteDeliveryFlag_AUTLF,
  VBAK.VBKLA AS OriginalSystem_VBKLA,
  VBAK.VBKLT AS DocumentIndicator_VBKLT,
  VBAK.KALSM AS PricingProcedure_KALSM,
  VBAK.VSBED AS ShippingConditions_VSBED,
  VBAK.FKARA AS ProposedBillingType_FKARA,
  VBAK.AWAHR AS SalesProbability_AWAHR,
  VBAK.KTEXT AS SearchTermForProductProposal_KTEXT,
  VBAK.BSTNK AS CustomerPurchaseOrderNumber_BSTNK,
  VBAK.BSARK AS CustomerPurchaseOrderType_BSARK,
  VBAK.BSTDK AS CustomerPurchaseOrderDate_BSTDK,
  VBAK.BSTZD AS PurchaseOrderNumberSupplement_BSTZD,
  VBAK.IHREZ AS YourReference_IHREZ,
  VBAK.BNAME AS NameOfOrderer_BNAME,
  VBAK.TELF1 AS TelephoneNumber_TELF1,
  VBAK.MAHZA AS NumberOfContactsFromTheCustomer_MAHZA,
  VBAK.MAHDT AS LastCustomerContactDate_MAHDT,
  VBAK.KUNNR AS SoldToParty_KUNNR,
  VBAK.KOSTL AS CostCenterHdr_KOSTL,
  VBAK.STAFO AS UpdateGroupForStatistics_STAFO,
  VBAK.STWAE AS StatisticScurrency_STWAE,
  VBAK.AEDAT AS ChangedOn_AEDAT,
  VBAK.KVGR1 AS CustomerGroup1_KVGR1,
  VBAK.KVGR2 AS CustomerGroup2_KVGR2,
  VBAK.KVGR3 AS CustomerGroup3_KVGR3,
  VBAK.KVGR4 AS CustomerGroup4_KVGR4,
  VBAK.KVGR5 AS CustomerGroup5_KVGR5,
  VBAK.KNUMA AS Agreement_KNUMA,
  VBAK.KOKRS AS ControllingArea_KOKRS,
  VBAK.PS_PSP_PNR AS WBSElementHdr_PS_PSP_PNR,
  VBAK.KURST AS ExchangeRateType_KURST,
  VBAK.KKBER AS CreditControlArea_KKBER,
  VBAK.KNKLI AS CustomerCreditLimitRef_KNKLI,
  VBAK.GRUPP AS CustomerCreditGroup_GRUPP,
  VBAK.SBGRP AS CreditRepresentativeGroupForCreditManagement_SBGRP,
  VBAK.CTLPC AS RiskCategory_CTLPC,
  VBAK.CMWAE AS CurrencyKeyOfCreditControlArea_CMWAE,
  VBAK.CMFRE AS ReleASeDateOfTheDocumentDeterminedByCreditManagement_CMFRE,
  VBAK.CMNUP AS DateOfNextCreditCheckOfDocument_CMNUP,
  VBAK.CMNGV AS NextDate_CMNGV,
  VBAK.AMTBL AS ReleasedCreditValueOfTheDocument_AMTBL,
  VBAK.HITYP_PR AS HierarchyTypeForPricing_HITYP_PR,
  VBAK.ABRVW AS UsageIndicator_ABRVW,
  VBAK.ABDIS AS MRPForDeliveryScheduleTypes_ABDIS,
  VBAK.VGBEL AS DocumentNumberOfTheReferenceDocument_VGBEL,
  VBAK.OBJNR AS ObjectNumberAtHeaderLevel_OBJNR,
  VBAK.BUKRS_VF AS CompanyCodeToBeBilled_BUKRS_VF,
  VBAK.TAXK1 AS AlternativeTaxClassification_TAXK1,
  VBAK.TAXK2 AS TaxClassification2_TAXK2,
  VBAK.TAXK3 AS TaxClassification3_TAXK3,
  VBAK.TAXK4 AS TaxClassification4_TAXK4,
  VBAK.TAXK5 AS TaxClassification5_TAXK5,
  VBAK.TAXK6 AS TaxClassification6_TAXK6,
  VBAK.TAXK7 AS TaxClassification7_TAXK7,
  VBAK.TAXK8 AS TaxClassification8_TAXK8,
  VBAK.TAXK9 AS TaxClassification9_TAXK9,
  VBAK.XBLNR AS ReferenceDocumentNumber_XBLNR,
  VBAK.ZUONR AS AssignmentNumber_ZUONR,
  VBAK.VGTYP AS PreDocCategory_VGTYP,
  VBAK.AUFNR AS OrderNumberHdr_AUFNR,
  VBAK.QMNUM AS NotificationNo_QMNUM,
  VBAK.VBELN_GRP AS MasterContractNumber_VBELN_GRP,
  VBAK.STCEG_L AS TaxDestinationCountry_STCEG_L,
  VBAK.LANDTX AS TaxDepartureCountry_LANDTX,
  VBAK.HANDLE AS InternationalUniqueKey_HANDLE,
  VBAK.PROLI AS DangerousGoodsManagementProfile_PROLI,
  VBAK.CONT_DG AS DangerousGoodsFlag_CONT_DG,
  VBAK.UPD_TMSTMP AS UTCTimeStampL_UPD_TMSTMP,
  VBUK.ABSTK AS RejectionsStatus_ABSTK,
  VBUK.BESTK AS ConfirmationStatus_BESTK,
  VBUK.CMGST AS OverallStatusOfCreditChecks_CMGST,
  VBUK.DCSTK AS DelayStatus_DCSTK,
  VBUK.FSSTK AS BillingBlockStatus_FSSTK,
  VBUK.GBSTK AS OverallProcessingStatus_GBSTK,
  VBUK.LFGSK AS OverallDeliveryStatus_LFGSK,
  VBAP.MATWA AS MaterialEntered_MATWA,
  VBAP.PMATN AS PricingReferenceMaterial_PMATN,
  VBAP.CHARG AS BatchNumber_CHARG,
  VBAP.MATKL AS MaterialGroup_MATKL,
  VBAP.ARKTX AS ShortText_ARKTX,
  VBAP.PSTYV AS ItemCategory_PSTYV,
  VBAP.POSAR AS ItemType_POSAR,
  VBAP.LFREL AS RelevantForDelivery_LFREL,
  VBAP.FKREL AS RelevantForBilling_FKREL,
  VBAP.UEPOS AS BOMItemLevel_UEPOS,
  VBAP.GRPOS AS AlternativeForItem_GRPOS,
  VBAP.ABGRU AS RejectionReason_ABGRU,
  VBAP.PRODH AS ProductHierarchy_PRODH,
  VBAP.ZWERT AS TargetValue_ZWERT,
  VBAP.ZMENG AS TargetQuantityUoM_ZMENG,
  VBAP.ZIEME AS TargetQuantityUoM_ZIEME,
  VBAP.UMZIZ AS BaseTargetConversionFactor_UMZIZ,
  VBAP.UMZIN AS ConversionFactor_UMZIN,
  VBAP.MEINS AS BaseUnitOfMeasure_MEINS,
  VBAP.SMENG AS ScaleQuantity_SMENG,
  VBAP.ABLFZ AS RoundingQuantityForDelivery_ABLFZ,
  VBAP.ABDAT AS ReconciliationDate_ABDAT,
  VBAP.ABSFZ AS AllowedDeviation_ABSFZ,
  VBAP.POSEX AS ItemNumberOfTheUnderlyingPurchaseOrder_POSEX,
  VBAP.KDMAT AS CustomerMaterialNumber_KDMAT,
  VBAP.KBVER AS AllowedDeviationPercent_KBVER,
  VBAP.KEVER AS DaysByWhichTheQuantityCanBeShifted_KEVER,
  VBAP.VKGRU AS RepairProcessing_VKGRU,
  VBAP.VKAUS AS UsageIndicator_VKAUS,
  VBAP.GRKOR AS DeliveryGroup_GRKOR,
  VBAP.FMENG AS QuantityIsFixed_FMENG,
  VBAP.UEBTK AS UnlimitedOverDeliveryAllowed_UEBTK,
  VBAP.UEBTO AS OverDeliveryToleranceLimit_UEBTO,
  VBAP.UNTTO AS UnderDeliveryToleranceLimit_UNTTO,
  VBAP.FAKSP AS BillingBlockForItem_FAKSP,
  VBAP.ATPKZ AS ReplacementPart_ATPKZ,
  VBAP.RKFKF AS FormOfBillingForCO_RKFKF,
  VBAP.SPART AS Division_SPART,
  VBAP.GSBER AS BusinessArea_GSBER,
  VBAP.NETWR AS NetPrice_NETWR,
  VBAP.WAERK AS Currency_WAERK,
  VBAP.ANTLF AS MaximumPartialDeliveries_ANTLF,
  VBAP.KZTLF AS PartialDeliveryAtItemLevel_KZTLF,
  VBAP.CHSPL AS BatchSplitAllowed_CHSPL,
  VBAP.KWMENG AS CumulativeOrderQuantity_KWMENG,
  VBAP.LSMENG AS CumulativeTargetDeliveryQty_LSMENG,
  VBAP.KBMENG AS CumulativeConfirmedQuantity_KBMENG,
  VBAP.KLMENG AS CumulativeConfirmedQuantityInBASeUoM_KLMENG,
  VBAP.VRKME AS SalesUnit_VRKME,
  VBAP.UMVKZ AS NumeratorQty_UMVKZ,
  VBAP.UMVKN AS DenominatorQty_UMVKN,
  VBAP.BRGEW AS GrossWeightOfItem_BRGEW,
  VBAP.NTGEW AS NetWeightOfItem_NTGEW,
  VBAP.GEWEI AS WeightUnit_GEWEI,
  VBAP.VOLUM AS VolumeOfTheItem_VOLUM,
  VBAP.VOLEH AS VolumeUnit_VOLEH,
  VBAP.VBELV AS OriginatingDocument_VBELV,
  VBAP.POSNV AS OriginatingItem_POSNV,
  VBAP.VGBEL AS ReferenceDocument_VGBEL,
  VBAP.VGPOS AS ReferenceItem_VGPOS,
  VBAP.VOREF AS ReferenceIndicator_VOREF,
  VBAP.UPFLU AS UpdateIndicator_UPFLU,
  VBAP.ERLRE AS CompletionRuleForQuotation_ERLRE,
  VBAP.LPRIO AS DeliveryPriority_LPRIO,
  VBAP.WERKS AS Plant_WERKS,
  VBAP.LGORT AS StorageLocation_LGORT,
  VBAP.VSTEL AS ShippingReceivingPoint_VSTEL,
  VBAP.ROUTE AS Route_ROUTE,
  VBAP.STKEY AS BOMOrigin_STKEY,
  VBAP.STDAT AS BOMDate_STDAT,
  VBAP.STLNR AS BOM_STLNR,
  VBAP.AWAHR AS OrderProbabilityOfTheItem_AWAHR,
  VBAP.TAXM1 AS TaxClassification1_TAXM1,
  VBAP.TAXM2 AS TaxClassification2_TAXM2,
  VBAP.TAXM3 AS TaxClassification3_TAXM3,
  VBAP.TAXM4 AS TaxClassification4_TAXM4,
  VBAP.TAXM5 AS TaxClassification5_TAXM5,
  VBAP.TAXM6 AS TaxClassification6_TAXM6,
  VBAP.TAXM7 AS TaxClassification7_TAXM7,
  VBAP.TAXM8 AS TaxClassification8_TAXM8,
  VBAP.TAXM9 AS TaxClassification9_TAXM9,
  VBAP.VBEAF AS FixedShippingProcessingTimeInDays_VBEAF,
  VBAP.VBEAV AS VariableShippingProcessingTimeInDays_VBEAV,
  VBAP.VGREF AS PrecedingDocumentHasResultedFromReference_VGREF,
  VBAP.NETPR AS NetPrice_NETPR,
  VBAP.KPEIN AS ConditionPricingUnit_KPEIN,
  VBAP.KMEIN AS ConditionUnit_KMEIN,
  VBAP.SHKZG AS ReturnsItem_SHKZG,
  VBAP.SKTOF AS CashDiscountIndicator_SKTOF,
  VBAP.MTVFP AS CheckingGroupForAvailabilityCheck_MTVFP,
  VBAP.SUMBD AS SummingUpOfRequirements_SUMBD,
  VBAP.KONDM AS MaterialPricingGroup_KONDM,
  VBAP.KTGRM AS AccountAssignmentGroupForThisMaterial_KTGRM,
  VBAP.BONUS AS VolumeRebateGroup_BONUS,
  VBAP.PROVG AS CommissionGroup_PROVG,
  VBAP.PRSOK AS PricingIsOK_PRSOK,
  VBAP.BWTAR AS ValuationType_BWTAR,
  VBAP.BWTEX AS SeparateValuation_BWTEX,
  VBAP.XCHPF AS BatchManagementRequirementIndicator_XCHPF,
  VBAP.XCHAR AS BatchManagementIndicator_XCHAR,
  VBAP.LFMNG AS MinimumDeliveryQuantityInDeliveryNoteProcessing_LFMNG,
  VBAP.STAFO AS UpdateGroupForStatisticsUpdate_STAFO,
  VBAP.KZWI1 AS SubTotal1FromPricingProcedureForCondition_KZWI1,
  VBAP.KZWI2 AS SubTotal2FromPricingProcedureForCondition_KZWI2,
  VBAP.KZWI3 AS SubTotal3FromPricingProcedureForCondition_KZWI3,
  VBAP.KZWI4 AS SubTotal4FromPricingProcedureForCondition_KZWI4,
  VBAP.KZWI5 AS SubTotal5FromPricingProcedureForCondition_KZWI5,
  VBAP.KZWI6 AS SubTotal6FromPricingProcedureForCondition_KZWI6,
  VBAP.STCUR AS ExchangeRateForStatistics_STCUR,
  VBAP.AEDAT AS LastChangedOn_AEDAT,
  VBAP.EAN11 AS InternationalArticleNumber_EAN11,
  VBAP.FIXMG AS DeliveryDateAndQuantityFixed_FIXMG,
  VBAP.PRCTR AS ProfitCenter_PRCTR,
  VBAP.MVGR1 AS MaterialGroup1_MVGR1,
  VBAP.MVGR2 AS MaterialGroup2_MVGR2,
  VBAP.MVGR3 AS MaterialGroup3_MVGR3,
  VBAP.MVGR4 AS MaterialGroup4_MVGR4,
  VBAP.MVGR5 AS MaterialGroup5_MVGR5,
  VBAP.KMPMG AS ComponentQuantity_KMPMG,
  VBAP.SUGRD AS ReasonForMaterialSubstitution_SUGRD,
  VBAP.SOBKZ AS SpecialStockIndicator_SOBKZ,
  VBAP.VPZUO AS AllocationIndicator_VPZUO,
  VBAP.PAOBJNR AS ProfitabilitySegmentNumber_PAOBJNR,
  VBAP.PS_PSP_PNR AS WBSElement_PS_PSP_PNR,
  VBAP.AUFNR AS OrderNumber_AUFNR,
  VBAP.VPMAT AS PlanningMaterial_VPMAT,
  VBAP.VPWRK AS PlanningPlant_VPWRK,
  VBAP.PRBME AS BaseUnitOfMeasureForProductGroup_PRBME,
  VBAP.UMREF AS ConversionFactorQuantities_UMREF,
  VBAP.KNTTP AS AccountAssignmentCategory_KNTTP,
  VBAP.KZVBR AS ConsumptionPosting_KZVBR,
  VBAP.SERNR AS BOMExplosionNumber_SERNR,
  VBAP.OBJNR AS ObjectNumberAtItemLevel_OBJNR,
  VBAP.ABGRS AS ResultsAnalysisKey_ABGRS,
  VBAP.BEDAE AS RequirementsType_BEDAE,
  VBAP.CMPRE AS ItemCreditPrice_CMPRE,
  VBAP.CMTFG AS CreditBlock_CMTFG,
  VBAP.CMPNT AS RelevantForCredit_CMPNT,
  VBAP.CUOBJ AS Configuration_CUOBJ,
  VBAP.CUOBJ_CH AS InternalObjectNumberOfTheBatchClassification_CUOBJ_CH,
  VBAP.CEPOK AS StatusExpectedPrice_CEPOK,
  VBAP.KOUPD AS ConditionUpdate_KOUPD,
  VBAP.SERAIL AS SerialNumberProfile_SERAIL,
  VBAP.ANZSN AS NumberOfSerialNumbers_ANZSN,
  VBAP.NACHL AS CustomerHasNotPostedGoodsReceipt_NACHL,
  VBAP.MAGRV AS PackagingMaterials_MAGRV,
  VBAP.MPROK AS StatusManualPriceChange_MPROK,
  VBAP.VGTYP AS PrecedingDocCategory_VGTYP,
  VBAP.KALNR AS CostEstimateNumber_KALNR,
  VBAP.KLVAR AS CostingVariant_KLVAR,
  VBAP.SPOSN AS BOMItemNumber_SPOSN,
  VBAP.KOWRR AS StatisticalValues_KOWRR,
  VBAP.STADAT AS StatisticsDate_STADAT,
  VBAP.EXART AS BusinessTransactionTypeForForeignTrade_EXART,
  VBAP.PREFE AS ImportExportFlag_PREFE,
  VBAP.KNUMH AS NumberOfConditionRecord_KNUMH,
  VBAP.CLINT AS InternalClassNumber_CLINT,
  VBAP.STLTY AS BOMCategory_STLTY,
  VBAP.STLKN AS BOMItemNodeNumber_STLKN,
  VBAP.STPOZ AS InternalCounter_STPOZ,
  VBAP.STMAN AS InconsistentConfiguration_STMAN,
  VBAP.ZSCHL_K AS OverHeadKey_ZSCHL_K,
  VBAP.KALSM_K AS CostingSheet_KALSM_K,
  VBAP.KALVAR AS CostingVariant_KALVAR,
  VBAP.KOSCH AS ProductAllocation_KOSCH,
  VBAP.UPMAT AS PricingReferenceMaterial_UPMAT,
  VBAP.UKONM AS MaterialPricingGroup_UKONM,
  VBAP.MFRGR AS MaterialFreightGroup_MFRGR,
  VBAP.PLAVO AS PlanningReleaseRegulation_PLAVO,
  VBAP.KANNR AS KANBAN_KANNR,
  VBAP.CMPRE_FLT AS ItemCreditPrice_CMPRE_FLT,
  VBAP.ABFOR AS FormOfPaymentGuarantee_ABFOR,
  VBAP.ABGES AS GuaranteedFactor_ABGES,
  VBAP.WKTNR AS ValueContractNo_WKTNR,
  VBAP.WKTPS AS ValueContractItem_WKTPS,
  VBAP.SKOPF AS AssortmentModule_SKOPF,
  VBAP.KZBWS AS ValuationofSpecialStock_KZBWS,
  VBAP.WGRU1 AS MaterialGroupHierarchy1_WGRU1,
  VBAP.WGRU2 AS MaterialGroupHierarchy2_WGRU2,
  VBAP.KNUMA_PI AS Promotion_KNUMA_PI,
  VBAP.KNUMA_AG AS SalesDeal_KNUMA_AG,
  VBAP.KZFME AS LeadingUoM_KZFME,
  VBAP.LSTANR AS FreeGoodsDeliveryControl_LSTANR,
  VBAP.TECHS AS ParameterVariant_TECHS,
  VBAP.BERID AS MRPArea_BERID,
  VBAP.PCTRF AS ProfitCenterForBilling_PCTRF,
  VBAP.STOCKLOC AS ManagingLocation_STOCKLOC,
  VBAP.SLOCTYPE AS TypeOfFirstInventory_SLOCTYPE,
  VBAP.MSR_RET_REASON AS ReturnReason_MSR_RET_REASON,
  VBAP.MSR_REFUND_CODE AS ReturnsRefundCode_MSR_REFUND_CODE,
  VBAP.MSR_APPROV_BLOCK AS ApprovalBlock_MSR_APPROV_BLOCK,
  VBAP.NRAB_KNUMH AS ConditionRecordNumber_NRAB_KNUMH,
  VBAP.TRMRISK_RELEVANT AS RiskRelevancyInSales_TRMRISK_RELEVANT,
  VBAP.HANDOVERLOC AS LocationForAPhysicalHandOverOfGoods_HANDOVERLOC,
  VBAP.HANDOVERDATE AS HandOverDateAtTheHandOverLocation_HANDOVERDATE,
  VBAP.HANDOVERTIME AS HandOverTimeAtTheHandOverLocation_HANDOVERTIME,
  VBAP.TC_AUT_DET AS TaxCodeAutomaticallyDetermined_TC_AUT_DET,
  VBAP.MANUAL_TC_REASON AS ManualTaxCodeReASon_MANUAL_TC_REASON,
  VBAP.FISCAL_INCENTIVE AS TaxIncentiveType_FISCAL_INCENTIVE,
  VBAP.FISCAL_INCENTIVE_ID AS IncentiveID_FISCAL_INCENTIVE_ID,
  VBAP.SPCSTO AS NotAFiscalSpecialCaseForCFOPDetermination_SPCSTO,
  VBAP.KOSTL AS CostCenter_KOSTL,
  VBAP.FONDS AS Fund_FONDS,
  VBAP.FISTL AS FundsCenter_FISTL,
  VBAP.FKBER AS FunctionalArea_FKBER,
  AGGVBPAITEM.SoldToPartyItem_KUNNR,
  AGGVBPAITEM.SoldToPartyItemName_KUNNR,
  AGGVBPAITEM.ShipToPartyItem_KUNNR,
  AGGVBPAITEM.ShipToPartyItemName_KUNNR,
  AGGVBPAITEM.BillToPartyItem_KUNNR,
  AGGVBPAITEM.BillToPartyItemName_KUNNR,
  AGGVBPAITEM.PayerItem_KUNNR,
  AGGVBPAITEM.PayerItemName_KUNNR,
  AGGVBPAHEADER.SoldToPartyHeader_KUNNR,
  AGGVBPAHEADER.SoldToPartyHeaderName_KUNNR,
  AGGVBPAHEADER.ShipToPartyHeader_KUNNR,
  AGGVBPAHEADER.ShipToPartyHeaderName_KUNNR,
  AGGVBPAHEADER.BillToPartyHeader_KUNNR,
  AGGVBPAHEADER.BillToPartyHeaderName_KUNNR,
  AGGVBPAHEADER.PayerHeader_KUNNR,
  AGGVBPAHEADER.PayerHeaderName_KUNNR,
  AGGKONV.KNUMV,
  AGGKONV.KPOSN,
  AGGKONV.ListPrice,
  AGGKONV.AdjustedPrice,
  AGGKONV.InterCompanyPrice,
  AGGKONV.Discount,
  AGGVBEP.ConfirmedOrderQuantity_BMENG,
  -- Sales Order Value at item level 
  COALESCE(
    VBAK.NETWR * TCURX_VBAK.CURRFIX, VBAK.NETWR
  ) AS NetValueOfTheSalesOrderInDocumentCurrency_NETWR,
  COALESCE(VBAP.WAVWR * Tcurx_VBAP.CURRFIX, VBAP.WAVWR) AS CostInDocumentCurrency_WAVWR,
  COALESCE(VBAP.MWSBP * Tcurx_VBAP.CURRFIX, VBAP.MWSBP) AS TaxAmountInDocumentCurrency_MWSBP,
  EXTRACT(YEAR FROM VBAK.ERDAT) AS YearOfSalesOrderCreationDate_ERDAT,
  EXTRACT(MONTH FROM VBAK.ERDAT) AS MonthOfSalesOrderCreationDate_ERDAT,
  EXTRACT(WEEK FROM VBAK.ERDAT) AS WeekOfSalesOrderCreationDate_ERDAT,
  EXTRACT(DAY FROM VBAK.ERDAT) AS DayOfSalesOrderCreationDate_ERDAT,
  (VBAP.NETPR * VBAP.KWMENG) AS SalesOrderValueLineItem

FROM `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.vbak` AS VBAK
INNER JOIN `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.vbap` AS VBAP
  ON VBAK.VBELN = VBAP.VBELN
    AND VBAK.MANDT = VBAP.MANDT
LEFT OUTER JOIN AGGVBEP
  ON VBAP.MANDT = AGGVBEP.MANDT
    AND VBAP.VBELN = AGGVBEP.VBELN
    AND VBAP.POSNR = AGGVBEP.POSNR

LEFT OUTER JOIN `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.vbuk` AS VBUK
  ON VBAK.MANDT = VBUK.MANDT AND VBAK.VBELN = VBUK.VBELN
LEFT JOIN AGGVBPAITEM
  ON VBAP.MANDT = AGGVBPAITEM.MANDT
    AND VBAP.VBELN = AGGVBPAITEM.VBELN
    AND VBAP.POSNR = AGGVBPAITEM.POSNR
LEFT JOIN AGGVBPAHEADER
  ON VBAP.MANDT = AGGVBPAHEADER.MANDT
    AND VBAP.VBELN = AGGVBPAHEADER.VBELN
    AND (AGGVBPAHEADER.POSNR IS NULL OR AGGVBPAHEADER.POSNR = '000000')
LEFT OUTER JOIN AGGKONV
  ON LPAD(CAST(AGGKONV.KNUMV AS STRING), 10, '0') = VBAK.KNUMV
    AND LPAD(CAST(AGGKONV.KPOSN AS STRING), 6, '0') = VBAP.POSNR
    AND CAST(AGGKONV.MANDT AS INT64) = CAST(VBAP.MANDT AS INT64)
LEFT JOIN TCURX AS Tcurx_VBAK
  ON VBAK.WAERK = Tcurx_VBAK.CURRKEY
LEFT JOIN TCURX AS Tcurx_VBAP
  ON VBAP.WAERK = Tcurx_VBAP.CURRKEY
