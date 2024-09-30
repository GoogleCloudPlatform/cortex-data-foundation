WITH
  AGG_PRCD_ELEMENTS AS (
    SELECT
      Prcd_Elements.client AS Mandt,
      Prcd_Elements.knumv AS Knumv,
      Prcd_Elements.kposn AS Kposn,
      SUM(IF(Prcd_Elements.koaid = 'C' AND Prcd_Elements.kinak IS NULL, Prcd_Elements.kwert, NULL)) AS Rebate
    FROM
      `{{ project_id_src }}.{{ dataset_cdc_processed_s4 }}.prcd_elements` AS Prcd_Elements
    GROUP BY Mandt, Knumv, Kposn
  )

SELECT
  VBRK.MANDT AS Client_MANDT,
  VBRK.FKART AS BillingType_FKART,
  VBRK.FKTYP AS BillingCategory_FKTYP,
  VBRK.VKORG AS SalesOrganization_VKORG,
  VBRK.VTWEG AS DistributionChannel_VTWEG,
  VBRK.SPART AS Division_SPART,
  VBRK.VBTYP AS SDDocumentCategory_VBTYP,
  VBRK.BZIRK AS SalesDistrict_BZIRK,
  VBRK.PLTYP AS PriceListType_PLTYP,
  VBRK.FKSTO AS BillingDocumentIsCancelled_FKSTO,
  -- VBRK.BLART AS DocumentType_BLART,
  -- VBRK.GBSTK AS OverallProcessingStatus_GBSTK,
  -- VBRK.BUCHK AS PostingStatusOfBillingDocument_BUCHK,
  -- VBRK.RELIK AS InvoiceListStatusOfBillingDocument_RELIK,
  -- VBRK.UVALS AS IncompletionStatus_UVALS,
  -- VBRK.UVPRS AS PricingIncompletionStatus_UVPRS,
  -- VBRK.FKSAK AS BillingStatus_FKSAK,
  -- VBRK.ABSTK AS RejectionStatus_ABSTK,
  VBRK.KUNRG AS Payer_KUNRG,
  VBRK.INCO1 AS IncotermsPart1_INCO1,
  VBRK.INCO2 AS IncotermsPart2_INCO2,
  VBRK.LAND1 AS DestinationCountry_LAND1,
  VBRK.REGIO AS Region_REGIO,
  VBRK.COUNC AS CountryCode_COUNC,
  VBRK.CITYC AS CityCode_CITYC,
  VBRK.TAXK1 AS TaxClassification1ForCustomer_TAXK1,
  VBRK.TAXK2 AS TaxClassification2ForCustomer_TAXK2,
  VBRK.TAXK3 AS TaxClassification3ForCustomer_TAXK3,
  VBRK.TAXK4 AS TaxClassification4ForCustomer_TAXK4,
  VBRK.TAXK5 AS TaxClassification5ForCustomer_TAXK5,
  VBRK.LANDTX AS TaxDepartureCountry_LANDTX,
  VBRK.STCEG_H AS OriginOfSalesTaxIDNumber_STCEG_H,
  VBRK.STCEG_L AS CountryOfSalesTaxIDNumber_STCEG_L,
  VBRK.XBLNR AS ReferenceDocumentNumber_XBLNR,
  VBRK.KONDA AS CustomerPriceGroup_KONDA,
  VBRK.RFBSK AS StatusForTransferToAccounting_RFBSK,
  VBRK.FKDAT AS BillingDate_FKDAT,
  VBRK.GJAHR AS FiscalYear_GJAHR,
  VBRK.POPER AS PostingPeriod_POPER,
  VBRK.ERDAT AS RecordCreationDate_ERDAT,
  VBRK.AEDAT AS LastChangeDate_AEDAT,
  VBRK.KDGRP AS CustomerGroup_KDGRP,
  VBRK.ZLSCH AS PaymentMethod_ZLSCH,
  VBRK.BUKRS AS CompanyCode_BUKRS,
  VBRK.MSCHL AS DunningKey_MSCHL,
  VBRK.MANSP AS DunningBlock_MANSP,
  VBRK.KUNAG AS SoldToParty_KUNAG,
  VBRK.FKART_AB AS AccrualBillingType_FKART,
  VBRK.BELNR AS AccountingDocumentNumber_BELNR,
  VBRK.VSBED AS ShippingConditions_VSBED,
  VBRK.WAERK AS SdDocumentCurrency_WAERK,
  VBRP.GSBER AS BusinessArea_GSBER,
  VBRP.VBELN AS BillingDocument_VBELN,
  VBRP.POSNR AS BillingItem_POSNR,
  VBRP.PSTYV AS SalesDocumentItemCategory_PSTYV,
  VBRP.POSAR AS ItemType_POSAR,
  VBRP.KOSTL AS CostCenter_KOSTL,
  VBRP.VKGRP AS SalesGroup_VKGRP,
  VBRP.VKBUR AS SalesOffice_VKBUR,
  VBRP.PRCTR AS ProfitCenter_PRCTR,
  VBRP.KOKRS AS ControllingArea_KOKRS,
  VBRP.VGTYP AS DocumentCategoryOfPrecedingSDDocument_VGTYP,
  VBRP.MATNR AS MaterialNumber_MATNR,
  VBRP.PMATN AS PricingReferenceMaterial_PMATN,
  VBRP.CHARG AS BatchNumber_CHARG,
  VBRP.MATKL AS MaterialGroup_MATKL,
  VBRP.PRODH AS ProductHierarchy_PRODH,
  VBRP.WERKS AS Plant_WERKS,
  VBRP.KONDM AS MaterialPriceGroup_KONDM,
  VBRP.LGORT AS StorageLocation_LGORT,
  VBRP.EAN11 AS InternationalArticleNumber_EAN11,
  VBRP.MVGR1 AS MaterialGroup1_MVGR1,
  VBRP.MVGR2 AS MaterialGroup2_MVGR2,
  VBRP.MVGR3 AS MaterialGroup3_MVGR3,
  VBRP.MVGR4 AS MaterialGroup4_MVGR4,
  VBRP.MVGR5 AS MaterialGroup5_MVGR5,
  VBRP.SERNR AS BOMExplosionNumber_SERNR,
  VBRP.KVGR1 AS CustomerGroup1_KVGR1,
  VBRP.KVGR2 AS CustomerGroup2_KVGR2,
  VBRP.KVGR3 AS CustomerGroup3_KVGR3,
  VBRP.KVGR4 AS CustomerGroup4_KVGR4,
  VBRP.KVGR5 AS CustomerGroup5_KVGR5,
  VBRP.TXJCD AS TaxJurisdiction_TXJCD,
  VBRP.VSTEL AS ShippingPointReceivingPoint_VSTEL,
  VBRP.VGBEL AS DocumentNumberOfTheReferenceDocument_VGBEL,
  VBRP.VGPOS AS ItemNumberOfTheReferenceItem_VGPOS,
  VBRP.AUBEL AS SalesDocument_AUBEL,
  VBRP.AUPOS AS SalesDocumentItem_AUPOS,
  VBRP.FKIMG AS ActualBilledQuantity_FKIMG,
  VBRP.VOLUM AS Volume_VOLUM,
  VBRP.BRGEW AS GrossWeight_BRGEW,
  VBRP.NTGEW AS NetWeight_NTGEW,
  AGG_PRCD_ELEMENTS.KNUMV AS NumberOfTheDocumentCondition_KNUMV,
  AGG_PRCD_ELEMENTS.KPOSN AS ConditionItemNumber_KPOSN,
  --##CORTEX-CUSTOMER Consider adding other dimensions from the calendar_date_dim table as per your requirement
  CalendarDateDimension_FKDAT.CalYear AS YearOfBillingDate_FKDAT,
  CalendarDateDimension_FKDAT.CalMonth AS MonthOfBillingDate_FKDAT,
  CalendarDateDimension_FKDAT.CalWeek AS WeekOfBillingDate_FKDAT,
  CalendarDateDimension_FKDAT.CalQuarter AS DayOfBillingDate_FKDAT,
  -- ##CORTEX-CUSTOMER If you prefer to use currency conversion, uncomment below
  -- currency_conversion.UKURS AS ExchangeRate_UKURS,
  -- currency_conversion.conv_date AS Conversion_date,
  -- currency_conversion.TCURR AS TargetCurrency_TCURR,
  -- VBRP.NETWR * currency_conversion.UKURS AS NetValueInTargetCurrency_NETWR,
  -- VBRK.MWSBK * currency_conversion.UKURS AS TaxAmountInTargetCurrency_MWSBK,
  -- AGG_PRCD_ELEMENTS.rebate * currency_conversion.UKURS AS RebateInTargetCurrency,
  COALESCE(VBRP.NETWR * currency_decimal.CURRFIX, VBRP.NETWR) AS NetValue_NETWR,
  COALESCE(VBRK.MWSBK * currency_decimal.CURRFIX, VBRK.MWSBK) AS TaxAmount_MWSBK,
  COALESCE(VBRP.MWSBP * currency_decimal.CURRFIX, VBRP.MWSBP) AS TaxAmountPos_MWSBP,
  COALESCE(AGG_PRCD_ELEMENTS.rebate * currency_decimal.CURRFIX, AGG_PRCD_ELEMENTS.rebate) AS Rebate,
  COUNT(VBRK.VBELN) OVER (PARTITION BY CalendarDateDimension_FKDAT.CalYear) AS YearOrderCount,
  COUNT(VBRK.VBELN) OVER (PARTITION BY CalendarDateDimension_FKDAT.CalYear, CalendarDateDimension_FKDAT.CalMonth) AS MonthOrderCount,
  COUNT(VBRK.VBELN) OVER (PARTITION BY CalendarDateDimension_FKDAT.CalYear, CalendarDateDimension_FKDAT.CalMonth, CalendarDateDimension_FKDAT.CalWeek) AS WeekOrderCount
FROM `{{ project_id_src }}.{{ dataset_cdc_processed_s4 }}.vbrk` AS VBRK
INNER JOIN `{{ project_id_src }}.{{ dataset_cdc_processed_s4 }}.vbrp` AS VBRP
  ON
    VBRK.VBELN = VBRP.VBELN
    AND VBRK.MANDT = VBRP.MANDT
INNER JOIN AGG_PRCD_ELEMENTS
  ON
    AGG_PRCD_ELEMENTS.MANDT = vbrk.MANDT
    AND CAST(AGG_PRCD_ELEMENTS.Knumv AS STRING) = vbrk.knumv
    AND CAST(AGG_PRCD_ELEMENTS.Kposn AS STRING) = vbrp.posnr
LEFT JOIN `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.currency_decimal` AS currency_decimal
  ON vbrk.WAERK = currency_decimal.CURRKEY
-- ##CORTEX-CUSTOMER If you prefer to use currency conversion, uncomment below
-- LEFT JOIN `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.currency_conversion` AS currency_conversion
--   ON VBRK.MANDT = currency_conversion.MANDT
--     AND VBRK.WAERK = currency_conversion.FCURR
--     AND VBRK.FKDAT = currency_conversion.conv_date
--     AND currency_conversion.TCURR IN UNNEST({{ sap_currencies }})
-- ##CORTEX-CUSTOMER Modify the exchange rate type based on your requirement
--     AND currency_conversion.KURST = 'M'
LEFT JOIN `{{ project_id_src }}.{{ k9_datasets_processing }}.calendar_date_dim` AS CalendarDateDimension_FKDAT
  ON CalendarDateDimension_FKDAT.Date = VBRK.FKDAT
