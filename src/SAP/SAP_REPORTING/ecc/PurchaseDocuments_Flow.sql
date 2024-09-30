(
  WITH
    TCURX AS (
      SELECT DISTINCT
        CURRKEY,
        CAST(POWER(10, 2 - COALESCE(CURRDEC, 0)) AS NUMERIC) AS CURRENCY_FIX
      FROM
        `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.tcurx`
    ),

    CONV AS (
      -- This table is used to convert rates from the transaction currency to USD.
      SELECT DISTINCT
        mandt,
        fcurr,
        tcurr,
        ukurs,
        PARSE_DATE('%Y%m%d', CAST(99999999 - CAST(gdatu AS INT64) AS STRING)) AS gdatu
      FROM `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.tcurr`
      WHERE
        mandt = '{{ mandt_ecc }}'
        AND kurst = 'M' -- Daily Corporate Rate
        AND tcurr IN UNNEST({{ sap_currencies }}) -- Convert to USD

      UNION ALL
      ## CORTEX-CUSTOMER replace USD below or add currencies as UNION clauses
      -- USD to USD rates do not exist in TCURR (or any other rates that are same-to-
      -- same such as EUR to EUR / JPY to JPY etc.
      SELECT
        '{{ mandt_ecc }}' AS mandt,
        'USD' AS fcurr,
        'USD' AS tcurr,
        1 AS ukurs,
        date_array AS gdatu
      FROM
        UNNEST(GENERATE_DATE_ARRAY('1990-01-01', '2060-12-31')) AS date_array
    ),

    EKKN AS (
      SELECT
        EKKN.* EXCEPT (netwr),
        COALESCE(SAFE_DIVIDE(EKKN.menge, EKPO.menge) * EKPO.brtwr, EKPO.brtwr) AS brtwr,
        CASE
          WHEN EKKN.netwr IS NOT NULL THEN EKKN.netwr
          WHEN EKKN.netwr IS NULL AND EKKN.menge IS NOT NULL THEN SAFE_DIVIDE(EKKN.menge, EKPO.menge) * EKPO.netwr
          WHEN EKKN.netwr IS NULL AND EKKN.menge IS NULL THEN EKPO.netwr
        END AS netwr,
        COALESCE(SAFE_DIVIDE(EKKN.menge, EKPO.menge) * EKPO.effwr, EKPO.effwr) AS effwr
      FROM `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.ekkn` AS EKKN
      INNER JOIN `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.ekpo` AS EKPO
        USING (mandt, ebelp, ebeln) --L032
    ),

    GoodsReceiptStaging AS (
      SELECT
        MANDT,
        EBELN,
        EBELP,
        ZEKKN,
        MAX(BUDAT) AS MAX_BUDAT,
        SUM(
          CASE
            WHEN SHKZG = 'S' THEN MENGE
            WHEN SHKZG = 'H' THEN MENGE * -1
            ELSE MENGE
          END
        ) AS MENGE,
        SUM(
          CASE
            WHEN SHKZG = 'S' THEN WRBTR
            WHEN SHKZG = 'H' THEN WRBTR * -1
            ELSE WRBTR
          END
        ) AS WRBTR
      FROM `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.ekbe`
      WHERE VGABE = '1' -- Goods Receipt
      GROUP BY
        1, 2, 3, 4
    ),

    GoodsReceiptStagingBELNR AS (
      SELECT
        EBELN,
        EBELP,
        ZEKKN,
        BELNR,
        BUDAT
      FROM `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.ekbe`
      WHERE VGABE = '1' -- Goods Receipt
      GROUP BY 1, 2, 3, 4, 5
    ),

    GoodsReceipt AS (
      SELECT
        GoodsReceiptStaging.MANDT,
        GoodsReceiptStaging.EBELN,
        GoodsReceiptStaging.EBELP,
        GoodsReceiptStaging.ZEKKN,
        GoodsReceiptStaging.MAX_BUDAT,
        GoodsReceiptStaging.MENGE,
        GoodsReceiptStaging.WRBTR,
        ANY_VALUE(GoodsReceiptStagingBELNR.BELNR) AS MAX_BELNR
      FROM GoodsReceiptStaging
      INNER JOIN GoodsReceiptStagingBELNR
        ON
          GoodsReceiptStaging.EBELN = GoodsReceiptStagingBELNR.EBELN
          AND GoodsReceiptStaging.EBELP = GoodsReceiptStagingBELNR.EBELP
          AND GoodsReceiptStaging.ZEKKN = GoodsReceiptStagingBELNR.ZEKKN
          AND GoodsReceiptStaging.MAX_BUDAT = GoodsReceiptStagingBELNR.BUDAT
      GROUP BY 1, 2, 3, 4, 5, 6, 7
    ),

    InvoiceReceiptStaging AS (
      SELECT
        MANDT,
        EBELN,
        EBELP,
        ZEKKN,
        MAX(BUDAT) AS MAX_BUDAT,
        SUM(
          CASE
            WHEN SHKZG = 'S' THEN MENGE
            WHEN SHKZG = 'H' THEN MENGE * -1
            ELSE MENGE
          END
        ) AS MENGE,
        SUM(
          CASE
            WHEN SHKZG = 'S' THEN refwr
            WHEN SHKZG = 'H' THEN refwr * -1
            ELSE refwr
          END
        ) AS refwr
      FROM
        `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.ekbe`
      WHERE
        VGABE = '2'  -- Invoice Receipt
      GROUP BY
        1, 2, 3, 4
    ),

    InvoiceReceiptStagingBELNR AS (
      SELECT
        EBELN,
        EBELP,
        ZEKKN,
        BELNR,
        BUDAT
      FROM `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.ekbe`
      WHERE VGABE = '2' -- Goods Receipt
      GROUP BY 1, 2, 3, 4, 5
    ),

    InvoiceReceipt AS (
      SELECT
        InvoiceReceiptStaging.MANDT,
        InvoiceReceiptStaging.EBELN,
        InvoiceReceiptStaging.EBELP,
        InvoiceReceiptStaging.ZEKKN,
        InvoiceReceiptStaging.MAX_BUDAT,
        InvoiceReceiptStaging.MENGE,
        InvoiceReceiptStaging.refwr,
        ANY_VALUE(InvoiceReceiptStagingBELNR.BELNR) AS MAX_BELNR
      FROM InvoiceReceiptStaging
      INNER JOIN InvoiceReceiptStagingBELNR
        ON
          InvoiceReceiptStaging.EBELN = InvoiceReceiptStagingBELNR.EBELN
          AND InvoiceReceiptStaging.EBELP = InvoiceReceiptStagingBELNR.EBELP
          AND InvoiceReceiptStaging.MAX_BUDAT = InvoiceReceiptStagingBELNR.BUDAT
      GROUP BY 1, 2, 3, 4, 5, 6, 7
    )

  SELECT
    -- PRIMARY KEY
    EKPO.MANDT AS Client_MANDT,
    EKPO.EBELN AS DocumentNumber_EBELN,
    EKPO.EBELP AS Item_EBELP,
    -- Material PO (Goods Receipt PO) will exist in EKKO, EKPO, but not in EKKN.
    -- SAP will default these transactions to ZEKKN = '00' in tables such as BSEG.
    -- We are simulating that rule here with the IFNULL statement.
    EKPO.LOEKZ AS DeletionFlag_LOEKZ,
    EKKO.BUKRS AS Company_BUKRS, EKKO.BSTYP AS DocumentCategory_BSTYP,
    EKKO.BSART AS DocumentType_BSART,
    EKKO.BSAKZ AS ControlFlag_BSAKZ, EKKO.LOEKZ AS DeletionFlagHdr_LOEKZ,
    EKKO.STATU AS Status_STATU,
    EKKO.AEDAT AS CreatedOn_AEDAT,
    EKKO.ERNAM AS CreatedBy_ERNAM,
    EKKO.PINCR AS ItemNumberInterval_PINCR,
    EKKO.LPONR AS LastItemNumber_LPONR,
    EKKO.LIFNR AS VendorAccountNumber_LIFNR,
    EKKO.SPRAS AS Language_SPRAS,
    EKKO.ZTERM AS TermsPaymentKey_ZTERM,
    --   CONCAT(LFA1.NAME1, ' ', LFA1.NAME2, ' ', LFA1.NAME3, ' ', LFA1.NAME4) AS VendorName_LIFNR,
    EKKO.ZBD1T AS DiscountDays1_ZBD1T,
    EKKO.ZBD2T AS DiscountDays2_ZBD2T,
    EKKO.ZBD3T AS DiscountDays3_ZBD3T,
    EKKO.ZBD1P AS CashDiscountPercentage1_ZBD1P,
    EKKO.ZBD2P AS CashDiscountPercentage2_ZBD2P,
    EKKO.EKORG AS PurchasingOrganization_EKORG,
    EKKO.EKGRP AS PurchasingGroup_EKGRP,
    EKKO.WAERS AS CurrencyKey_WAERS,
    EKKO.WKURS AS ExchangeRate_WKURS,
    EKKO.KUFIX AS FlagFixingExchangeRate_KUFIX,
    EKKO.BEDAT AS PurchasingDocumentDate_BEDAT,
    EKKO.KDATB AS StartValidityPeriod_KDATB,
    EKKO.KDATE AS EndValidityPeriod_KDATE,
    EKKO.BWBDT AS ClosingDateforApplications_BWBDT,
    EKKO.ANGDT AS Deadline_ANGDT,
    EKKO.BNDDT AS BindingPeriodforQuotation_BNDDT,
    EKKO.GWLDT AS WarrantyDate_GWLDT,
    EKKO.AUSNR AS Bidinvitationnumber_AUSNR,
    EKKO.ANGNR AS QuotationNumber_ANGNR,
    EKKO.IHRAN AS QuotationSubmissionDate_IHRAN,
    EKKO.IHREZ AS YourReference_IHREZ,
    EKKO.VERKF AS VendorSalesperson_VERKF,
    EKKO.TELF1 AS VendorTelephone_TELF1,
    EKKO.LLIEF AS SupplyingVendor_LLIEF,
    EKKO.KUNNR AS Customer_KUNNR,
    EKKO.KONNR AS PrincipalPurchaseAgreement_KONNR,
    EKKO.AUTLF AS CompleteDeliveryStipulated_AUTLF,
    EKKO.WEAKT AS GoodsReceiptMsgFlag_WEAKT,
    EKKO.RESWK AS SupplyTransportOrders_RESWK,
    EKKO.KTWRT AS AreaPerDistributionValue_KTWRT,
    EKKO.SUBMI AS CollectiveNumber_SUBMI,
    EKKO.KNUMV AS Numberthedocumentcondition_KNUMV,
    EKKO.KALSM AS Procedure_KALSM,
    EKKO.STAFO AS UpdateGroupStatistics_STAFO,
    EKKO.LIFRE AS DifferentInvoicingParty_LIFRE,
    EKKO.EXNUM AS ForeignTradeDocument_EXNUM,
    EKKO.UNSEZ AS OurReference_UNSEZ,
    EKKO.LOGSY AS LogicalSystem_LOGSY,
    EKKO.UPINC AS ItemNumberInterval_UPINC,
    EKKO.STAKO AS TimeDependentConditions_STAKO,
    EKKO.FRGGR AS Releasegroup_FRGGR,
    EKKO.FRGSX AS ReleaseStrategy_FRGSX,
    EKKO.FRGKE AS PurchasingDocumentRelease_FRGKE,
    EKKO.FRGZU AS ReleaseStatus_FRGZU,
    EKKO.FRGRL AS ReleaseIncomplete_FRGRL,
    EKKO.LANDS AS CountryforTaxReturn_LANDS,
    EKKO.LPHIS AS SchedulingAgreement_LPHIS,
    EKKO.ADRNR AS Address_ADRNR,
    EKKO.STCEG_L AS CountrySalesTaxIDNumber_STCEG_L,
    EKKO.STCEG AS VATRegistrationNumber_STCEG,
    EKKO.ABSGR AS ReasonforCancellation_ABSGR,
    EKKO.ADDNR AS AdditionalDocument_ADDNR,
    EKKO.KORNR AS CorrectionMiscProvisions_KORNR,
    EKKO.MEMORY AS IncompleteFlag_MEMORY,
    EKKO.PROCSTAT AS ProcessingState_PROCSTAT,
    EKKO.RLWRT AS ValueAtRelease_RLWRT,
    EKKO.REVNO AS VersionnumberinPurchasing_REVNO,
    EKKO.SCMPROC AS SCMProcess_SCMPROC,
    EKKO.REASON_CODE AS GoodsReceiptReason_REASON_CODE,
    EKKO.MEMORYTYPE AS CategoryIncompleteness_MEMORYTYPE,
    EKKO.RETTP AS RetentionFlag_RETTP,
    EKKO.MSR_ID AS ProcessIdentificationNumber_MSR_ID,
    EKKO.HIERARCHY_EXISTS AS PartaContractHierarchy_HIERARCHY_EXISTS,
    EKKO.THRESHOLD_EXISTS AS ExchangeThresholdValue_THRESHOLD_EXISTS,
    EKKO.LEGAL_CONTRACT AS LegalContractNumber_LEGAL_CONTRACT,
    EKKO.DESCRIPTION AS ContractName_DESCRIPTION,
    EKKO.RELEASE_DATE AS ReleaseDateContract_RELEASE_DATE,
    EKKO.HANDOVERLOC AS Physicalhandover_HANDOVERLOC,
    EKKO.FORCE_ID AS InternalKeyforForceElement_FORCE_ID,
    EKKO.FORCE_CNT AS InternalCounter_FORCE_CNT,
    EKKO.RELOC_ID AS RelocationID_RELOC_ID,
    EKKO.RELOC_SEQ_ID AS RelocationStepID_RELOC_SEQ_ID,
    EKKO.SOURCE_LOGSYS AS Logicalsystem_SOURCE_LOGSYS,
    EKKO.VZSKZ AS InterestcalculationFlag_VZSKZ,
    EKKO.POHF_TYPE AS SeasonalProcesingDocument_POHF_TYPE,
    EKKO.EQ_EINDT AS SameDeliveryDate_EQ_EINDT,
    EKKO.EQ_WERKS AS SameReceivingPlant_EQ_WERKS,
    EKKO.FIXPO AS FirmDealFlag_FIXPO,
    EKKO.EKGRP_ALLOW AS TakeAccountPurchGroup_EKGRP_ALLOW,
    EKKO.WERKS_ALLOW AS TakeAccountPlants_WERKS_ALLOW,
    EKKO.CONTRACT_ALLOW AS TakeAccountContracts_CONTRACT_ALLOW,
    EKKO.PSTYP_ALLOW AS TakeAccountItemCategories_PSTYP_ALLOW,
    EKKO.FIXPO_ALLOW AS TakeAccountFixedDate_FIXPO_ALLOW,
    EKKO.KEY_ID_ALLOW AS ConsiderBudget_KEY_ID_ALLOW,
    EKKO.AUREL_ALLOW AS TakeAccountAllocTableRelevance_AUREL_ALLOW,
    EKKO.DELPER_ALLOW AS TakeAccountDlvyPeriod_DELPER_ALLOW,
    EKKO.EINDT_ALLOW AS TakeAccountDeliveryDate_EINDT_ALLOW,
    EKKO.LTSNR_ALLOW AS IncludeVendorSubrange_LTSNR_ALLOW,
    EKKO.OTB_LEVEL AS OTBCheckLevel_OTB_LEVEL,
    EKKO.OTB_COND_TYPE AS OTBConditionType_OTB_COND_TYPE,
    EKKO.KEY_ID AS UniqueNumberBudget_KEY_ID,
    EKKO.OTB_VALUE AS RequiredBudget_OTB_VALUE,
    EKKO.OTB_CURR AS OTBCurrency_OTB_CURR,
    EKKO.OTB_RES_VALUE AS ReservedBudgetforOTB_OTB_RES_VALUE,
    EKKO.OTB_SPEC_VALUE AS SpecialReleaseBudget_OTB_SPEC_VALUE,
    EKKO.BUDG_TYPE AS BudgetType_BUDG_TYPE,
    EKKO.OTB_STATUS AS OTBCheckStatus_OTB_STATUS,
    EKKO.OTB_REASON AS ReasonFlagforOTBCheckStatus_OTB_REASON,
    EKKO.CHECK_TYPE AS TypeOTBCheck_CHECK_TYPE,
    EKKO.CON_OTB_REQ AS OTBRelevantContract_CON_OTB_REQ,
    EKKO.CON_PREBOOK_LEV AS OTBFlagLevelforContracts_CON_PREBOOK_LEV,
    EKKO.CON_DISTR_LEV AS DistributionUsingTargetValueorItemData_CON_DISTR_LEV,
    EKPO.STATU AS RFQtatus_STATU,
    EKPO.AEDAT AS ChangeDate_AEDAT,
    EKPO.TXZ01 AS ShortText_TXZ01,
    EKPO.MATNR AS MaterialNumber_MATNR,
    EKPO.EMATN AS MaterialNumber_EMATN,
    EKPO.BUKRS AS CompanyCode_BUKRS,
    EKPO.WERKS AS Plant_WERKS,
    EKPO.LGORT AS StorageLocation_LGORT,
    EKPO.BEDNR AS RequirementTrackingNumber_BEDNR,
    EKPO.MATKL AS MaterialGroup_MATKL,
    EKPO.INFNR AS NumberofPurchasingInfoRecord_INFNR,
    EKPO.IDNLF AS MaterialNumberVendor_IDNLF,
    EKPO.KTMNG AS TargetQuantity_KTMNG,
    EKPO.MENGE AS POQuantity_MENGE,
    EKPO.MEINS AS UoM_MEINS,
    EKPO.BPRME AS OrderPriceUnit_BPRME,
    EKPO.BPUMZ AS OrderUnitNumerator_BPUMZ,
    EKPO.BPUMN AS OrderUnitDenominator_BPUMN,
    EKPO.UMREZ AS NumeratorforConversionofOrderUnittoBaseUnit_UMREZ,
    EKPO.UMREN AS DenominatorforConversionofOrderUnittoBaseUnit_UMREN,
    EKPO.PEINH AS PriceUnit_PEINH,
    EKPO.AGDAT AS DeadlineforSubmissionofBid_AGDAT,
    EKPO.WEBAZ AS GoodsReceiptProcessingTimeinDays_WEBAZ,
    --   EKPO.NETPR AS NetPrice_NETPR,
    EKPO.MWSKZ AS Taxcode_MWSKZ,
    EKPO.BONUS AS SettlementGroup1_BONUS,
    EKPO.INSMK AS StockType_INSMK,
    --   EKPO.NETWR AS NetOrderValueinPOCurrency_NETWR,
    EKPO.SPINF AS UpdateInfoRecordFlag_SPINF,
    EKPO.PRSDR AS PricePrintout_PRSDR,
    --   EKPO.BRTWR AS GrossordervalueinPOcurrency_BRTWR,
    EKPO.SCHPR AS EstimatedPriceFlag_SCHPR,
    EKPO.MAHNZ AS NumberofReminders_MAHNZ,
    EKPO.MAHN1 AS NumberofDaysforFirstReminder_MAHN1,
    EKPO.MAHN2 AS NumberofDaysforSecondReminder_MAHN2,
    EKPO.MAHN3 AS NumberofDaysforThirdReminder_MAHN3,
    EKPO.UEBTO AS OverdeliveryToleranceLimit_UEBTO,
    EKPO.UEBTK AS UnlimitedOverdeliveryAllowed_UEBTK,
    EKPO.UNTTO AS UnderdeliveryToleranceLimit_UNTTO,
    EKPO.BWTAR AS ValuationType_BWTAR,
    EKPO.BWTTY AS ValuationCategory_BWTTY,
    EKPO.ABSKZ AS RejectionFlag_ABSKZ,
    EKPO.AGMEM AS InternalCommentonQuotation_AGMEM,
    EKPO.ELIKZ AS DeliveryCompletedFlag_ELIKZ,
    EKPO.EREKZ AS FinalInvoiceFlag_EREKZ,
    EKPO.PSTYP AS ItemCategoryinPurchasingDocument_PSTYP,
    EKPO.KNTTP AS AccountAssignmentCategory_KNTTP,
    EKPO.KZVBR AS ConsumptionPosting_KZVBR,
    EKPO.VRTKZ AS DistributionFlagformultipleaccountassignment_VRTKZ,
    EKPO.TWRKZ AS PartialInvoiceFlag_TWRKZ,
    EKPO.WEPOS AS GoodsReceiptFlag_WEPOS,
    EKPO.WEUNB AS GoodsReceiptNonValuated_WEUNB,
    EKPO.REPOS AS InvoiceReceiptFlag_REPOS,
    EKPO.WEBRE AS FlagGRBasedInvoiceVerification_WEBRE,
    EKPO.KZABS AS OrderAcknowledgmentRequirement_KZABS,
    EKPO.LABNR AS OrderAcknowledgmentNumber_LABNR,
    EKPO.KONNR AS NumberofPrincipalPurchaseAgreement_KONNR,
    EKPO.KTPNR AS ItemNumberofPrincipalPurchaseAgreement_KTPNR,
    EKPO.ABDAT AS ReconciliationDateforAgreedCumulativeQuantity_ABDAT,
    EKPO.ABFTZ AS AgreedCumulativeQuantity_ABFTZ,
    EKPO.ETFZ1 AS FirmZone_ETFZ1,
    EKPO.ETFZ2 AS TradeOffZone_ETFZ2,
    EKPO.KZSTU AS FirmTradeOffZones_KZSTU,
    EKPO.NOTKZ AS ExclusioninOutlineAgreementItemwithMaterialClass_NOTKZ,
    EKPO.LMEIN AS BaseUnitofMeasure_LMEIN,
    EKPO.EVERS AS ShippingInstructions_EVERS,
    EKPO.ZWERT AS TargetValueforOutlineAgreementinDocumentCurrency_ZWERT,
    EKPO.NAVNW AS Nondeductibleinputtax_NAVNW,
    EKPO.ABMNG AS Standardreleaseorderquantity_ABMNG,
    EKPO.PRDAT AS DateofPriceDetermination_PRDAT,
    EKPO.BSTYP AS PurchasingDocumentCategory_BSTYP,
    EKPO.XOBLR AS Itemaffectscommitments_XOBLR,
    EKPO.ADRNR AS Manualaddressnumberinpurchasingdocumentitem_ADRNR,
    EKPO.EKKOL AS ConditionGroupwithVendor_EKKOL,
    EKPO.SKTOF AS ItemDoesNotQualifyforCashDiscount_SKTOF,
    EKPO.STAFO AS Updategroupforstatisticsupdate_STAFO,
    EKPO.PLIFZ AS PlannedDeliveryTimeinDays_PLIFZ,
    EKPO.NTGEW AS NetWeight_NTGEW,
    EKPO.GEWEI AS UnitofWeight_GEWEI,
    EKPO.TXJCD AS TaxJurisdiction_TXJCD,
    EKPO.ETDRK AS FlagPrintrelevantSchedulelinesexist_ETDRK,
    EKPO.SOBKZ AS SpecialStockFlag_SOBKZ,
    --   EKPO.EFFWR AS Effectivevalueofitem_EFFWR,
    EKPO.ARSNR AS Settlementreservationnumber_ARSNR,
    EKPO.ARSPS AS Itemnumberofthesettlementreservation_ARSPS,
    EKPO.INSNC AS QualityinspectionFlagcannotbechanged_INSNC,
    EKPO.SSQSS AS ControlKeyforQualityManagementinProcurement_SSQSS,
    EKPO.ZGTYP AS CertificateType_ZGTYP,
    EKPO.EAN11 AS InternationalArticleNumber_EAN11,
    EKPO.BSTAE AS ConfirmationControlKey_BSTAE,
    EKPO.REVLV AS RevisionLevel_REVLV,
    EKPO.GEBER AS Fund_GEBER,
    EKPO.FISTL AS FundsCenter_FISTL,
    EKPO.FIPOS AS CommitmentItem_FIPOS,
    EKPO.KO_GSBER AS Businessareareportedtothepartner_KO_GSBER,
    EKPO.KO_PARGB AS assumedbusinessareaofthebusinesspartner_KO_PARGB,
    EKPO.KO_PRCTR AS ProfitCenter_KO_PRCTR,
    EKPO.KO_PPRCTR AS PartnerProfitCenter_KO_PPRCTR,
    EKPO.MEPRF AS PricingDateControl_MEPRF,
    EKPO.BRGEW AS GrossWeight_BRGEW,
    EKPO.VOLUM AS Volume_VOLUM,
    EKPO.VOLEH AS Volumeunit_VOLEH,
    EKPO.INCO1 AS Incoterms1_INCO1,
    EKPO.INCO2 AS Incoterms2_INCO2,
    EKPO.VORAB AS Advanceprocurement_VORAB,
    EKPO.KOLIF AS PriorVendor_KOLIF,
    EKPO.LTSNR AS VendorSubrange_LTSNR,
    EKPO.PACKNO AS Packagenumber_PACKNO,
    EKPO.FPLNR AS Invoicingplannumber_FPLNR,
    EKPO.GNETWR AS Currentlynotused_GNETWR,
    EKPO.STAPO AS Itemisstatistical_STAPO,
    EKPO.UEBPO AS HigherLevelIteminPurchasingDocuments_UEBPO,
    EKPO.LEWED AS LatestPossibleGoodsReceipt_LEWED,
    EKPO.EMLIF AS Vendortobesupplied_EMLIF,
    EKPO.LBLKZ AS Subcontractingvendor_LBLKZ,
    EKPO.SATNR AS CrossPlantConfigurableMaterial_SATNR,
    EKPO.ATTYP AS MaterialCategory_ATTYP,
    EKPO.VSART AS Shippingtype_VSART,
    EKPO.HANDOVERLOC AS Locationforaphysicalhandoverofgoods_HANDOVERLOC,
    EKPO.KANBA AS KanbanFlag_KANBA,
    EKPO.ADRN2 AS Numberofdeliveryaddress_ADRN2,
    EKPO.CUOBJ AS internalObjectNumber_CUOBJ,
    EKPO.XERSY AS EvaluatedReceiptSettlement_XERSY,
    EKPO.EILDT AS StartDateforGRBasedSettlement_EILDT,
    EKPO.DRDAT AS LastTransmission_DRDAT,
    EKPO.DRUHR AS Time_DRUHR,
    EKPO.DRUNR AS SequentialNumber_DRUNR,
    EKPO.AKTNR AS Promotion_AKTNR,
    EKPO.ABELN AS AllocationTableNumber_ABELN,
    EKPO.ABELP AS Itemnumberofallocationtable_ABELP,
    EKPO.ANZPU AS NumberofPoints_ANZPU,
    EKPO.PUNEI AS Pointsunit_PUNEI,
    EKPO.SAISO AS SeasonCategory_SAISO,
    EKPO.SAISJ AS SeasonYear_SAISJ,
    EKPO.EBON2 AS SettlementGroup2_EBON2,
    EKPO.EBON3 AS SettlementGroup3_EBON3,
    EKPO.EBONF AS ItemRelevanttoSubsequentSettlement_EBONF,
    EKPO.MLMAA AS Materialledgeractivatedatmateriallevel_MLMAA,
    EKPO.MHDRZ AS MinimumRemainingShelfLife_MHDRZ,
    EKPO.ANFNR AS RFQNumber_ANFNR,
    EKPO.ANFPS AS ItemNumberofRFQ_ANFPS,
    EKPO.KZKFG AS OriginofConfiguration_KZKFG,
    EKPO.USEQU AS Quotaarrangementusage_USEQU,
    EKPO.UMSOK AS SpecialStockFlagforPhysicalStockTransfer_UMSOK,
    EKPO.BANFN AS PurchaseRequisitionNumber_BANFN,
    EKPO.BNFPO AS ItemNumberofPurchaseRequisition_BNFPO,
    EKPO.MTART AS MaterialType_MTART,
    EKPO.UPTYP AS SubitemCategory_UPTYP,
    EKPO.UPVOR AS SubitemsExist_UPVOR,
    EKPO.KZWI1 AS Subtotal1frompricingprocedureforcondition_KZWI1,
    EKPO.KZWI2 AS Subtotal2frompricingprocedureforcondition_KZWI2,
    EKPO.KZWI3 AS Subtotal3frompricingprocedureforcondition_KZWI3,
    EKPO.KZWI4 AS Subtotal4frompricingprocedureforcondition_KZWI4,
    EKPO.KZWI5 AS Subtotal5frompricingprocedureforcondition_KZWI5,
    EKPO.KZWI6 AS Subtotal6frompricingprocedureforcondition_KZWI6,
    EKPO.SIKGR AS Processingkeyforsubitems_SIKGR,
    EKPO.MFZHI AS MaximumCumulativeMaterialGoAheadQuantity_MFZHI,
    EKPO.FFZHI AS MaximumCumulativeProductionGoAheadQuantity_FFZHI,
    EKPO.RETPO AS ReturnsItem_RETPO,
    EKPO.AUREL AS RelevanttoAllocationTable_AUREL,
    EKPO.BSGRU AS ReasonforOrdering_BSGRU,
    EKPO.LFRET AS DeliveryTypeforReturnstoVendors_LFRET,
    EKPO.MFRGR AS Materialfreightgroup_MFRGR,
    EKPO.NRFHG AS Materialqualifiesfordiscountinkind_NRFHG,
    EKPO.ABUEB AS ReleaseCreationProfile_ABUEB,
    EKPO.NLABD AS NextForecastDeliveryScheduleTransmission_NLABD,
    EKPO.NFABD AS NextJITDeliveryScheduleTransmission_NFABD,
    EKPO.KZBWS AS ValuationofSpecialStock_KZBWS,
    EKPO.FABKZ AS FlagItemRelevanttoJITDeliverySchedules_FABKZ,
    EKPO.J_1AINDXP AS InflationIndex_J_1AINDXP,
    EKPO.J_1AIDATEP AS InflationIndexDate_J_1AIDATEP,
    EKPO.MPROF AS ManufacturerPartProfile_MPROF,
    EKPO.EGLKZ AS OutwardDeliveryCompletedFlag_EGLKZ,
    EKPO.KZTLF AS StockTransfer_KZTLF,
    EKPO.KZFME AS Unitsofmeasureusage_KZFME,
    EKPO.RDPRF AS RoundingProfile_RDPRF,
    EKPO.TECHS AS StandardVariant_TECHS,
    EKPO.CHG_SRV AS Configurationchanged_CHG_SRV,
    EKPO.CHG_FPLNR AS Noinvoiceforthisitemalthoughnotfreeofcharge_CHG_FPLNR,
    EKPO.MFRPN AS ManufacturerPartNumber_MFRPN,
    EKPO.MFRNR AS NumberofaManufacturer_MFRNR,
    --   EKPO.BONBA AS Rebatebasis1_BONBA,
    EKPO.EMNFR AS Externalmanufacturercodenameornumber_EMNFR,
    EKPO.NOVET AS ItemblockedforSDdelivery_NOVET,
    EKPO.AFNAM AS NameofRequester_AFNAM,
    EKPO.TZONRC AS Timezoneofrecipientlocation_TZONRC,
    EKPO.IPRKZ AS PeriodFlagforShelfLifeExpirationDate_IPRKZ,
    EKPO.LEBRE AS FlagforServiceBasedInvoiceVerification_LEBRE,
    EKPO.BERID AS MRPArea_BERID,
    EKPO.XCONDITIONS AS Conditionsforitemalthoughnoinvoice_XCONDITIONS,
    EKPO.APOMS AS APOasPlanningSystem_APOMS,
    EKPO.CCOMP AS PostingLogicintheCaseofStockTransfers_CCOMP,
    EKPO.GRANT_NBR AS Grant_GRANT_NBR,
    EKPO.FKBER AS FunctionalArea_FKBER,
    EKPO.STATUS AS StatusofPurchasingDocumentItem_STATUS,
    EKPO.RESLO AS IssuingStorageLocationforStockTransportOrder_RESLO,
    EKPO.KBLNR AS DocumentNumberforEarmarkedFunds_KBLNR,
    EKPO.KBLPOS AS EarmarkedFundsDocumentItem_KBLPOS,
    EKPO.WEORA AS AcceptanceAtOrigin_WEORA,
    EKPO.SRV_BAS_COM AS ServiceBasedCommitment_SRV_BAS_COM,
    EKPO.PRIO_URG AS RequirementUrgency_PRIO_URG,
    EKPO.PRIO_REQ AS RequirementPriority_PRIO_REQ,
    EKPO.EMPST AS Receivingpoint_EMPST,
    EKPO.DIFF_INVOICE AS DifferentialInvoicing_DIFF_INVOICE,
    EKPO.TRMRISK_RELEVANT AS RiskRelevancyinPurchasing_TRMRISK_RELEVANT,
    EKPO.SPE_ABGRU AS Reasonforrejectionofquotationsandsalesorders_SPE_ABGRU,
    EKPO.SPE_CRM_SO AS CRMSalesOrderNumberforTPOP_SPE_CRM_SO,
    EKPO.SPE_CRM_SO_ITEM AS CRMSalesOrderItemNumberinTPOP_SPE_CRM_SO_ITEM,
    EKPO.SPE_CRM_REF_SO AS CRMReferenceOrderNumberforTPOP_SPE_CRM_REF_SO,
    EKPO.SPE_CRM_REF_ITEM AS CRMReferenceSalesOrderItemNumberinTPOP_SPE_CRM_REF_ITEM,
    EKPO.SPE_CRM_FKREL AS BillingRelevanceCRM_SPE_CRM_FKREL,
    EKPO.SPE_CHNG_SYS AS LastChangerSystemType_SPE_CHNG_SYS,
    EKPO.SPE_INSMK_SRC AS StockTypeofSourceStorageLocationinSTO_SPE_INSMK_SRC,
    EKPO.SPE_CQ_CTRLTYPE AS CQControlType_SPE_CQ_CTRLTYPE,
    EKPO.SPE_CQ_NOCQ AS NoTransmissionofCumulativeQuantitiesinSARelease_SPE_CQ_NOCQ,
    EKPO.REASON_CODE AS GoodsReceiptReasonCode_REASON_CODE,
    EKPO.CQU_SAR AS CumulativeGoodsReceipts_CQU_SAR,
    EKPO.ANZSN AS Numberofserialnumbers_ANZSN,
    EKPO.SPE_EWM_DTC AS EWMDeliveryBasedToleranceCheck_SPE_EWM_DTC,
    EKPO.EXLIN AS ItemNumberLength_EXLIN,
    EKPO.EXSNR AS ExternalSorting_EXSNR,
    EKPO.EHTYP AS ExternalHierarchyCategory_EHTYP,
    EKPO.RETPC AS RetentioninPercent_RETPC,
    EKPO.DPTYP AS DownPaymentFlag_DPTYP,
    EKPO.DPPCT AS DownPaymentPercentage_DPPCT,
    EKPO.DPAMT AS DownPaymentinDocumentCurrency_DPAMT,
    EKPO.DPDAT AS DueDateforDownPayment_DPDAT,
    EKPO.FLS_RSTO AS StoreReturnwithInboundandOutboundDelivery_FLS_RSTO,
    EKPO.EXT_RFX_NUMBER AS DocumentNumberofExternalDocument_EXT_RFX_NUMBER,
    EKPO.EXT_RFX_ITEM AS ItemNumberofExternalDocument_EXT_RFX_ITEM,
    EKPO.EXT_RFX_SYSTEM AS LogicalSystem_EXT_RFX_SYSTEM,
    EKPO.SRM_CONTRACT_ID AS CentralContract_SRM_CONTRACT_ID,
    EKPO.SRM_CONTRACT_ITM AS CentralContractItemNumber_SRM_CONTRACT_ITM,
    EKPO.BLK_REASON_ID AS BlockingReasonID_BLK_REASON_ID,
    EKPO.BLK_REASON_TXT AS BlockingReasonText_BLK_REASON_TXT,
    EKPO.ITCONS AS RealTimeConsumptionPostingofSubcontractingComponents_ITCONS,
    EKPO.FIXMG AS DeliveryDateandQuantityFixed_FIXMG,
    EKPO.WABWE AS FlagforGIbasedgoodsreceipt_WABWE,
    EKPO.TC_AUT_DET AS TaxCodeAutomaticallyDetermined_TC_AUT_DET,
    EKPO.MANUAL_TC_REASON AS ManualTaxCodeReason_MANUAL_TC_REASON,
    EKPO.FISCAL_INCENTIVE AS TaxIncentiveType_FISCAL_INCENTIVE,
    EKPO.TAX_SUBJECT_ST AS TaxSubject_TAX_SUBJECT_ST,
    EKPO.FISCAL_INCENTIVE_ID AS IncentiveID_FISCAL_INCENTIVE_ID,
    EKPO.ADVCODE AS AdviceCode_ADVCODE,
    EKPO.BUDGET_PD AS FMBudgetPeriod_BUDGET_PD,
    EKPO.EXCPE AS AcceptancePeriod_EXCPE,
    EKPO.IUID_RELEVANT AS IUIDRelevant_IUID_RELEVANT,
    EKPO.MRPIND AS RetailPriceRelevant_MRPIND,
    EKPO.REFSITE AS ReferenceSiteForPurchasing_REFSITE,
    EKPO.SERRU AS Typeofsubcontracting_SERRU,
    EKPO.SERNP AS SerialNumberProfile_SERNP,
    EKPO.DISUB_SOBKZ AS SpecialstockFlagSubcontracting_DISUB_SOBKZ,
    EKPO.DISUB_PSPNR AS WBSElement_DISUB_PSPNR,
    EKPO.DISUB_KUNNR AS CustomerNumber_DISUB_KUNNR,
    EKPO.DISUB_VBELN AS SalesandDistributionDocumentNumber_DISUB_VBELN,
    EKPO.DISUB_POSNR AS ItemnumberoftheSDdocument_DISUB_POSNR,
    EKPO.DISUB_OWNER AS Ownerofstock_DISUB_OWNER,
    EKPO.REF_ITEM AS ReferenceItemforRemainingQtyCancellation_REF_ITEM,
    EKPO.SOURCE_ID AS OriginProfile_SOURCE_ID,
    EKPO.SOURCE_KEY AS KeyinSourceSystem_SOURCE_KEY,
    EKPO.PUT_BACK AS FlagforPuttingBackfromGroupedPODocument_PUT_BACK,
    EKPO.POL_ID AS OrderListItemNumber_POL_ID,
    EKPO.CONS_ORDER AS PurchaseOrderforConsignment_CONS_ORDER,
    EKKN.SAKTO AS GlAccount_SAKTO,
    EKKN.GSBER AS BusinessArea_GSBER,
    EKKN.KOSTL AS CostCenter_KOSTL,
    EKKN.VBELN AS SalesDocumentNumber_VBELN_VBELN,
    EKKN.VBELP AS SalesDocumentItem_VBELP_VBELP,
    EKKN.ANLN1 AS MainAssetNumber_ANLN1,
    EKKN.ANLN2 AS AssetSubnumber_ANLN2,
    EKKN.AUFNR AS OrderNumber_AUFNR,
    EKKN.WEMPF AS Goodsrecipient_WEMPF,
    EKKN.ABLAD AS UnloadingPoint_ABLAD,
    EKKN.KOKRS AS ControllingArea_KOKRS,
    EKKN.KSTRG AS CostObject_KSTRG,
    EKKN.PAOBJNR AS ProfitabilitySegmentNumber_PAOBJNR,
    EKKN.PRCTR AS ProfitCenter_PRCTR,
    EKKN.PS_PSP_PNR AS WBS_PS_PSP_PNR,
    --   SKAT.txt50 AS GlAccountDesc_TXT50,
    EKKN.NPLNR AS NetworkNumber_NPLNR,
    EKKN.AUFPL AS Routingnumber_AUFPL,
    EKKN.IMKEY AS InternalKey_IMKEY,
    EKKN.APLZL AS Internalcounter_APLZL,
    EKKN.VPTNR AS Partneraccount_VPTNR,
    GoodsReceipt.MAX_BELNR AS LatestGrDocument_BELNR,
    GoodsReceipt.MAX_BUDAT AS LatestGrDocumentDate_BUDAT,
    GoodsReceipt.MENGE AS GrQuantity_MENGE,
    InvoiceReceipt.MAX_BELNR AS LatestIrDocument_BELNR,
    InvoiceReceipt.MAX_BUDAT AS LatestIrDocumentDate_BUDAT,
    InvoiceReceipt.MENGE AS IrQuantity_MENGE,
    COALESCE(EKKN.ZEKKN, '00') AS AccountAssignment_ZEKKN,
    CASE
      WHEN EKKO.STATU = '' THEN 'No Quotation Exists/Not Created via Sales'
      WHEN EKKO.STATU = 'A' THEN 'Quotation Exists in Case of RFQ Item'
      WHEN EKKO.STATU = 'F' THEN 'Created via Production Order'
      WHEN EKKO.STATU = 'V' THEN 'Created via Sales document'
      WHEN EKKO.STATU = 'W' THEN 'Created via Allocation Table'
      ELSE CAST(NULL AS STRING)
    END AS StatusDesc_STATU,
    CASE
      WHEN EKKO.FRGZU = '01' THEN 'Version in process'
      WHEN EKKO.FRGZU = '02' THEN 'Active'
      WHEN EKKO.FRGZU = '03' THEN 'In release'
      WHEN EKKO.FRGZU = '04' THEN 'Partially released'
      WHEN EKKO.FRGZU = '05' THEN 'Release completed'
      WHEN EKKO.FRGZU = '08' THEN 'Rejected'
      WHEN EKKO.FRGZU = '11' THEN 'In Distribution'
      WHEN EKKO.FRGZU = '12' THEN 'Error in Distribution'
      WHEN EKKO.FRGZU = '13' THEN 'Distributed'
      WHEN EKKO.FRGZU = '26' THEN 'In external approval'
      WHEN EKKO.FRGZU = '14' THEN 'In Preparation'
      ELSE CAST(NULL AS STRING)
    END AS ReleaseStatusDesc_FRGZU,
    COALESCE(
      EKPO.NETPR * TCURX.currency_fix,
      EKPO.NETPR
    ) AS NetPrice_NETPR,
    COALESCE(
      EKPO.NETPR * TCURX.currency_fix,
      EKPO.NETPR
    ) * CONV.UKURS AS NetPriceUSD_NETPR,
    COALESCE(
      EKKN.NETWR * TCURX.currency_fix,
      EKKN.NETWR,
      EKPO.NETWR * TCURX.currency_fix,
      EKPO.NETWR
    ) AS NetOrderValueinPOCurrency_NETWR,
    COALESCE(
      EKKN.NETWR * TCURX.currency_fix,
      EKKN.NETWR,
      EKPO.NETWR * TCURX.currency_fix,
      EKPO.NETWR
    ) * CONV.UKURS AS NetOrderValueinPOCurrencyUSD_NETWR,
    COALESCE(
      EKKN.BRTWR * TCURX.currency_fix,
      EKKN.BRTWR,
      EKPO.BRTWR * TCURX.currency_fix,
      EKPO.BRTWR
    ) AS GrossordervalueinPOcurrency_BRTWR,
    COALESCE(
      EKKN.BRTWR * TCURX.currency_fix,
      EKKN.BRTWR,
      EKPO.BRTWR * TCURX.currency_fix,
      EKPO.BRTWR
    ) * CONV.UKURS AS GrossordervalueinPOcurrencyUSD_BRTWR,
    CASE
      WHEN EKPO.PSTYP = '0' THEN 'Standard'
      WHEN EKPO.PSTYP = '1' THEN 'Limit'
      WHEN EKPO.PSTYP = '2' THEN 'Consignment'
      WHEN EKPO.PSTYP = '3' THEN 'Subcontracting'
      WHEN EKPO.PSTYP = '4' THEN 'Material unknown'
      WHEN EKPO.PSTYP = '5' THEN 'Third-party'
      WHEN EKPO.PSTYP = '6' THEN 'Text'
      WHEN EKPO.PSTYP = '7' THEN 'Stock transfer'
      WHEN EKPO.PSTYP = '8' THEN 'Material group'
      WHEN EKPO.PSTYP = '9' THEN 'Service'
      WHEN EKPO.PSTYP = 'A' THEN 'Enhanced Limit'
      WHEN EKPO.PSTYP = 'C' THEN 'Stock prov.by cust.'
      WHEN EKPO.PSTYP = 'P' THEN 'Return.trans.pack.'
      ELSE CAST(NULL AS STRING)
    END AS ItemCategoryinPurchasingDocumentDesc_PSTYP,

    CASE
      WHEN EKPO.BSTYP = 'A' THEN 'Request for quotation'
      WHEN EKPO.BSTYP = 'B' THEN 'Purchase requisition'
      WHEN EKPO.BSTYP = 'F' THEN 'Purchase order'
      WHEN EKPO.BSTYP = 'I' THEN 'Info record'
      WHEN EKPO.BSTYP = 'K' THEN 'Contract'
      WHEN EKPO.BSTYP = 'L' THEN 'Scheduling agreement'
      WHEN EKPO.BSTYP = 'Q' THEN 'Service entry sheet'
      WHEN EKPO.BSTYP = 'W' THEN 'Source list'
      WHEN EKPO.BSTYP = 'S' THEN 'Simplified service entry sheet'
      WHEN EKPO.BSTYP = 'R' THEN 'Request for Quotation'
      WHEN EKPO.BSTYP = 'O' THEN 'Quotation'
      WHEN EKPO.BSTYP = 'N' THEN 'Central Request for Quotation'
      WHEN EKPO.BSTYP = 'T' THEN 'Central Quotation'
      WHEN EKPO.BSTYP = 'C' THEN 'Central Contract'
      ELSE CAST(NULL AS STRING)
    END AS PurchasingDocumentCategoryDesc_BSTYP,
    COALESCE(
      EKKN.EFFWR * TCURX.currency_fix,
      EKKN.EFFWR,
      EKPO.EFFWR * TCURX.currency_fix,
      EKPO.EFFWR
    ) AS Effectivevalueofitem_EFFWR,
    --   GoodsReceipt.WRBTR AS GrAmount_WRBTR,
    COALESCE(
      EKKN.EFFWR * TCURX.currency_fix,
      EKKN.EFFWR,
      EKPO.EFFWR * TCURX.currency_fix,
      EKPO.EFFWR
    ) * CONV.UKURS AS EffectivevalueofitemUSD_EFFWR,
    COALESCE(EKPO.BONBA * TCURX.currency_fix, EKPO.BONBA) AS Rebatebasis1_BONBA,
    COALESCE(EKPO.MENGE, 0) - COALESCE(GoodsReceipt.MENGE, 0) AS GrBalanceQuantity_MENGE,
    COALESCE(GoodsReceipt.WRBTR * TCURX.currency_fix, GoodsReceipt.WRBTR) AS GrAmount_WRBTR,
    COALESCE(GoodsReceipt.WRBTR * TCURX.currency_fix, GoodsReceipt.WRBTR) * CONV.UKURS AS GrAmountUSD_WRBTR,
    COALESCE(
      COALESCE(
        EKKN.NETWR * TCURX.currency_fix,
        EKKN.NETWR,
        EKPO.NETWR * TCURX.currency_fix,
        EKPO.NETWR
      ), 0
    )
    -
    COALESCE(
      COALESCE(
        GoodsReceipt.WRBTR * TCURX.currency_fix,
        GoodsReceipt.WRBTR
      ), 0
    ) AS GrBalanceAmount_NETWR_WRBTR,
    (COALESCE(
      COALESCE(
        EKKN.NETWR * TCURX.currency_fix,
        EKKN.NETWR,
        EKPO.NETWR * TCURX.currency_fix,
        EKPO.NETWR
      ), 0
    )
    -
    COALESCE(
      COALESCE(
        GoodsReceipt.WRBTR * TCURX.currency_fix,
        GoodsReceipt.WRBTR
      ), 0
    )) * CONV.UKURS AS GrBalanceAmountUSD_NETWR_WRBTR,
    COALESCE(EKPO.MENGE, 0) - COALESCE(InvoiceReceipt.MENGE, 0) AS IrBalanceQuantity_MENGE,
    --   InvoiceReceipt.REFWR AS IrAmount_REFWR,
    COALESCE(InvoiceReceipt.REFWR * TCURX.currency_fix, InvoiceReceipt.REFWR) AS IrAmount_REFWR,
    COALESCE(InvoiceReceipt.REFWR * TCURX.currency_fix, InvoiceReceipt.REFWR) * CONV.UKURS AS IrAmountUSD_REFWR,
    COALESCE(
      COALESCE(
        EKKN.NETWR * TCURX.currency_fix,
        EKKN.NETWR,
        EKPO.NETWR * TCURX.currency_fix,
        EKPO.NETWR
      ), 0
    )
    -
    COALESCE(
      COALESCE(
        InvoiceReceipt.REFWR * TCURX.currency_fix,
        InvoiceReceipt.REFWR
      ), 0
    ) AS IrBalanceAmount_NETWR_REFWR,
    (COALESCE(
      COALESCE(
        EKKN.NETWR * TCURX.currency_fix,
        EKKN.NETWR,
        EKPO.NETWR * TCURX.currency_fix,
        EKPO.NETWR
      ), 0
    )
    -
    COALESCE(
      COALESCE(
        InvoiceReceipt.REFWR * TCURX.currency_fix,
        InvoiceReceipt.REFWR
      ), 0
    )) * CONV.UKURS AS IrBalanceAmountUSD_NETWR_REFWR
  FROM `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.ekko` AS EKKO
  LEFT JOIN `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.ekpo` AS EKPO
    ON
      EKKO.MANDT = EKPO.MANDT
      AND EKKO.EBELN = EKPO.EBELN
  LEFT JOIN EKKN
    ON
      EKKO.MANDT = EKPO.MANDT
      AND EKKO.EBELN = EKPO.EBELN
      AND EKPO.EBELP = EKKN.EBELP

  -- BI Enhancement to GR & IR Domains
  LEFT JOIN GoodsReceipt
    ON
      EKKO.MANDT = GoodsReceipt.MANDT
      AND EKKO.EBELN = GoodsReceipt.EBELN
      AND EKPO.EBELP = GoodsReceipt.EBELP
      AND COALESCE(EKKN.ZEKKN, '00') = GoodsReceipt.ZEKKN
  LEFT JOIN InvoiceReceipt
    ON
      EKKO.MANDT = InvoiceReceipt.MANDT
      AND EKKO.EBELN = InvoiceReceipt.EBELN
      AND EKPO.EBELP = InvoiceReceipt.EBELP
      AND COALESCE(EKKN.ZEKKN, '00') = InvoiceReceipt.ZEKKN
  -- Correction on decimal misplacement
  LEFT JOIN TCURX
    ON EKKO.WAERS = TCURX.CURRKEY
  LEFT JOIN CONV
    ON EKKO.MANDT = CONV.MANDT
      AND EKKO.WAERS = CONV.FCURR
      AND CAST(EKKO.aedat AS DATE) = CONV.GDATU
)
