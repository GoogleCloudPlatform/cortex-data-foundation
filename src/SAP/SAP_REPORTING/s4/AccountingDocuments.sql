WITH BSEG AS (
  -------------------------------------------------------------------------
  -- Subquery to flip "debit/credit" transactions with "positive" for debit
  -- and "negative" for credit
  SELECT
    * EXCEPT (
      DMBTR,
      WRBTR,
      KZBTR,
      PSWBT,
      TXBHW,
      TXBFW,
      MWSTS,
      WMWST,
      HWBAS,
      FWBAS,
      HWZUZ,
      FWZUZ,
      QSSHB,
      GBETR,
      BDIF2,
      FDWBT,
      SKFBT,
      SKNTO,
      WSKTO,
      NEBTR,
      DMBT1,
      WRBT1,
      DMBT2,
      WRBT2,
      DMBT3,
      WRBT3,
      BLNBT,
      KLIBT,
      QBSHB,
      QSFBT,
      REWRT,
      REWWR,
      BONFB,
      DMBE2,
      DMBE3,
      DMB21,
      DMB22,
      DMB23,
      DMB31,
      DMB32,
      DMB33,
      MWST2,
      MWST3,
      NAVH2,
      NAVH3,
      SKNT2,
      SKNT3,
      BDIF3,
      RDIF3,
      TXBH2,
      TXBH3,
      STTAX,
      PYAMT,
      PENLC1,
      PENLC2,
      PENLC3,
      PENFC
    ),
    CASE
      WHEN shkzg = 'S' THEN DMBTR -- When debit = keep it the way it is
      WHEN shkzg = 'H' THEN DMBTR * -1 -- When credit = make it negative
      ELSE DMBTR
    END AS DMBTR,
    CASE
      WHEN shkzg = 'S' THEN WRBTR
      WHEN shkzg = 'H' THEN WRBTR * -1
      ELSE WRBTR
    END AS WRBTR,
    CASE
      WHEN shkzg = 'S' THEN KZBTR
      WHEN shkzg = 'H' THEN KZBTR * -1
      ELSE KZBTR
    END AS KZBTR,
    CASE
      WHEN shkzg = 'S' THEN PSWBT
      WHEN shkzg = 'H' THEN PSWBT * -1
      ELSE PSWBT
    END AS PSWBT,
    CASE
      WHEN shkzg = 'S' THEN TXBHW
      WHEN shkzg = 'H' THEN TXBHW * -1
      ELSE TXBHW
    END AS TXBHW,
    CASE
      WHEN shkzg = 'S' THEN TXBFW
      WHEN shkzg = 'H' THEN TXBFW * -1
      ELSE TXBFW
    END AS TXBFW,
    CASE
      WHEN shkzg = 'S' THEN MWSTS
      WHEN shkzg = 'H' THEN MWSTS * -1
      ELSE MWSTS
    END AS MWSTS,
    CASE
      WHEN shkzg = 'S' THEN WMWST
      WHEN shkzg = 'H' THEN WMWST * -1
      ELSE WMWST
    END AS WMWST,
    CASE
      WHEN shkzg = 'S' THEN HWBAS
      WHEN shkzg = 'H' THEN HWBAS * -1
      ELSE HWBAS
    END AS HWBAS,
    CASE
      WHEN shkzg = 'S' THEN FWBAS
      WHEN shkzg = 'H' THEN FWBAS * -1
      ELSE FWBAS
    END AS FWBAS,
    CASE
      WHEN shkzg = 'S' THEN HWZUZ
      WHEN shkzg = 'H' THEN HWZUZ * -1
      ELSE HWZUZ
    END AS HWZUZ,
    CASE
      WHEN shkzg = 'S' THEN FWZUZ
      WHEN shkzg = 'H' THEN FWZUZ * -1
      ELSE FWZUZ
    END AS FWZUZ,
    CASE
      WHEN shkzg = 'S' THEN QSSHB
      WHEN shkzg = 'H' THEN QSSHB * -1
      ELSE QSSHB
    END AS QSSHB,
    CASE
      WHEN shkzg = 'S' THEN GBETR
      WHEN shkzg = 'H' THEN GBETR * -1
      ELSE GBETR
    END AS GBETR,
    CASE
      WHEN shkzg = 'S' THEN BDIF2
      WHEN shkzg = 'H' THEN BDIF2 * -1
      ELSE BDIF2
    END AS BDIF2,
    CASE
      WHEN shkzg = 'S' THEN FDWBT
      WHEN shkzg = 'H' THEN FDWBT * -1
      ELSE FDWBT
    END AS FDWBT,
    CASE
      WHEN shkzg = 'S' THEN SKFBT
      WHEN shkzg = 'H' THEN SKFBT * -1
      ELSE SKFBT
    END AS SKFBT,
    CASE
      WHEN shkzg = 'S' THEN SKNTO
      WHEN shkzg = 'H' THEN SKNTO * -1
      ELSE SKNTO
    END AS SKNTO,
    CASE
      WHEN shkzg = 'S' THEN WSKTO
      WHEN shkzg = 'H' THEN WSKTO * -1
      ELSE WSKTO
    END AS WSKTO,
    CASE
      WHEN shkzg = 'S' THEN NEBTR
      WHEN shkzg = 'H' THEN NEBTR * -1
      ELSE NEBTR
    END AS NEBTR,
    CASE
      WHEN shkzg = 'S' THEN DMBT1
      WHEN shkzg = 'H' THEN DMBT1 * -1
      ELSE DMBT1
    END AS DMBT1,
    CASE
      WHEN shkzg = 'S' THEN WRBT1
      WHEN shkzg = 'H' THEN WRBT1 * -1
      ELSE WRBT1
    END AS WRBT1,
    CASE
      WHEN shkzg = 'S' THEN DMBT2
      WHEN shkzg = 'H' THEN DMBT2 * -1
      ELSE DMBT2
    END AS DMBT2,
    CASE
      WHEN shkzg = 'S' THEN WRBT2
      WHEN shkzg = 'H' THEN WRBT2 * -1
      ELSE WRBT2
    END AS WRBT2,
    CASE
      WHEN shkzg = 'S' THEN DMBT3
      WHEN shkzg = 'H' THEN DMBT3 * -1
      ELSE DMBT3
    END AS DMBT3,
    CASE
      WHEN shkzg = 'S' THEN WRBT3
      WHEN shkzg = 'H' THEN WRBT3 * -1
      ELSE WRBT3
    END AS WRBT3,
    CASE
      WHEN shkzg = 'S' THEN BLNBT
      WHEN shkzg = 'H' THEN BLNBT * -1
      ELSE BLNBT
    END AS BLNBT,
    CASE
      WHEN shkzg = 'S' THEN KLIBT
      WHEN shkzg = 'H' THEN KLIBT * -1
      ELSE KLIBT
    END AS KLIBT,
    CASE
      WHEN shkzg = 'S' THEN QBSHB
      WHEN shkzg = 'H' THEN QBSHB * -1
      ELSE QBSHB
    END AS QBSHB,
    CASE
      WHEN shkzg = 'S' THEN QSFBT
      WHEN shkzg = 'H' THEN QSFBT * -1
      ELSE QSFBT
    END AS QSFBT,
    CASE
      WHEN shkzg = 'S' THEN REWRT
      WHEN shkzg = 'H' THEN REWRT * -1
      ELSE REWRT
    END AS REWRT,
    CASE
      WHEN shkzg = 'S' THEN REWWR
      WHEN shkzg = 'H' THEN REWWR * -1
      ELSE REWWR
    END AS REWWR,
    CASE
      WHEN shkzg = 'S' THEN BONFB
      WHEN shkzg = 'H' THEN BONFB * -1
      ELSE BONFB
    END AS BONFB,
    CASE
      WHEN shkzg = 'S' THEN DMBE2
      WHEN shkzg = 'H' THEN DMBE2 * -1
      ELSE DMBE2
    END AS DMBE2,
    CASE
      WHEN shkzg = 'S' THEN DMBE3
      WHEN shkzg = 'H' THEN DMBE3 * -1
      ELSE DMBE3
    END AS DMBE3,
    CASE
      WHEN shkzg = 'S' THEN DMB21
      WHEN shkzg = 'H' THEN DMB21 * -1
      ELSE DMB21
    END AS DMB21,
    CASE
      WHEN shkzg = 'S' THEN DMB22
      WHEN shkzg = 'H' THEN DMB22 * -1
      ELSE DMB22
    END AS DMB22,
    CASE
      WHEN shkzg = 'S' THEN DMB23
      WHEN shkzg = 'H' THEN DMB23 * -1
      ELSE DMB23
    END AS DMB23,
    CASE
      WHEN shkzg = 'S' THEN DMB31
      WHEN shkzg = 'H' THEN DMB31 * -1
      ELSE DMB31
    END AS DMB31,
    CASE
      WHEN shkzg = 'S' THEN DMB32
      WHEN shkzg = 'H' THEN DMB32 * -1
      ELSE DMB32
    END AS DMB32,
    CASE
      WHEN shkzg = 'S' THEN DMB33
      WHEN shkzg = 'H' THEN DMB33 * -1
      ELSE DMB33
    END AS DMB33,
    CASE
      WHEN shkzg = 'S' THEN MWST2
      WHEN shkzg = 'H' THEN MWST2 * -1
      ELSE MWST2
    END AS MWST2,
    CASE
      WHEN shkzg = 'S' THEN MWST3
      WHEN shkzg = 'H' THEN MWST3 * -1
      ELSE MWST3
    END AS MWST3,
    CASE
      WHEN shkzg = 'S' THEN NAVH2
      WHEN shkzg = 'H' THEN NAVH2 * -1
      ELSE NAVH2
    END AS NAVH2,
    CASE
      WHEN shkzg = 'S' THEN NAVH3
      WHEN shkzg = 'H' THEN NAVH3 * -1
      ELSE NAVH3
    END AS NAVH3,
    CASE
      WHEN shkzg = 'S' THEN SKNT2
      WHEN shkzg = 'H' THEN SKNT2 * -1
      ELSE SKNT2
    END AS SKNT2,
    CASE
      WHEN shkzg = 'S' THEN SKNT3
      WHEN shkzg = 'H' THEN SKNT3 * -1
      ELSE SKNT3
    END AS SKNT3,
    CASE
      WHEN shkzg = 'S' THEN BDIF3
      WHEN shkzg = 'H' THEN BDIF3 * -1
      ELSE BDIF3
    END AS BDIF3,
    CASE
      WHEN shkzg = 'S' THEN RDIF3
      WHEN shkzg = 'H' THEN RDIF3 * -1
      ELSE RDIF3
    END AS RDIF3,
    CASE
      WHEN shkzg = 'S' THEN TXBH2
      WHEN shkzg = 'H' THEN TXBH2 * -1
      ELSE TXBH2
    END AS TXBH2,
    CASE
      WHEN shkzg = 'S' THEN TXBH3
      WHEN shkzg = 'H' THEN TXBH3 * -1
      ELSE TXBH3
    END AS TXBH3,
    CASE
      WHEN shkzg = 'S' THEN STTAX
      WHEN shkzg = 'H' THEN STTAX * -1
      ELSE STTAX
    END AS STTAX,
    CASE
      WHEN shkzg = 'S' THEN PYAMT
      WHEN shkzg = 'H' THEN PYAMT * -1
      ELSE PYAMT
    END AS PYAMT,
    CASE
      WHEN shkzg = 'S' THEN PENLC1
      WHEN shkzg = 'H' THEN PENLC1 * -1
      ELSE PENLC1
    END AS PENLC1,
    CASE
      WHEN shkzg = 'S' THEN PENLC2
      WHEN shkzg = 'H' THEN PENLC2 * -1
      ELSE PENLC2
    END AS PENLC2,
    CASE
      WHEN shkzg = 'S' THEN PENLC3
      WHEN shkzg = 'H' THEN PENLC3 * -1
      ELSE PENLC3
    END AS PENLC3,
    CASE
      WHEN shkzg = 'S' THEN PENFC
      WHEN shkzg = 'H' THEN PENFC * -1
      ELSE PENFC
    END AS PENFC
  FROM
    `{{ project_id_src }}.{{ dataset_cdc_processed_s4 }}.bseg`
)

SELECT
  BKPF.MANDT AS Client_MANDT,
  BKPF.BUKRS AS CompanyCode_BUKRS,
  BKPF.BELNR AS AccountingDocumentNumber_BELNR,
  BKPF.GJAHR AS FiscalYear_GJAHR,
  BSEG.BUZEI AS NumberOfLineItemWithinAccountingDocument_BUZEI,
  BKPF.BLART AS DocumentType_BLART,
  BKPF.BLDAT AS DocumentDateInDocument_BLDAT,
  BSEG.H_BUDAT AS PostingDateInTheDocument_BUDAT,
  BKPF.MONAT AS FiscalPeriod_MONAT,
  BKPF.CPUDT AS DayOnWhichAccountingDocumentWasEntered_CPUDT,
  BKPF.CPUTM AS TimeOfEntry_CPUTM,
  BKPF.AEDAT AS DateOfTheLastDocumentChangeByTransaction_AEDAT,
  BKPF.UPDDT AS DateOfTheLastDocumentUpdate_UPDDT,
  BKPF.WWERT AS TranslationDate_WWERT,
  BKPF.USNAM AS UserName_USNAM,
  BKPF.TCODE AS TransactionCode_TCODE,
  BKPF.BVORG AS NumberOfACrossCompanyCodePostingTransaction_BVORG,
  BKPF.XBLNR AS ReferenceDocumentNumber_XBLNR,
  BKPF.DBBLG AS RecurringEntryDocumentNumber_DBBLG,
  BKPF.STBLG AS ReverseDocumentNumber_STBLG,
  BKPF.STJAH AS ReverseDocumentFiscalYear_STJAH,
  BKPF.BKTXT AS DocumentHeaderText_BKTXT,
  BKPF.WAERS AS CurrencyKey_WAERS,
  BKPF.KURSF AS ExchangeRate_KURSF,
  BKPF.KZWRS AS CurrencyKeyForTheGroupCurrency_KZWRS,
  BKPF.KZKRS AS GroupCurrencyExchangeRate_KZKRS,
  BKPF.BSTAT AS DocumentStatus_BSTAT,
  BKPF.XNETB AS Indicator_DocumentPostedNet_XNETB,
  BKPF.FRATH AS UnplannedDeliveryCosts_FRATH,
  BKPF.XRUEB AS Indicator_DocumentIsPostedToAPreviousPeriod_XRUEB,
  BKPF.GLVOR AS BusinessTransaction_GLVOR,
  BKPF.GRPID AS BatchInputSessionName_GRPID,
  BKPF.DOKID AS DocumentNameInTheArchiveSystem_DOKID,
  BKPF.ARCID AS ExtractIdDocumentHeader_ARCID,
  BKPF.IBLAR AS InternalDocumentTypeForDocumentControl_IBLAR,
  BKPF.AWTYP AS ReferenceProcedure_AWTYP,
  BKPF.AWKEY AS ObjectKey_AWKEY,
  BKPF.FIKRS AS FinancialManagementArea_FIKRS,
  BKPF.HWAER AS LocalCurrency_HWAER,
  BKPF.HWAE2 AS CurrencyKeyOfSecondLocalCurrency_HWAE2,
  BKPF.HWAE3 AS CurrencyKeyOfThirdLocalCurrency_HWAE3,
  BKPF.KURS2 AS ExchangeRateForTheSecondLocalCurrency_KURS2,
  BKPF.KURS3 AS ExchangeRateForTheThirdLocalCurrency_KURS3,
  BKPF.BASW2 AS SourceCurrencyForCurrencyTranslation_BASW2,
  BKPF.BASW3 AS SourceCurrencyForCurrencyTranslation_BASW3,
  BKPF.UMRD2 AS TranslationDateTypeForSecondLocalCurrency_UMRD2,
  BKPF.UMRD3 AS TranslationDateTypeForThirdLocalCurrency_UMRD3,
  BKPF.XSTOV AS Indicator_DocumentIsFlaggedForReversal_XSTOV,
  BKPF.STODT AS PlannedDateForTheReversePosting_STODT,
  BKPF.XMWST AS CalculateTaxAutomatically_XMWST,
  BKPF.CURT2 AS CurrencyTypeOfSecondLocalCurrency_CURT2,
  BKPF.CURT3 AS CurrencyTypeOfThirdLocalCurrency_CURT3,
  BKPF.KUTY2 AS ExchangeRateType_KUTY2,
  BKPF.KUTY3 AS ExchangeRateType_KUTY3,
  BKPF.XSNET AS GlAccountAmountsEnteredExcludeTax_XSNET,
  BKPF.AUSBK AS SourceCompanyCode_AUSBK,
  BKPF.XUSVR AS EnterVatSalesTaxOnDetailScreen_XUSVR,
  BKPF.DUEFL AS StatusOfDataTransferIntoSubsequentRelease_DUEFL,
  BKPF.AWSYS AS LogicalSystem_AWSYS,
  BKPF.TXKRS AS ExchangeRateForTaxes_TXKRS,
  BKPF.CTXKRS AS RateForTaxValuesInLocalCurrency__plantsAbroad___CTXKRS,
  BKPF.LOTKZ AS LotNumberForRequests_LOTKZ,
  BKPF.XWVOF AS Indicator_CustomerBillOfExchangePaymentBeforeDueDate_XWVOF,
  BKPF.STGRD AS ReasonForReversal_STGRD,
  BKPF.PPNAM AS NameOfUserWhoParkedThisDocument_PPNAM,
  BKPF.BRNCH AS BranchNumber_BRNCH,
  BKPF.NUMPG AS NumberOfPagesOfInvoice_NUMPG,
  BKPF.ADISC AS Indicator_EntryRepresentsADiscountDocument_ADISC,
  BKPF.XREF1_HD AS ReferenceKey1InternalForDocumentHeader_XREF1_HD,
  BKPF.XREF2_HD AS ReferenceKey2InternalForDocumentHeader_XREF2_HD,
  BKPF.XREVERSAL AS IsReversalDoc_XREVERSAL,
  BKPF.REINDAT AS InvoiceReceiptDate_REINDAT,
  BKPF.RLDNR AS LedgerInGeneralLedgerAccounting_RLDNR,
  BKPF.LDGRP AS LedgerGroup_LDGRP,
  BKPF.PROPMANO AS RealEstateManagementMandate_PROPMANO,
  BKPF.XBLNR_ALT AS AlternativeReferenceNumber_XBLNR_ALT,
  BKPF.VATDATE AS TaxReportingDate_VATDATE,
  BKPF.DOCCAT AS ClassificationOfAnFiDocument_DOCCAT,
  BKPF.XSPLIT AS FiDocumentOriginatesFromSplitPosting__indicator___XSPLIT,
  BKPF.CASH_ALLOC AS CashRelevantDocument_CASH_ALLOC,
  BKPF.FOLLOW_ON AS FollowOnDocumentIndicator_FOLLOW_ON,
  BKPF.XREORG AS ContainsOpenItemTransferredDuringReorg_XREORG,
  BKPF.SUBSET AS DefinesSubsetOfComponentsForTheFicoInterface_SUBSET,
  BKPF.KURST AS ExchangeRateType_KURST,
  BKPF.KURSX AS MarketDataExchangeRate_KURSX,
  BKPF.KUR2X AS MarketDataExchangeRate2_KUR2X,
  BKPF.KUR3X AS MarketDataExchangeRate3_KUR3X,
  BKPF.XMCA AS DocumentOriginatesFromMultiCurrencyAccounting_XMCA,
  BKPF.RESUBMISSION AS DateOfResubmission_RESUBMISSION,
  BKPF.PSOTY AS DocumentCategoryPaymentRequests_PSOTY,
  BKPF.PSOAK AS Reason_PSOAK,
  BKPF.PSOKS AS Region_PSOKS,
  BKPF.PSOSG AS ReasonForReversalIsPsRequests_PSOSG,
  BKPF.PSOFN AS IsPs_FileNumber_PSOFN,
  BKPF.INTFORM AS InterestFormula_INTFORM,
  BKPF.INTDATE AS InterestCalcDate_INTDATE,
  BKPF.PSOBT AS PostingDay_PSOBT,
  BKPF.PSOZL AS ActualPosting_PSOZL,
  BKPF.PSODT AS DateOfLastChange_PSODT,
  BKPF.PSOTM AS LastChangedAt_PSOTM,
  BKPF.FM_UMART AS TypeOfPaymentTransfer_FM_UMART,
  BKPF.CCINS AS PaymentCards_CardType_CCINS,
  BKPF.CCNUM AS PaymentCards_CardNumber_CCNUM,
  BKPF.SSBLK AS PaymentStatisticalSamplingBlock_SSBLK,
  BKPF.BATCH AS LotNumberForDocuments_BATCH,
  BKPF.SNAME AS UserName_SNAME,
  BKPF.SAMPLED AS SampledInvoiceByPaymentCertification_SAMPLED,
  BKPF.EXCLUDE_FLAG AS PpaExcludeIndicator_EXCLUDE_FLAG,
  BKPF.BLIND AS BudgetaryLedgerIndicator_BLIND,
  BKPF.OFFSET_STATUS AS TreasuryOffsetStatus_OFFSET_STATUS,
  BKPF.OFFSET_REFER_DAT AS DateRecordReferredToTreasury_OFFSET_REFER_DAT,
  BKPF.KNUMV AS NumberOfTheDocumentCondition_KNUMV,
  BSEG.BUZID AS IdentificationOfTheLineItem_BUZID,
  BSEG.AUGDT AS ClearingDate_AUGDT,
  BSEG.AUGCP AS ClearingEntryDate_AUGCP,
  BSEG.AUGBL AS DocumentNumberOfTheClearingDocument_AUGBL,
  BSEG.BSCHL AS PostingKey_BSCHL,
  BSEG.KOART AS AccountType_KOART,
  BSEG.UMSKZ AS SpecialGlIndicator_UMSKZ,
  BSEG.UMSKS AS SpecialGlTransactionType_UMSKS,
  BSEG.ZUMSK AS TargetSpecialGlIndicator_ZUMSK,
  BSEG.SHKZG AS DebitcreditIndicator_SHKZG,
  BSEG.GSBER AS BusinessArea_GSBER,
  BSEG.PARGB AS TradingPartnersBusinessArea_PARGB,
  BSEG.MWSKZ AS TaxOnSalespurchasesCode_MWSKZ,
  BSEG.QSSKZ AS WithholdingTaxCode_QSSKZ,
  BSEG.PSWSL AS UpdateCurrencyForGeneralLedgerTransactionFigures_PSWSL,
  BSEG.FWZUZ AS AdditionalTaxInDocumentCurrency_FWZUZ,
  BSEG.SHZUZ AS DebitcreditAdditionForCashDiscount_SHZUZ,
  BSEG.STEKZ AS VersionNumberComponent_STEKZ,
  BSEG.MWART AS TaxType_MWART,
  BSEG.TXGRP AS GroupIndicatorForTaxLineItems_TXGRP,
  BSEG.KTOSL AS TransactionKey_KTOSL,
  BSEG.KURSR AS HedgedExchangeRate_KURSR,
  BSEG.BDIFF AS ValuationDifference_BDIFF,
  BSEG.BDIF2 AS ValuationDifferenceForTheSecondLocalCurrency_BDIF2,
  BSEG.VALUT AS ValueDate_VALUT,
  BSEG.ZUONR AS AssignmentNumber_ZUONR,
  BSEG.SGTXT AS ItemText_SGTXT,
  BSEG.ZINKZ AS ExemptedFromInterestCalculation_ZINKZ,
  BSEG.VBUND AS CompanyIdOfTradingPartner_VBUND,
  BSEG.BEWAR AS TransactionType_BEWAR,
  BSEG.ALTKT AS GroupAccountNumber_ALTKT,
  BSEG.VORGN AS TransactionTypeForGeneralLedger_VORGN,
  BSEG.FDLEV AS PlanningLevel_FDLEV,
  BSEG.FDGRP AS PlanningGroup_FDGRP,
  BSEG.FDWBT AS PlannedAmountInDocumentOrGlAccountCurrency_FDWBT,
  BSEG.FDTAG AS PlanningDate_FDTAG,
  BSEG.FKONT AS FinancialBudgetItem_FKONT,
  BSEG.KOKRS AS ControllingArea_KOKRS,
  BSEG.KOSTL AS CostCenter_KOSTL,
  BSEG.PROJN AS Ps_posnr_PROJN,
  BSEG.AUFNR AS OrderNumber_AUFNR,
  BSEG.VBELN AS BillingDocument_VBELN,
  BSEG.VBEL2 AS SalesDocument_VBEL2,
  BSEG.POSN2 AS SalesDocumentItem_POSN2,
  BSEG.ETEN2 AS ScheduleLineNumber_ETEN2,
  BSEG.ANLN1 AS MainAssetNumber_ANLN1,
  BSEG.ANLN2 AS AssetSubnumber_ANLN2,
  BSEG.ANBWA AS AssetTransactionType_ANBWA,
  BSEG.BZDAT AS AssetValueDate_BZDAT,
  BSEG.PERNR AS PersonnelNumber_PERNR,
  BSEG.XUMSW AS Indicator_SalesRelatedItem_XUMSW,
  BSEG.XHRES AS Indicator_ResidentGlAccount_XHRES,
  BSEG.XKRES AS Indicator_CanLineItemsBeDisplayedByAccount_XKRES,
  BSEG.XOPVW AS Indicator_OpenItemManagement_XOPVW,
  BSEG.XCPDD AS Indicator_AddressAndBankDataSetIndividually_XCPDD,
  BSEG.XSKST AS Indicator_StatisticalPostingToCostCenter_XSKST,
  BSEG.XSAUF AS Indicator_PostingToOrderIsStatistical_XSAUF,
  BSEG.XSPRO AS Indicator_PostingToProjectIsStatistical_XSPRO,
  BSEG.XSERG AS Indicator_PostingToProfitabilityAnalysisIsStatistical_XSERG,
  BSEG.XFAKT AS Indicator_BillingDocumentUpdateSuccessful_XFAKT,
  BSEG.XUMAN AS Indicator_TransferPostingFromDownPayment_XUMAN,
  BSEG.XANET AS Indicator_DownPaymentInNetProcedure_XANET,
  BSEG.XSKRL AS Indicator_LineItemNotLiableToCashDiscount_XSKRL,
  BSEG.XINVE AS Indicator_CapitalGoodsAffected_XINVE,
  BSEG.XPANZ AS DisplayItem_XPANZ,
  BSEG.XAUTO AS Indicator_LineItemAutomaticallyCreated_XAUTO,
  BSEG.XNCOP AS Indicator_ItemsCannotBeCopied_XNCOP,
  BSEG.XZAHL AS Indicator_IsPostingKeyUsedInAPaymentTransaction_XZAHL,
  BSEG.SAKNR AS GlAccountNumber_SAKNR,
  BSEG.HKONT AS GeneralLedgerAccount_HKONT,
  BSEG.KUNNR AS CustomerNumber_KUNNR,
  BSEG.LIFNR AS AccountNumberOfVendorOrCreditor_LIFNR,
  BSEG.FILKD AS AccountNumberOfTheBranch_FILKD,
  BSEG.XBILK AS Indicator_AccountIsABalanceSheetAccount_XBILK,
  BSEG.GVTYP AS PlStatementAccountType_GVTYP,
  BSEG.HZUON AS AssignmentNumberForSpecialGlAccounts_HZUON,
  BSEG.ZFBDT AS BaselineDateForDueDateCalculation_ZFBDT,
  BSEG.ZTERM AS TermsOfPaymentKey_ZTERM,
  BSEG.ZBD1T AS CashDiscountDays1_ZBD1T,
  BSEG.ZBD2T AS CashDiscountDays2_ZBD2T,
  BSEG.ZBD3T AS NetPaymentTermsPeriod_ZBD3T,
  BSEG.ZBD1P AS CashDiscountPercentage1_ZBD1P,
  BSEG.ZBD2P AS CashDiscountPercentage2_ZBD2P,
  BSEG.ZLSCH AS PaymentMethod_ZLSCH,
  BSEG.ZLSPR AS PaymentBlockKey_ZLSPR,
  BSEG.ZBFIX AS FixedPaymentTerms_ZBFIX,
  BSEG.HBKID AS ShortKeyForAHouseBank_HBKID,
  BSEG.BVTYP AS PartnerBankType_BVTYP,
  BSEG.MWSK1 AS TaxCodeForDistribution_MWSK1,
  BSEG.MWSK2 AS TaxCodeForDistribution_MWSK2,
  BSEG.MWSK3 AS TaxCodeForDistribution_MWSK3,
  BSEG.REBZG AS InvoiceToWhichTheTransactionBelongs_REBZG,
  BSEG.REBZJ AS FiscalYearOfTheRelevantInvoice__forCreditMemo___REBZJ,
  BSEG.REBZZ AS LineItemInTheRelevantInvoice_REBZZ,
  BSEG.REBZT AS FollowOnDocumentType_REBZT,
  BSEG.ZOLLT AS CustomsTariffNumber_ZOLLT,
  BSEG.ZOLLD AS CustomsDate_ZOLLD,
  BSEG.LZBKZ AS StateCentralBankIndicator_LZBKZ,
  BSEG.LANDL AS SupplyingCountry_LANDL,
  BSEG.DIEKZ AS ServiceIndicator__foreignPayment___DIEKZ,
  BSEG.SAMNR AS InvoiceListNumber_SAMNR,
  BSEG.ABPER AS SettlementPeriod_ABPER,
  BSEG.VRSKZ AS InsuranceIndicator_VRSKZ,
  BSEG.VRSDT AS InsuranceDate_VRSDT,
  BSEG.DISBN AS NumberOfBillOfExchangeUsageDocument_DISBN,
  BSEG.DISBJ AS FiscalYearOfBillOfExchangeUsageDocument_DISBJ,
  BSEG.DISBZ AS LineItemWithinTheBillOfExchangeUsageDocument_DISBZ,
  BSEG.WVERW AS BillOfExchangeUsageType_WVERW,
  BSEG.ANFBN AS DocumentNumberOfTheBillOfExchangePaymentRequest_ANFBN,
  BSEG.ANFBJ AS FiscalYearOfTheBillOfExchangePaymentRequestDocument_ANFBJ,
  BSEG.ANFBU AS CompanyCodeInWhichBillOfExchPaymentRequestIsPosted_ANFBU,
  BSEG.ANFAE AS BillOfExchangePaymentRequestDueDate_ANFAE,
  BSEG.BLNBT AS BaseAmountForDeterminingThePreferenceAmount_BLNBT,
  BSEG.BLNKZ AS SubsidyIndicatorForDeterminingTheReductionRates_BLNKZ,
  BSEG.BLNPZ AS PreferencePercentageRate_BLNPZ,
  BSEG.MSCHL AS DunningKey_MSCHL,
  BSEG.MANSP AS DunningBlock_MANSP,
  BSEG.MADAT AS DateOfLastDunningNotice_MADAT,
  BSEG.MANST AS DunningLevel_MANST,
  BSEG.MABER AS DunningArea_MABER,
  BSEG.ESRNR AS PorSubscriberNumber_ESRNR,
  BSEG.ESRRE AS PorReferenceNumber_ESRRE,
  BSEG.ESRPZ AS PorCheckDigit_ESRPZ,
  BSEG.QSZNR AS CertificateNumberOfTheWithholdingTaxExemption_QSZNR,
  BSEG.NAVHW AS NonDeductibleInputTax_NAVHW,
  BSEG.NAVFW AS NonDeductibleInputTax_NAVFW,
  BSEG.MATNR AS MaterialNumber_MATNR,
  BSEG.WERKS AS Plant_WERKS,
  BSEG.MENGE AS Quantity_MENGE,
  BSEG.MEINS AS BaseUnitOfMeasure_MEINS,
  BSEG.ERFMG AS QuantityInUnitOfEntry_ERFMG,
  BSEG.ERFME AS UnitOfEntry_ERFME,
  BSEG.BPMNG AS QuantityInPurchaseOrderPriceUnit_BPMNG,
  BSEG.BPRME AS OrderPriceUnit__purchasing___BPRME,
  BSEG.EBELN AS PurchasingDocumentNumber_EBELN,
  BSEG.EBELP AS ItemNumberOfPurchasingDocument_EBELP,
  BSEG.ZEKKN AS SequentialNumberOfAccountAssignment_ZEKKN,
  BSEG.ELIKZ AS deliveryCompleted_ELIKZ,
  BSEG.VPRSV AS PriceControlIndicator_VPRSV,
  BSEG.PEINH AS PriceUnit_PEINH,
  BSEG.BWKEY AS ValuationArea_BWKEY,
  BSEG.BWTAR AS ValuationType_BWTAR,
  BSEG.BUSTW AS PostingStringForValues_BUSTW,
  BSEG.REWRT AS InvoiceValueEntered__inLocalCurrency___REWRT,
  BSEG.REWWR AS InvoiceValueInForeignCurrency_REWWR,
  BSEG.BUALT AS AmountPostedInAlternativePriceControl_BUALT,
  BSEG.PSALT AS AlternativePriceControl_PSALT,
  BSEG.NPREI AS NewPrice_NPREI,
  BSEG.TBTKZ AS Indicator_SubsequentDebitcredit_TBTKZ,
  BSEG.SPGRP AS BlockingReason_Price_SPGRP,
  BSEG.SPGRM AS BlockingReason_Quantity_SPGRM,
  BSEG.SPGRT AS BlockingReason_Date_SPGRT,
  BSEG.SPGRG AS BlockingReason_OrderPriceQuantity_SPGRG,
  BSEG.SPGRV AS BlockingReason_ProjectBudget_SPGRV,
  BSEG.SPGRQ AS ManualBlockingReason_SPGRQ,
  BSEG.STCEG AS VatRegistrationNumber_STCEG,
  BSEG.EGBLD AS CountryOfDestinationForDeliveryOfGoods_EGBLD,
  BSEG.EGLLD AS SupplyingCountryForDeliveryOfGoods_EGLLD,
  BSEG.RSTGR AS ReasonCodeForPayments_RSTGR,
  BSEG.RYACQ AS YearOfAcquisition_RYACQ,
  BSEG.RPACQ AS PeriodOfAcquisition_RPACQ,
  BSEG.RDIFF AS ExchangeRateGainlossRealized_RDIFF,
  BSEG.RDIF2 AS ExchangeRateDifferenceRealizedForSecondLocalCurrency_RDIF2,
  BSEG.PRCTR AS ProfitCenter_PRCTR,
  BSEG.XHKOM AS Indicator_GlAccountAssignedManually_XHKOM,
  BSEG.VNAME AS JointVenture_VNAME,
  BSEG.RECID AS RecoveryIndicator_RECID,
  BSEG.EGRUP AS EquityGroup_EGRUP,
  BSEG.VPTNR AS PartnerAccountNumber_VPTNR,
  BSEG.VERTT AS ContractType_VERTT,
  BSEG.VERTN AS ContractNumber_VERTN,
  BSEG.VBEWA AS FlowType_VBEWA,
  BSEG.DEPOT AS SecuritiesAccount_DEPOT,
  BSEG.TXJCD AS TaxJurisdiction_TXJCD,
  BSEG.IMKEY AS InternalKeyForRealEstateObject_IMKEY,
  BSEG.DABRZ AS ReferenceDateForSettlement_DABRZ,
  BSEG.POPTS AS RealEstateOptionRate_POPTS,
  BSEG.FIPOS AS CommitmentItem_FIPOS,
  BSEG.KSTRG AS CostObject_KSTRG,
  BSEG.NPLNR AS NetworkNumberForAccountAssignment_NPLNR,
  BSEG.AUFPL AS TaskListNumberForOperationsInOrder_AUFPL,
  BSEG.APLZL AS GeneralCounterForOrder_APLZL,
  BSEG.PROJK AS WorkBreakdownStructureElement__wbsElement___PROJK,
  BSEG.PAOBJNR AS ProfitabilitySegmentNumber_PAOBJNR,
  BSEG.PASUBNR AS ProfitabilitySegmentChanges_PASUBNR,
  BSEG.SPGRS AS BlockingReason_ItemAmount_SPGRS,
  BSEG.SPGRC AS BlockingReason_Quality_SPGRC,
  BSEG.BTYPE AS PayrollType_BTYPE,
  BSEG.ETYPE AS EquityType_ETYPE,
  BSEG.XEGDR AS Indicator_TriangularDealWithinTheEu_XEGDR,
  BSEG.LNRAN AS SequenceNumberOfAssetLineItemsInFiscalYear_LNRAN,
  BSEG.HRKFT AS OriginGroupAsSubdivisionOfCostElement_HRKFT,
  BSEG.BDIF3 AS ValuationDifferenceForTheThirdLocalCurrency_BDIF3,
  BSEG.RDIF3 AS ExchangeRateDifferenceRealizedForThirdLocalCurrency_RDIF3,
  BSEG.HWMET AS MethodWithWhichTheLocalCurrencyAmountWasDetermined_HWMET,
  BSEG.GLUPM AS UpdateMethodForFmFi_GLUPM,
  BSEG.XRAGL AS Indicator_ClearingWasReversed_XRAGL,
  BSEG.UZAWE AS PaymentMethodSupplement_UZAWE,
  BSEG.LOKKT AS AlternativeAccountNumberInCompanyCode_LOKKT,
  BSEG.FISTL AS FundsCenter_FISTL,
  BSEG.GEBER AS Fund_GEBER,
  BSEG.STBUK AS TaxCompanyCode_STBUK,
  BSEG.TXBH2 AS TaxBaseoriginalTaxBaseInSecondLocalCurrency_TXBH2,
  BSEG.TXBH3 AS TaxBaseoriginalTaxBaseInThirdLocalCurrency_TXBH3,
  BSEG.PPRCT AS PartnerProfitCenter_PPRCT,
  BSEG.XREF1 AS BusinessPartnerReferenceKey_XREF1,
  BSEG.XREF2 AS BusinessPartnerReferenceKey_XREF2,
  BSEG.KBLNR AS DocumentNumberForEarmarkedFunds_KBLNR,
  BSEG.KBLPOS AS EarmarkedFunds_DocumentItem_KBLPOS,
  BSEG.FKBER AS FunctionalArea_FKBER,
  BSEG.OBZEI AS NumberOfLineItemInOriginalDocument_OBZEI,
  BSEG.XNEGP AS Indicator_NegativePosting_XNEGP,
  BSEG.RFZEI AS PaymentCardItem_RFZEI,
  BSEG.CCBTC AS PaymentCards_SettlementRun_CCBTC,
  BSEG.KKBER AS CreditControlArea_KKBER,
  BSEG.EMPFB AS Payeepayer_EMPFB,
  BSEG.XREF3 AS ReferenceKeyForLineItem_XREF3,
  BSEG.DTWS1 AS InstructionKey1_DTWS1,
  BSEG.DTWS2 AS InstructionKey2_DTWS2,
  BSEG.DTWS3 AS InstructionKey3_DTWS3,
  BSEG.DTWS4 AS InstructionKey4_DTWS4,
  BSEG.GRICD AS ActivityCodeForGrossIncomeTax_GRICD,
  BSEG.GRIRG AS Region_GRIRG,
  BSEG.GITYP AS DistributionTypeForEmploymentTax_GITYP,
  BSEG.XPYPR AS Indicator_ItemsFromPaymentProgramBlocked_XPYPR,
  BSEG.KIDNO AS PaymentReference_KIDNO,
  BSEG.ABSBT AS CreditManagement_HedgedAmount_ABSBT,
  BSEG.IDXSP AS InflationIndex_IDXSP,
  BSEG.LINFV AS LastAdjustmentDate_LINFV,
  BSEG.KONTT AS AccountAssignmentCategoryForIndustrySolution_KONTT,
  BSEG.KONTL AS AcctAssignment_KONTL,
  BSEG.TXDAT AS DateForDefiningTaxRates_TXDAT,
  BSEG.AGZEI AS ClearingItem_AGZEI,
  BSEG.PYCUR AS CurrencyForAutomaticPayment_PYCUR,
  BSEG.PYAMT AS AmountInPaymentCurrency_PYAMT,
  BSEG.BUPLA AS BusinessPlace_BUPLA,
  BSEG.SECCO AS SectionCode_SECCO,
  BSEG.LSTAR AS ActivityType_LSTAR,
  BSEG.CESSION_KZ AS AccountsReceivablePledgingIndicator_CESSION_KZ,
  BSEG.PRZNR AS BusinessProcess_PRZNR,
  BSEG.PPDIFF AS RealizedExchangeRateGainloss1_PPDIFF,
  BSEG.PPDIF2 AS RealizedExchangeRateGainloss2_PPDIF2,
  BSEG.PPDIF3 AS RealizedExchangeRateGainloss3_PPDIF3,
  BSEG.PENDAYS AS NumberOfDaysForPenaltyChargeCalculation_PENDAYS,
  BSEG.PENRC AS ReasonForLatePayment_PENRC,
  BSEG.GRANT_NBR AS Grant_GRANT_NBR,
  BSEG.SCTAX AS TaxPortionFiCaLocalCurrency_SCTAX,
  BSEG.FKBER_LONG AS FunctionalArea_FKBER_LONG,
  BSEG.GMVKZ AS ItemIsInExecution_GMVKZ,
  BSEG.SRTYPE AS TypeOfAdditionalReceivable_SRTYPE,
  BSEG.INTRENO AS InternalRealEstateMasterDataCode_INTRENO,
  BSEG.MEASURE AS FundedProgram_MEASURE,
  BSEG.AUGGJ AS FiscalYearOfClearingDocument_AUGGJ,
  BSEG.PPA_EX_IND AS PpaExcludeIndicator_PPA_EX_IND,
  BSEG.DOCLN AS SixCharacterPostingItemForLedger_DOCLN,
  BSEG.SEGMENT AS SegmentForSegmentalReporting_SEGMENT,
  BSEG.PSEGMENT AS PartnerSegmentForSegmentalReporting_PSEGMENT,
  BSEG.PFKBER AS PartnerFunctionalArea_PFKBER,
  BSEG.HKTID AS IdForAccountDetails_HKTID,
  BSEG.KSTAR AS CostElement_KSTAR,
  BSEG.XLGCLR AS ClearingSpecificToLedgerGroups_XLGCLR,
  BSEG.TAXPS AS TaxDocumentItemNumber_TAXPS,
  BSEG.PAYS_PROV AS PaymentServiceProvider_PAYS_PROV,
  BSEG.PAYS_TRAN AS PaymentReferenceOfPaymentServiceProvider_PAYS_TRAN,
  BSEG.MNDID AS UniqueReferenceToMandateForEachPayee_MNDID,
  BSEG.XFRGE_BSEG AS PaymentIsReleased_XFRGE_BSEG,
  BSEG.SQUAN AS QuantitySign_SQUAN,
  BSEG.RE_BUKRS AS CashLedger_CompanyCodeForExpenserevenue_RE_BUKRS,
  BSEG.RE_ACCOUNT AS CashLedger_ExpenseOrRevenueAccount_RE_ACCOUNT,
  BSEG.PGEBER AS PartnerFund_PGEBER,
  BSEG.PGRANT_NBR AS PartnerGrant_PGRANT_NBR,
  BSEG.BUDGET_PD AS Fm_BudgetPeriod_BUDGET_PD,
  BSEG.PBUDGET_PD AS Fm_PartnerBudgetPeriod_PBUDGET_PD,
  BSEG.J_1TPBUPL AS BranchCode_J_1TPBUPL,
  BSEG.PEROP_BEG AS BillingPeriodOfPerformanceStartDate_PEROP_BEG,
  BSEG.PEROP_END AS BillingPeriodOfPerformanceEndDate_PEROP_END,
  BSEG.FASTPAY AS PpaFastPayIndicator_FASTPAY,
  BSEG.IGNR_IVREF AS Fmfg_IgnoreTheInvoiceReferenceDuringFiDocSplitting_IGNR_IVREF,
  BSEG.FMFGUS_KEY AS UnitedStatesFederalGovernmentFields_FMFGUS_KEY,
  BSEG.FMXDOCNR AS FmReferenceDocumentNumber_FMXDOCNR,
  BSEG.FMXYEAR AS FmReferenceYear_FMXYEAR,
  BSEG.FMXDOCLN AS FmReferenceLineItem_FMXDOCLN,
  BSEG.FMXZEKKN AS FmReferenceSequenceAccountAssignment_FMXZEKKN,
  BSEG.PRODPER AS ProductionMonth__dateToFindPeriodAndYear___PRODPER,
  BSEG.RECRF AS ServiceTaxRecreditFlag_RECRF,
  --##CORTEX-CUSTOMER Consider adding other dimensions from the calendar_date_dim table as per your requirement
  CalendarDateDimension_H_BUDAT.CalYear AS YearOfPostingDateInTheDocument_BUDAT,
  CalendarDateDimension_H_BUDAT.CalMonth AS MonthOfPostingDateInTheDocument_BUDAT,
  CalendarDateDimension_H_BUDAT.CalWeek AS WeekOfPostingDateInTheDocument_BUDAT,
  CalendarDateDimension_H_BUDAT.CalQuarter AS QuarterOfPostingDateInTheDocument_BUDAT,
  CalendarDateDimension_BLDAT.CalYear AS YearOfDocumentDateInDocument_BLDAT,
  CalendarDateDimension_BLDAT.CalMonth AS MonthOfDocumentDateInDocument_BLDAT,
  CalendarDateDimension_BLDAT.CalWeek AS WeekOfDocumentDateInDocument_BLDAT,
  CalendarDateDimension_BLDAT.CalQuarter AS QuarterOfDocumentDateInDocument_BLDAT,
  COALESCE(BSEG.DMBTR * TCURXHWAER.CURRFIX, BSEG.DMBTR) AS AmountInLocalCurrency_DMBTR,
  COALESCE(BSEG.WRBTR * TCURXWAERS.CURRFIX, BSEG.WRBTR) AS AmountInDocumentCurrency_WRBTR,
  COALESCE(BSEG.KZBTR * TCURXHWAER.CURRFIX, BSEG.KZBTR) AS OriginalReductionAmountInLocalCurrency_KZBTR,
  COALESCE(BSEG.PSWBT * TCURXPSWSL.CURRFIX, BSEG.PSWBT) AS AmountForUpdatingInGeneralLedger_PSWBT,
  COALESCE(BSEG.TXBHW * TCURXHWAER.CURRFIX, BSEG.TXBHW) AS OriginalTaxBaseAmountInLocalCurrency_TXBHW,
  COALESCE(BSEG.TXBFW * TCURXWAERS.CURRFIX, BSEG.TXBFW) AS OriginalTaxBaseAmountInDocumentCurrency_TXBFW,
  COALESCE(BSEG.MWSTS * TCURXHWAER.CURRFIX, BSEG.MWSTS) AS TaxAmountInLocalCurrency_MWSTS,
  COALESCE(BSEG.WMWST * TCURXWAERS.CURRFIX, BSEG.WMWST) AS TaxAmountInDocumentCurrency_WMWST,
  COALESCE(BSEG.HWBAS * TCURXHWAER.CURRFIX, BSEG.HWBAS) AS TaxBaseAmountInLocalCurrency_HWBAS,
  COALESCE(BSEG.FWBAS * TCURXWAERS.CURRFIX, BSEG.FWBAS) AS TaxBaseAmountInDocumentCurrency_FWBAS,
  COALESCE(BSEG.HWZUZ * TCURXHWAER.CURRFIX, BSEG.HWZUZ) AS ProvisionAmountInLocalCurrency_HWZUZ,
  COALESCE(BSEG.QSSHB * TCURXWAERS.CURRFIX, BSEG.QSSHB) AS WithholdingTaxBaseAmount_QSSHB,
  COALESCE(BSEG.GBETR * TCURXHWAER.CURRFIX, BSEG.GBETR) AS HedgedAmountInForeignCurrency_GBETR,
  COALESCE(BSEG.SKFBT * TCURXWAERS.CURRFIX, BSEG.SKFBT) AS AmountEligibleForCashDiscountInDocumentCurrency_SKFBT,
  COALESCE(BSEG.SKNTO * TCURXHWAER.CURRFIX, BSEG.SKNTO) AS CashDiscountAmountInLocalCurrency_SKNTO,
  COALESCE(BSEG.WSKTO * TCURXWAERS.CURRFIX, BSEG.WSKTO) AS CashDiscountAmountInDocumentCurrency_WSKTO,
  COALESCE(BSEG.NEBTR * TCURXWAERS.CURRFIX, BSEG.NEBTR) AS NetPaymentAmount_NEBTR,
  COALESCE(BSEG.DMBT1 * TCURXHWAER.CURRFIX, BSEG.DMBT1) AS AmountInLocalCurrencyForTaxDistribution_DMBT1,
  COALESCE(BSEG.WRBT1 * TCURXHWAER.CURRFIX, BSEG.WRBT1) AS AmountInForeignCurrencyForTaxBreakdown_WRBT1,
  COALESCE(BSEG.DMBT2 * TCURXHWAE2.CURRFIX, BSEG.DMBT2) AS AmountInLocalCurrencyForTaxDistribution_DMBT2,
  COALESCE(BSEG.WRBT2 * TCURXHWAE2.CURRFIX, BSEG.WRBT2) AS AmountInForeignCurrencyForTaxBreakdown_WRBT2,
  COALESCE(BSEG.DMBT3 * TCURXHWAE3.CURRFIX, BSEG.DMBT3) AS AmountInLocalCurrencyForTaxDistribution_DMBT3,
  COALESCE(BSEG.WRBT3 * TCURXHWAE3.CURRFIX, BSEG.WRBT3) AS AmountInForeignCurrencyForTaxBreakdown_WRBT3,
  COALESCE(BSEG.KLIBT * TCURXHWAER.CURRFIX, BSEG.KLIBT) AS CreditControlAmount_KLIBT,
  COALESCE(BSEG.QBSHB * TCURXWAERS.CURRFIX, BSEG.QBSHB) AS WithholdingTaxAmount__inDocumentCurrency___QBSHB,
  COALESCE(BSEG.QSFBT * TCURXWAERS.CURRFIX, BSEG.QSFBT) AS WithholdingTaxExemptAmount_QSFBT,
  COALESCE(BSEG.BONFB * TCURXHWAER.CURRFIX, BSEG.BONFB) AS AmountQualifyingForBonusInLocalCurrency_BONFB,
  COALESCE(BSEG.DMBE2 * TCURXHWAE2.CURRFIX, BSEG.DMBE2) AS AmountInSecondLocalCurrency_DMBE2,
  --BSEG.ZZSPREG AS SpecialRegion_ZZSPREG,
  --BSEG.ZZBUSPARTN AS BusinessPartner_ZZBUSPARTN,
  --BSEG.ZZCHAN AS DistributionChannel_ZZCHAN,
  --BSEG.ZZPRODUCT AS ProductGroup_ZZPRODUCT,
  --BSEG.ZZLOCA AS City_ZZLOCA,
  --BSEG.ZZLOB AS BusinessLine_ZZLOB,
  --BSEG.ZZUSERFLD1 AS Territory_ZZUSERFLD1,
  --BSEG.ZZUSERFLD2 AS Ownercont_ZZUSERFLD2,
  --BSEG.ZZUSERFLD3 AS Vein_ZZUSERFLD3,
  --BSEG.ZZSTATE AS StateprovinceCode_ZZSTATE,
  --BSEG.ZZREGION AS Location_ZZREGION,
  COALESCE(BSEG.DMBE3 * TCURXHWAE3.CURRFIX, BSEG.DMBE3) AS AmountInThirdLocalCurrency_DMBE3,
  COALESCE(BSEG.DMB21 * TCURXHWAE2.CURRFIX, BSEG.DMB21) AS AmountInSecondLocalCurrencyForTaxBreakdown_DMB21,
  COALESCE(BSEG.DMB22 * TCURXHWAE2.CURRFIX, BSEG.DMB22) AS AmountInSecondLocalCurrencyForTaxBreakdown_DMB22,
  COALESCE(BSEG.DMB23 * TCURXHWAE2.CURRFIX, BSEG.DMB23) AS AmountInSecondLocalCurrencyForTaxBreakdown_DMB23,
  COALESCE(BSEG.DMB31 * TCURXHWAE3.CURRFIX, BSEG.DMB31) AS AmountInThirdLocalCurrencyForTaxBreakdown_DMB31,
  COALESCE(BSEG.DMB32 * TCURXHWAE3.CURRFIX, BSEG.DMB32) AS AmountInThirdLocalCurrencyForTaxBreakdown_DMB32,
  COALESCE(BSEG.DMB33 * TCURXHWAE3.CURRFIX, BSEG.DMB33) AS AmountInThirdLocalCurrencyForTaxBreakdown_DMB33,
  COALESCE(BSEG.MWST2 * TCURXHWAE2.CURRFIX, BSEG.MWST2) AS TaxAmountInSecondLocalCurrency_MWST2,
  COALESCE(BSEG.MWST3 * TCURXHWAE3.CURRFIX, BSEG.MWST3) AS TaxAmountInThirdLocalCurrency_MWST3,
  COALESCE(BSEG.NAVH2 * TCURXHWAE2.CURRFIX, BSEG.NAVH2) AS NonDeductibleInputTaxInSecondLocalCurrency_NAVH2,
  COALESCE(BSEG.NAVH3 * TCURXHWAE3.CURRFIX, BSEG.NAVH3) AS NonDeductibleInputTaxInThirdLocalCurrency_NAVH3,
  COALESCE(BSEG.SKNT2 * TCURXHWAE2.CURRFIX, BSEG.SKNT2) AS CashDiscountAmountInSecondLocalCurrency_SKNT2,
  COALESCE(BSEG.SKNT3 * TCURXHWAE3.CURRFIX, BSEG.SKNT3) AS CashDiscountAmountInThirdLocalCurrency_SKNT3,
  COALESCE(BSEG.STTAX * TCURXWAERS.CURRFIX, BSEG.STTAX) AS TaxAmountAsStatisticalInformationInDocumentCurrency_STTAX,
  COALESCE(BSEG.PENLC1 * TCURXHWAER.CURRFIX, BSEG.PENLC1) AS PenaltyChargeAmountInFirstLocalCurrency_PENLC1,
  COALESCE(BSEG.PENLC2 * TCURXHWAE2.CURRFIX, BSEG.PENLC2) AS PenaltyChargeAmountInSecondLocalCurrency_PENLC2,
  COALESCE(BSEG.PENLC3 * TCURXHWAE3.CURRFIX, BSEG.PENLC3) AS PenaltyChargeAmountInThirdLocalCurrency_PENLC3,
  COALESCE(BSEG.PENFC * TCURXWAERS.CURRFIX, BSEG.PENFC) AS PenaltyChargeAmountInDocumentCurrency_PENFC,
  --BSEG.H_BLART AS DocumentType_BLART,
  --BSEG.H_BLDAT AS DocumentDateInDocument_BLDAT,
  --BSEG.H_BSTAT AS DocumentStatus_BSTAT,
  IF(BSEG.UMSKZ = 'D', BSEG.DMBTR, 0) AS WrittenOffAmount_DMBTR,
  IF(BSEG.UMSKZ = 'D', BSEG.DMBTR, 0) AS BadDebt_DMBTR,
  IF(BSEG.AUGDT IS NULL, COALESCE(BSEG.DMBTR * TCURXHWAER.CURRFIX, BSEG.DMBTR), 0) AS AmountInLocalCurrencyClearingDate_DMBTR,
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.NetDueDateCalc`(BSEG.KOART, BSEG.ZFBDT, BKPF.BLDAT, BSEG.SHKZG, BSEG.REBZG, BSEG.ZBD3T, BSEG.ZBD2T, BSEG.ZBD1T) AS NetDueDateCalc,
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.DueDateForCashDiscount1`(BSEG.KOART, BSEG.ZFBDT, BKPF.BLDAT, BSEG.SHKZG, BSEG.REBZG, BSEG.ZBD3T, BSEG.ZBD2T, BSEG.ZBD1T) AS sk1dtCalc,
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.DueDateForCashDiscount2`(BSEG.KOART, BSEG.ZFBDT, BKPF.BLDAT, BSEG.SHKZG, BSEG.REBZG, BSEG.ZBD3T, BSEG.ZBD2T, BSEG.ZBD1T) AS sk2dtCalc,
  IF(BSEG.H_BUDAT < CURRENT_DATE() AND `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.NetDueDateCalc`(BSEG.KOART, BSEG.ZFBDT, BKPF.BLDAT, BSEG.SHKZG, BSEG.REBZG, BSEG.ZBD3T, BSEG.ZBD2T, BSEG.ZBD1T) > CURRENT_DATE()
    AND BSEG.AUGDT IS NULL, BSEG.DMBTR, 0) AS OpenAndNotDue,
  IF(BSEG.H_BUDAT < CURRENT_DATE() AND BSEG.AUGDT > `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.NetDueDateCalc`(BSEG.KOART, BSEG.ZFBDT, BKPF.BLDAT, BSEG.SHKZG, BSEG.REBZG, BSEG.ZBD3T, BSEG.ZBD2T, BSEG.ZBD1T)
    AND BSEG.AUGDT IS NOT NULL, BSEG.DMBTR, 0) AS ClearedAfterDueDate,
  IF(BSEG.H_BUDAT < CURRENT_DATE() AND BSEG.AUGDT <= `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.NetDueDateCalc`(BSEG.KOART, BSEG.ZFBDT, BKPF.BLDAT, BSEG.SHKZG, BSEG.REBZG, BSEG.ZBD3T, BSEG.ZBD2T, BSEG.ZBD1T)
    AND BSEG.AUGDT IS NOT NULL, BSEG.DMBTR, 0) AS ClearedOnOrBeforeDueDate,

  IF(BSEG.H_BUDAT < CURRENT_DATE() AND `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.NetDueDateCalc`(BSEG.KOART, BSEG.ZFBDT, BKPF.BLDAT, BSEG.SHKZG, BSEG.REBZG, BSEG.ZBD3T, BSEG.ZBD2T, BSEG.ZBD1T) < CURRENT_DATE()
    AND BSEG.AUGDT IS NULL, BSEG.DMBTR, 0) AS OpenAndOverDue,

  IF(BSEG.H_BUDAT < CURRENT_DATE() AND (DATE_DIFF(`{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.NetDueDateCalc`(BSEG.KOART, BSEG.ZFBDT, BKPF.BLDAT, BSEG.SHKZG, BSEG.REBZG, BSEG.ZBD3T, BSEG.ZBD2T, BSEG.ZBD1T), CURRENT_DATE(), DAY) > 90 )
    AND BSEG.AUGDT IS NULL, BSEG.DMBTR, 0) AS DoubtfulReceivables,
  DATE_DIFF( `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.NetDueDateCalc`(BSEG.KOART, BSEG.ZFBDT, BKPF.BLDAT, BSEG.SHKZG, BSEG.REBZG, BSEG.ZBD3T, BSEG.ZBD2T, BSEG.ZBD1T), CURRENT_DATE(), DAY)
  AS DaysInArrear,
  --## CORTEX-CUSTOMER The calculation will give positive DoubtfulReceivables(amount) for the items which are due more than 90 days from the key date.
  -- IF(BKPF.BUDAT < CURRENT_DATE() AND (DATE_DIFF(CURRENT_DATE(), `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.NetDueDateCalc`(BSEG.KOART, BSEG.ZFBDT, BKPF.BLDAT, BSEG.SHKZG, BSEG.REBZG, BSEG.ZBD3T, BSEG.ZBD2T, BSEG.ZBD1T), DAY) > 90 )
  --   AND BSEG.AUGDT IS NULL, BSEG.DMBTR, 0) AS DoubtfulReceivables,

  --## CORTEX-CUSTOMER The calculation will give positive DaysInArrear for the late payment and negative when expected on time.
  -- DATE_DIFF(CURRENT_DATE(), `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.NetDueDateCalc`(BSEG.KOART, BSEG.ZFBDT, BKPF.BLDAT, BSEG.SHKZG, BSEG.REBZG, BSEG.ZBD3T, BSEG.ZBD2T, BSEG.ZBD1T), DAY)
  -- AS DaysInArrear,

  IF(bseg.koart = 'D' AND bseg.augdt IS NULL AND bseg.H_BUDAT < CURRENT_DATE(), bseg.dmbtr, 0) AS AccountsReceivable,
  IF(bseg.koart = 'D' AND bseg.H_BUDAT < CURRENT_DATE() AND bseg.xumsw = 'X', bseg.dmbtr, 0) AS Sales
FROM BSEG AS BSEG
INNER JOIN `{{ project_id_src }}.{{ dataset_cdc_processed_s4 }}.bkpf` AS BKPF
  ON BKPF.MANDT = BSEG.MANDT
    AND BKPF.BUKRS = BSEG.BUKRS
    AND BKPF.GJAHR = BSEG.GJAHR
    AND BKPF.BELNR = BSEG.BELNR
-- Joining to this table(currency_decimal) is necesssary to fix the decimal place of
-- amounts for non-decimal-based currencies. SAP stores these amounts
-- offset by a factor  of 1/100 within the system (FYI this gets
-- corrected when a user observes these in the GUI) Currencies w/
-- decimals are unimpacted.
-- Example of impacted currencies JPY, IDR, KRW, TWD
-- Example of non-impacted currencies USD, GBP, EUR
-- Example 1,000 JPY will appear as 10.00 JPY
LEFT JOIN `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.currency_decimal` AS TCURXWAERS
  ON BKPF.WAERS = TCURXWAERS.CURRKEY
LEFT JOIN `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.currency_decimal` AS TCURXHWAER
  ON BKPF.HWAER = TCURXHWAER.CURRKEY
LEFT JOIN `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.currency_decimal` AS TCURXHWAE2
  ON BKPF.HWAE2 = TCURXHWAE2.CURRKEY
LEFT JOIN `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.currency_decimal` AS TCURXHWAE3
  ON BKPF.HWAE3 = TCURXHWAE3.CURRKEY
LEFT JOIN `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.currency_decimal` AS TCURXPSWSL
  ON BSEG.PSWSL = TCURXPSWSL.CURRKEY
LEFT JOIN `{{ project_id_src }}.{{ k9_datasets_processing }}.calendar_date_dim` AS CalendarDateDimension_H_BUDAT
  ON CalendarDateDimension_H_BUDAT.Date = BSEG.H_BUDAT
LEFT JOIN `{{ project_id_src }}.{{ k9_datasets_processing }}.calendar_date_dim` AS CalendarDateDimension_BLDAT
  ON CalendarDateDimension_BLDAT.Date = BKPF.BLDAT
