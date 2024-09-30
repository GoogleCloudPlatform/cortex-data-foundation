#-- Copyright 2022 Google LLC
#--
#-- Licensed under the Apache License, Version 2.0 (the "License");
#-- you may not use this file except in compliance with the License.
#-- You may obtain a copy of the License at
#--
#--     https://www.apache.org/licenses/LICENSE-2.0
#--
#-- Unless required by applicable law or agreed to in writing, software
#-- distributed under the License is distributed on an "AS IS" BASIS,
#-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#-- See the License for the specific language governing permissions and
#-- limitations under the License.

#-- Copyright 2022 Google LLC
#--
#-- Licensed under the Apache License, Version 2.0 (the "License");
#-- you may not use this file except in compliance with the License.
#-- You may obtain a copy of the License at
#--
#--     https://www.apache.org/licenses/LICENSE-2.0
#--
#-- Unless required by applicable law or agreed to in writing, software
#-- distributed under the License is distributed on an "AS IS" BASIS,
#-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#-- See the License for the specific language governing permissions and
#-- limitations under the License.
CREATE OR REPLACE VIEW `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.CustomerOpenItems_BSID`
OPTIONS(
  description = "Finance Accounting Customer Open Items"
)
AS
SELECT BKPF.MANDT AS Client_MANDT, BKPF.BUKRS AS CompanyCode_BUKRS, BKPF.BELNR AS AccountingDocumentNumber_BELNR, BKPF.GJAHR AS FiscalYear_GJAHR,
  BSEG.BUZEI AS NumberOfLineItemWithinAccountingDocument_BUZEI, BSEG.BUZID AS IdentificationOfTheLineItem_BUZID,
  BSEG.AUGDT AS ClearingDate_AUGDT, BSEG.AUGCP AS ClearingEntryDate_AUGCP, BSEG.AUGBL AS DocumentNumberOfTheClearingDocument_AUGBL,
  BSEG.BSCHL AS PostingKey_BSCHL, BSEG.KOART AS AccountType_KOART, BSEG.UMSKZ AS SpecialGlIndicator_UMSKZ,
  BSEG.UMSKS AS SpecialGlTransactionType_UMSKS, BKPF.BLART AS DocumentType_BLART,
  BKPF.BLDAT AS DocumentDateInDocument_BLDAT, BKPF.BUDAT AS PostingDateInTheDocument_BUDAT,
  BKPF.MONAT AS FiscalPeriod_MONAT, BKPF.CPUDT AS DayOnWhichAccountingDocumentWasEntered_CPUDT, BKPF.CPUTM AS TimeOfEntry_CPUTM,
  BKPF.AEDAT AS DateOfTheLastDocumentChangeByTransaction_AEDAT, BKPF.UPDDT AS DateOfTheLastDocumentUpdate_UPDDT,
  BKPF.WWERT AS TranslationDate_WWERT, BKPF.USNAM AS UserName_USNAM, BKPF.TCODE AS TransactionCode_TCODE,
  BKPF.BVORG AS NumberOfACrossCompanyCodePostingTransaction_BVORG, BKPF.XBLNR AS ReferenceDocumentNumber_XBLNR,
  BKPF.DBBLG AS RecurringEntryDocumentNumber_DBBLG, BKPF.STBLG AS ReverseDocumentNumber_STBLG, BKPF.STJAH AS ReverseDocumentFiscalYear_STJAH,
  BKPF.BKTXT AS DocumentHeaderText_BKTXT, BKPF.WAERS AS CurrencyKey_WAERS, BKPF.KURSF AS ExchangeRate_KURSF,
  BKPF.KZWRS AS CurrencyKeyForTheGroupCurrency_KZWRS, BKPF.KZKRS AS GroupCurrencyExchangeRate_KZKRS, BKPF.BSTAT AS DocumentStatus_BSTAT,
  BKPF.XNETB AS Indicator_DocumentPostedNet_XNETB, BKPF.FRATH AS UnplannedDeliveryCosts_FRATH,
  BKPF.XRUEB AS Indicator_DocumentIsPostedToAPreviousPeriod_XRUEB, BKPF.GLVOR AS BusinessTransaction_GLVOR,
  BKPF.GRPID AS BatchInputSessionName_GRPID, BKPF.DOKID AS DocumentNameInTheArchiveSystem_DOKID, BKPF.ARCID AS ExtractIdDocumentHeader_ARCID,
  BKPF.IBLAR AS InternalDocumentTypeForDocumentControl_IBLAR, BKPF.AWTYP AS ReferenceProcedure_AWTYP, BKPF.AWKEY AS ObjectKey_AWKEY,
  BKPF.FIKRS AS FinancialManagementArea_FIKRS, BKPF.HWAER AS LocalCurrency_HWAER, BKPF.HWAE2 AS CurrencyKeyOfSecondLocalCurrency_HWAE2,
  BKPF.HWAE3 AS CurrencyKeyOfThirdLocalCurrency_HWAE3, BKPF.KURS2 AS ExchangeRateForTheSecondLocalCurrency_KURS2,
  BKPF.KURS3 AS ExchangeRateForTheThirdLocalCurrency_KURS3, BKPF.BASW2 AS SourceCurrencyForCurrencyTranslation_BASW2,
  BKPF.BASW3 AS SourceCurrencyForCurrencyTranslation_BASW3, BKPF.UMRD2 AS TranslationDateTypeForSecondLocalCurrency_UMRD2,
  BKPF.UMRD3 AS TranslationDateTypeForThirdLocalCurrency_UMRD3, BKPF.XSTOV AS Indicator_DocumentIsFlaggedForReversal_XSTOV,
  BKPF.STODT AS PlannedDateForTheReversePosting_STODT, BKPF.XMWST AS CalculateTaxAutomatically_XMWST,
  BKPF.CURT2 AS CurrencyTypeOfSecondLocalCurrency_CURT2, BKPF.CURT3 AS CurrencyTypeOfThirdLocalCurrency_CURT3,
  BKPF.KUTY2 AS ExchangeRateType_KUTY2, BKPF.KUTY3 AS ExchangeRateType_KUTY3, BKPF.XSNET AS GlAccountAmountsEnteredExcludeTax_XSNET,
  BKPF.AUSBK AS SourceCompanyCode_AUSBK, BKPF.DUEFL AS StatusOfDataTransferIntoSubsequentRelease_DUEFL, BKPF.AWSYS AS LogicalSystem_AWSYS, BKPF.TXKRS AS ExchangeRateForTaxes_TXKRS,
  BKPF.CTXKRS AS RateForTaxValuesInLocalCurrency__plantsAbroad___CTXKRS, BKPF.LOTKZ AS LotNumberForRequests_LOTKZ,
  BKPF.XWVOF AS Indicator_CustomerBillOfExchangePaymentBeforeDueDate_XWVOF, BKPF.STGRD AS ReasonForReversal_STGRD,
  BKPF.PPNAM AS NameOfUserWhoParkedThisDocument_PPNAM, BKPF.BRNCH AS BranchNumber_BRNCH, BKPF.NUMPG AS NumberOfPagesOfInvoice_NUMPG,
  BKPF.ADISC AS Indicator_EntryRepresentsADiscountDocument_ADISC, BKPF.XREF1_HD AS ReferenceKey1InternalForDocumentHeader_XREF1_HD,
  BKPF.XREF2_HD AS ReferenceKey2InternalForDocumentHeader_XREF2_HD, BKPF.XREVERSAL AS SpecifiesWhetherDocIsReversalDocOrReversedDoc_XREVERSAL,
  BKPF.REINDAT AS InvoiceReceiptDate_REINDAT, BKPF.RLDNR AS LedgerInGeneralLedgerAccounting_RLDNR, BKPF.LDGRP AS LedgerGroup_LDGRP,
  BKPF.PROPMANO AS RealEstateManagementMandate_PROPMANO, BKPF.XBLNR_ALT AS AlternativeReferenceNumber_XBLNR_ALT,
  BKPF.VATDATE AS TaxReportingDate_VATDATE, BKPF.DOCCAT AS ClassificationOfAnFiDocument_DOCCAT,
  BKPF.XSPLIT AS FiDocumentOriginatesFromSplitPosting__indicator___XSPLIT, BKPF.CASH_ALLOC AS CashRelevantDocument_CASH_ALLOC,
  BKPF.FOLLOW_ON AS FollowOnDocumentIndicator_FOLLOW_ON, BKPF.XREORG AS DocContainsOpenItemThatWasTransferredDuringReorg_XREORG,
  BKPF.SUBSET AS DefinesSubsetOfComponentsForTheFicoInterface_SUBSET, BKPF.KURST AS ExchangeRateType_KURST,
  BKPF.KURSX AS MarketDataExchangeRate_KURSX, BKPF.KUR2X AS MarketDataExchangeRate2_KUR2X, BKPF.KUR3X AS MarketDataExchangeRate3_KUR3X,
  BKPF.XMCA AS DocumentOriginatesFromMultiCurrencyAccounting_XMCA, BKPF.RESUBMISSION AS DateOfResubmission_RESUBMISSION,
  BKPF.PSOTY AS DocumentCategoryPaymentRequests_PSOTY, BKPF.PSOAK AS Reason_PSOAK,
  BKPF.PSOKS AS Region_PSOKS, BKPF.PSOSG AS ReasonForReversalIsPsRequests_PSOSG, BKPF.PSOFN AS IsPs_FileNumber_PSOFN,
  BKPF.INTFORM AS InterestFormula_INTFORM, BKPF.INTDATE AS InterestCalcDate_INTDATE, BKPF.PSOBT AS PostingDay_PSOBT,
  BKPF.PSOZL AS ActualPosting_PSOZL, BKPF.PSODT AS DateOfLastChange_PSODT, BKPF.PSOTM AS LastChangedAt_PSOTM,
  BKPF.FM_UMART AS TypeOfPaymentTransfer_FM_UMART, BKPF.CCINS AS PaymentCards_CardType_CCINS, BKPF.CCNUM AS PaymentCards_CardNumber_CCNUM,
  BKPF.SSBLK AS PaymentStatisticalSamplingBlock_SSBLK, BKPF.BATCH AS LotNumberForDocuments_BATCH, BKPF.SNAME AS UserName_SNAME,
  BKPF.SAMPLED AS SampledInvoiceByPaymentCertification_SAMPLED, BKPF.EXCLUDE_FLAG AS PpaExcludeIndicator_EXCLUDE_FLAG,
  BKPF.BLIND AS BudgetaryLedgerIndicator_BLIND, BKPF.OFFSET_STATUS AS TreasuryOffsetStatus_OFFSET_STATUS,
  BKPF.OFFSET_REFER_DAT AS DateRecordReferredToTreasury_OFFSET_REFER_DAT,
  BKPF.KNUMV AS NumberOfTheDocumentCondition_KNUMV,
  BSEG.ZUMSK AS TargetSpecialGlIndicator_ZUMSK, BSEG.SHKZG AS DebitcreditIndicator_SHKZG,
  BSEG.GSBER AS BusinessArea_GSBER, BSEG.PARGB AS TradingPartnerBusinessArea_PARGB, BSEG.MWSKZ AS TaxOnSalespurchasesCode_MWSKZ,
  BSEG.QSSKZ AS WithholdingTaxCode_QSSKZ,
  BSEG.KZBTR AS OriginalReductionAmountInLocalCurrency_KZBTR,
  BSEG.PSWBT AS AmountForUpdatingInGeneralLedger_PSWBT,
  BSEG.PSWSL AS UpdateCurrencyForGeneralLedgerTransactionFigures_PSWSL, BSEG.TXBHW AS OriginalTaxBaseAmountInLocalCurrency_TXBHW,
  BSEG.TXBFW AS OriginalTaxBaseAmountInDocumentCurrency_TXBFW, BSEG.MWSTS AS TaxAmountInLocalCurrency_MWSTS,
  BSEG.WMWST AS TaxAmountInDocumentCurrency_WMWST, BSEG.HWBAS AS TaxBaseAmountInLocalCurrency_HWBAS,
  BSEG.FWBAS AS TaxBaseAmountInDocumentCurrency_FWBAS, BSEG.HWZUZ AS ProvisionAmountInLocalCurrency_HWZUZ,
  BSEG.FWZUZ AS AdditionalTaxInDocumentCurrency_FWZUZ, BSEG.SHZUZ AS DebitcreditAdditionForCashDiscount_SHZUZ,
  BSEG.STEKZ AS VersionNumberComponent_STEKZ, BSEG.MWART AS TaxType_MWART,
  BSEG.TXGRP AS GroupIndicatorForTaxLineItems_TXGRP, BSEG.KTOSL AS TransactionKey_KTOSL, BSEG.QSSHB AS WithholdingTaxBaseAmount_QSSHB,
  BSEG.KURSR AS HedgedExchangeRate_KURSR, BSEG.GBETR AS HedgedAmountInForeignCurrency_GBETR, BSEG.BDIFF AS ValuationDifference_BDIFF,
  BSEG.BDIF2 AS ValuationDifferenceForTheSecondLocalCurrency_BDIF2, BSEG.VALUT AS ValueDate_VALUT,
  BSEG.ZUONR AS AssignmentNumber_ZUONR, BSEG.SGTXT AS ItemText_SGTXT, BSEG.ZINKZ AS ExemptedFromInterestCalculation_ZINKZ,
  BSEG.VBUND AS CompanyIdOfTradingPartner_VBUND, BSEG.BEWAR AS TransactionType_BEWAR, BSEG.ALTKT AS GroupAccountNumber_ALTKT,
  BSEG.VORGN AS TransactionTypeForGeneralLedger_VORGN, BSEG.FDLEV AS PlanningLevel_FDLEV, BSEG.FDGRP AS PlanningGroup_FDGRP,
  BSEG.FDWBT AS PlannedAmountInDocumentOrGlAccountCurrency_FDWBT, BSEG.FDTAG AS PlanningDate_FDTAG, BSEG.FKONT AS FinancialBudgetItem_FKONT,
  BSEG.KOKRS AS ControllingArea_KOKRS, BSEG.KOSTL AS CostCenter_KOSTL, BSEG.PROJN AS Old_ProjectNumberPs_posnr_PROJN,
  BSEG.AUFNR AS OrderNumber_AUFNR, BSEG.VBELN AS BillingDocument_VBELN, BSEG.VBEL2 AS SalesDocument_VBEL2,
  BSEG.POSN2 AS SalesDocumentItem_POSN2, BSEG.ETEN2 AS ScheduleLineNumber_ETEN2, BSEG.ANLN1 AS MainAssetNumber_ANLN1,
  BSEG.ANLN2 AS AssetSubnumber_ANLN2, BSEG.ANBWA AS AssetTransactionType_ANBWA, BSEG.BZDAT AS AssetValueDate_BZDAT,
  BSEG.PERNR AS PersonnelNumber_PERNR, BSEG.XUMSW AS Indicator_SalesRelatedItem_XUMSW, BSEG.XHRES AS Indicator_ResidentGlAccount_XHRES,
  BSEG.XKRES AS Indicator_CanLineItemsBeDisplayedByAccount_XKRES, BSEG.XOPVW AS Indicator_OpenItemManagement_XOPVW,
  BSEG.XCPDD AS Indicator_AddressAndBankDataSetIndividually_XCPDD, BSEG.XSKST AS Indicator_StatisticalPostingToCostCenter_XSKST,
  BSEG.XSAUF AS Indicator_PostingToOrderIsStatistical_XSAUF, BSEG.XSPRO AS Indicator_PostingToProjectIsStatistical_XSPRO,
  BSEG.XSERG AS Indicator_PostingToProfitabilityAnalysisIsStatistical_XSERG, BSEG.XFAKT AS Indicator_BillingDocumentUpdateSuccessful_XFAKT,
  BSEG.XUMAN AS Indicator_TransferPostingFromDownPayment_XUMAN, BSEG.XANET AS Indicator_DownPaymentInNetProcedure_XANET,
  BSEG.XSKRL AS Indicator_LineItemNotLiableToCashDiscount_XSKRL, BSEG.XINVE AS Indicator_CapitalGoodsAffected_XINVE,
  BSEG.XPANZ AS DisplayItem_XPANZ, BSEG.XAUTO AS Indicator_LineItemAutomaticallyCreated_XAUTO,
  BSEG.XNCOP AS Indicator_ItemsCannotBeCopied_XNCOP, BSEG.XZAHL AS Indicator_IsPostingKeyUsedInAPaymentTransaction_XZAHL, BSEG.SAKNR AS GlAccountNumber_SAKNR,
  BSEG.HKONT AS GeneralLedgerAccount_HKONT, BSEG.KUNNR AS CustomerNumber_KUNNR,
  BSEG.LIFNR AS AccountNumberOfVendorOrCreditor_LIFNR, BSEG.FILKD AS AccountNumberOfTheBranch_FILKD, BSEG.XBILK AS Indicator_AccountIsABalanceSheetAccount_XBILK,
  BSEG.GVTYP AS PLStatementAccountType_GVTYP, BSEG.HZUON AS AssignmentNumberForSpecialGlAccounts_HZUON,
  BSEG.ZFBDT AS BaselineDateForDueDateCalculation_ZFBDT, BSEG.ZTERM AS TermsOfPaymentKey_ZTERM,
  BSEG.ZBD1T AS CashDiscountDays1_ZBD1T, BSEG.ZBD2T AS CashDiscountDays2_ZBD2T, BSEG.ZBD3T AS NetPaymentTermsPeriod_ZBD3T,
  BSEG.ZBD1P AS CashDiscountPercentage1_ZBD1P, BSEG.ZBD2P AS CashDiscountPercentage2_ZBD2P, BSEG.SKFBT AS AmountEligibleForCashDiscountInDocumentCurrency_SKFBT,
  BSEG.SKNTO AS CashDiscountAmountInLocalCurrency_SKNTO, BSEG.WSKTO AS CashDiscountAmountInDocumentCurrency_WSKTO,
  BSEG.ZLSCH AS PaymentMethod_ZLSCH, BSEG.ZLSPR AS PaymentBlockKey_ZLSPR,
  BSEG.ZBFIX AS FixedPaymentTerms_ZBFIX, BSEG.HBKID AS ShortKeyForAHouseBank_HBKID, BSEG.BVTYP AS PartnerBankType_BVTYP,
  BSEG.NEBTR AS NetPaymentAmount_NEBTR, BSEG.MWSK1 AS TaxCodeForDistribution_MWSK1, BSEG.DMBT1 AS AmountInLocalCurrencyForTaxDistribution_DMBT1,
  BSEG.WRBT1 AS AmountInForeignCurrencyForTaxBreakdown_WRBT1, BSEG.MWSK2 AS TaxCodeForDistribution_MWSK2,
  BSEG.DMBT2 AS AmountInLocalCurrencyForTaxDistribution_DMBT2, BSEG.WRBT2 AS AmountInForeignCurrencyForTaxBreakdown_WRBT2,
  BSEG.MWSK3 AS TaxCodeForDistribution_MWSK3, BSEG.DMBT3 AS AmountInLocalCurrencyForTaxDistribution_DMBT3,
  BSEG.WRBT3 AS AmountInForeignCurrencyForTaxBreakdown_WRBT3, BSEG.REBZG AS DocumentNoOfTheInvoiceToWhichTheTransactionBelongs_REBZG,
  BSEG.REBZJ AS FiscalYearOfTheRelevantInvoice__forCreditMemo___REBZJ, BSEG.REBZZ AS LineItemInTheRelevantInvoice_REBZZ,
  BSEG.REBZT AS FollowOnDocumentType_REBZT, BSEG.ZOLLT AS CustomsTariffNumber_ZOLLT,
  BSEG.ZOLLD AS CustomsDate_ZOLLD, BSEG.LZBKZ AS StateCentralBankIndicator_LZBKZ, BSEG.LANDL AS SupplyingCountry_LANDL,
  BSEG.DIEKZ AS ServiceIndicator__foreignPayment___DIEKZ, BSEG.SAMNR AS InvoiceListNumber_SAMNR, BSEG.ABPER AS SettlementPeriod_ABPER,
  BSEG.VRSKZ AS InsuranceIndicator_VRSKZ, BSEG.VRSDT AS InsuranceDate_VRSDT, BSEG.DISBN AS NumberOfBillOfExchangeUsageDocument__discountDoc___DISBN,
  BSEG.DISBJ AS FiscalYearOfBillOfExchangeUsageDocument_DISBJ, BSEG.DISBZ AS LineItemWithinTheBillOfExchangeUsageDocument_DISBZ,
  BSEG.WVERW AS BillOfExchangeUsageType_WVERW, BSEG.ANFBN AS DocumentNumberOfTheBillOfExchangePaymentRequest_ANFBN,
  BSEG.ANFBJ AS FiscalYearOfTheBillOfExchangePaymentRequestDocument_ANFBJ, BSEG.ANFBU AS CompanyCodeInWhichBillOfExchPaymentRequestIsPosted_ANFBU,
  BSEG.ANFAE AS BillOfExchangePaymentRequestDueDate_ANFAE, BSEG.BLNBT AS BaseAmountForDeterminingThePreferenceAmount_BLNBT,
  BSEG.BLNKZ AS SubsidyIndicatorForDeterminingTheReductionRates_BLNKZ, BSEG.BLNPZ AS PreferencePercentageRate_BLNPZ,
  BSEG.MSCHL AS DunningKey_MSCHL, BSEG.MANSP AS DunningBlock_MANSP,
  BSEG.MADAT AS DateOfLastDunningNotice_MADAT, BSEG.MANST AS DunningLevel_MANST, BSEG.MABER AS DunningArea_MABER,
  BSEG.ESRNR AS PorSubscriberNumber_ESRNR, BSEG.ESRRE AS PorReferenceNumber_ESRRE, BSEG.ESRPZ AS PorCheckDigit_ESRPZ,
  BSEG.KLIBT AS CreditControlAmount_KLIBT, BSEG.QSZNR AS CertificateNumberOfTheWithholdingTaxExemption_QSZNR, BSEG.QBSHB AS WithholdingTaxAmount__inDocumentCurrency___QBSHB,
  BSEG.QSFBT AS WithholdingTaxExemptAmount__inDocumentCurrency___QSFBT, BSEG.NAVHW AS NonDeductibleInputTax__inLocalCurrency___NAVHW,
  BSEG.NAVFW AS NonDeductibleInputTax__inDocumentCurrency___NAVFW, BSEG.MATNR AS MaterialNumber_MATNR,
  BSEG.WERKS AS Plant_WERKS, BSEG.MENGE AS Quantity_MENGE, BSEG.MEINS AS BaseUnitOfMeasure_MEINS,
  BSEG.ERFMG AS QuantityInUnitOfEntry_ERFMG, BSEG.ERFME AS UnitOfEntry_ERFME, BSEG.BPMNG AS QuantityInPurchaseOrderPriceUnit_BPMNG,
  BSEG.BPRME AS OrderPriceUnit__purchasing___BPRME, BSEG.EBELN AS PurchasingDocumentNumber_EBELN, BSEG.EBELP AS ItemNumberOfPurchasingDocument_EBELP,
  BSEG.ZEKKN AS SequentialNumberOfAccountAssignment_ZEKKN, BSEG.ELIKZ AS DeliveryCompletedIndicator_ELIKZ,
  BSEG.VPRSV AS PriceControlIndicator_VPRSV, BSEG.PEINH AS PriceUnit_PEINH,
  BSEG.BWKEY AS ValuationArea_BWKEY, BSEG.BWTAR AS ValuationType_BWTAR, BSEG.BUSTW AS PostingStringForValues_BUSTW,
  BSEG.REWRT AS InvoiceValueEntered__inLocalCurrency___REWRT, BSEG.REWWR AS InvoiceValueInForeignCurrency_REWWR, BSEG.BONFB AS AmountQualifyingForBonusInLocalCurrency_BONFB,
  BSEG.BUALT AS AmountPostedInAlternativePriceControl_BUALT, BSEG.PSALT AS AlternativePriceControl_PSALT,
  BSEG.NPREI AS NewPrice_NPREI, BSEG.TBTKZ AS Indicator_SubsequentDebitcredit_TBTKZ, BSEG.SPGRP AS BlockingReason_Price_SPGRP,
  BSEG.SPGRM AS BlockingReason_Quantity_SPGRM, BSEG.SPGRT AS BlockingReason_Date_SPGRT, BSEG.SPGRG AS BlockingReason_OrderPriceQuantity_SPGRG,
  BSEG.SPGRV AS BlockingReason_ProjectBudget_SPGRV, BSEG.SPGRQ AS ManualBlockingReason_SPGRQ,
  BSEG.STCEG AS VatRegistrationNumber_STCEG, BSEG.EGBLD AS CountryOfDestinationForDeliveryOfGoods_EGBLD, BSEG.EGLLD AS SupplyingCountryForDeliveryOfGoods_EGLLD,
  BSEG.RSTGR AS ReasonCodeForPayments_RSTGR, BSEG.RYACQ AS YearOfAcquisition_RYACQ,
  BSEG.RPACQ AS PeriodOfAcquisition_RPACQ, BSEG.RDIFF AS ExchangeRateGainlossRealized_RDIFF, BSEG.RDIF2 AS ExchangeRateDifferenceRealizedForSecondLocalCurrency_RDIF2,
  BSEG.PRCTR AS ProfitCenter_PRCTR, BSEG.XHKOM AS Indicator_GlAccountAssignedManually_XHKOM,
  BSEG.VNAME AS JointVenture_VNAME, BSEG.RECID AS RecoveryIndicator_RECID, BSEG.EGRUP AS EquityGroup_EGRUP,
  BSEG.VPTNR AS PartnerAccountNumber_VPTNR, BSEG.VERTT AS ContractType_VERTT, BSEG.VERTN AS ContractNumber_VERTN,
  BSEG.VBEWA AS FlowType_VBEWA, BSEG.DEPOT AS SecuritiesAccount_DEPOT, BSEG.TXJCD AS TaxJurisdiction_TXJCD, BSEG.IMKEY AS InternalKeyForRealEstateObject_IMKEY,
  BSEG.DABRZ AS ReferenceDateForSettlement_DABRZ, BSEG.POPTS AS RealEstateOptionRate_POPTS, BSEG.FIPOS AS CommitmentItem_FIPOS,
  BSEG.KSTRG AS CostObject_KSTRG, BSEG.NPLNR AS NetworkNumberForAccountAssignment_NPLNR, BSEG.AUFPL AS TaskListNumberForOperationsInOrder_AUFPL,
  BSEG.APLZL AS GeneralCounterForOrder_APLZL, BSEG.PROJK AS WorkBreakdownStructureElement__wbsElement___PROJK,
  BSEG.PAOBJNR AS ProfitabilitySegmentNumber__coPa___PAOBJNR, BSEG.PASUBNR AS ProfitabilitySegmentChanges__coPa___PASUBNR,
  BSEG.SPGRS AS BlockingReason_ItemAmount_SPGRS, BSEG.SPGRC AS BlockingReason_Quality_SPGRC,
  BSEG.BTYPE AS PayrollType_BTYPE, BSEG.ETYPE AS EquityType_ETYPE, BSEG.XEGDR AS Indicator_TriangularDealWithinTheEu_XEGDR,
  BSEG.LNRAN AS SequenceNumberOfAssetLineItemsInFiscalYear_LNRAN, BSEG.HRKFT AS OriginGroupAsSubdivisionOfCostElement_HRKFT,
  BSEG.DMBE2 AS AmountInSecondLocalCurrency_DMBE2, BSEG.DMBE3 AS AmountInThirdLocalCurrency_DMBE3,
  BSEG.DMB21 AS AmountInSecondLocalCurrencyForTaxBreakdown_DMB21, BSEG.DMB22 AS AmountInSecondLocalCurrencyForTaxBreakdown_DMB22,
  BSEG.DMB23 AS AmountInSecondLocalCurrencyForTaxBreakdown_DMB23, BSEG.DMB31 AS AmountInThirdLocalCurrencyForTaxBreakdown_DMB31,
  BSEG.DMB32 AS AmountInThirdLocalCurrencyForTaxBreakdown_DMB32, BSEG.DMB33 AS AmountInThirdLocalCurrencyForTaxBreakdown_DMB33,
  BSEG.MWST2 AS TaxAmountInSecondLocalCurrency_MWST2, BSEG.MWST3 AS TaxAmountInThirdLocalCurrency_MWST3,
  BSEG.NAVH2 AS NonDeductibleInputTaxInSecondLocalCurrency_NAVH2, BSEG.NAVH3 AS NonDeductibleInputTaxInThirdLocalCurrency_NAVH3,
  BSEG.SKNT2 AS CashDiscountAmountInSecondLocalCurrency_SKNT2, BSEG.SKNT3 AS CashDiscountAmountInThirdLocalCurrency_SKNT3,
  BSEG.BDIF3 AS ValuationDifferenceForTheThirdLocalCurrency_BDIF3, BSEG.RDIF3 AS ExchangeRateDifferenceRealizedForThirdLocalCurrency_RDIF3,
  BSEG.HWMET AS MethodWithWhichTheLocalCurrencyAmountWasDetermined_HWMET, BSEG.GLUPM AS UpdateMethodForFmFiCaIntegration_GLUPM,
  BSEG.XRAGL AS Indicator_ClearingWasReversed_XRAGL, BSEG.UZAWE AS PaymentMethodSupplement_UZAWE,
  BSEG.LOKKT AS AlternativeAccountNumberInCompanyCode_LOKKT, BSEG.FISTL AS FundsCenter_FISTL,
  BSEG.GEBER AS Fund_GEBER, BSEG.STBUK AS TaxCompanyCode_STBUK, BSEG.TXBH2 AS TaxBaseoriginalTaxBaseInSecondLocalCurrency_TXBH2,
  BSEG.TXBH3 AS TaxBaseoriginalTaxBaseInThirdLocalCurrency_TXBH3, BSEG.PPRCT AS PartnerProfitCenter_PPRCT,
  BSEG.XREF1 AS BusinessPartnerReferenceKey_XREF1, BSEG.XREF2 AS BusinessPartnerReferenceKey_XREF2,
  BSEG.KBLNR AS DocumentNumberForEarmarkedFunds_KBLNR, BSEG.KBLPOS AS EarmarkedFunds_DocumentItem_KBLPOS,
  BSEG.STTAX AS TaxAmountAsStatisticalInformationInDocumentCurrency_STTAX, BSEG.FKBER AS FunctionalArea_FKBER,
  BSEG.OBZEI AS NumberOfLineItemInOriginalDocument_OBZEI, BSEG.XNEGP AS Indicator_NegativePosting_XNEGP,
  BSEG.RFZEI AS PaymentCardItem_RFZEI, BSEG.CCBTC AS PaymentCards_SettlementRun_CCBTC, BSEG.KKBER AS CreditControlArea_KKBER,
  BSEG.EMPFB AS Payeepayer_EMPFB, BSEG.XREF3 AS ReferenceKeyForLineItem_XREF3, BSEG.DTWS1 AS InstructionKey1_DTWS1,
  BSEG.DTWS2 AS InstructionKey2_DTWS2, BSEG.DTWS3 AS InstructionKey3_DTWS3, BSEG.DTWS4 AS InstructionKey4_DTWS4,
  BSEG.GRICD AS ActivityCodeForGrossIncomeTax_GRICD, BSEG.GRIRG AS Region_GRIRG, BSEG.GITYP AS DistributionTypeForEmploymentTax_GITYP,
  BSEG.XPYPR AS Indicator_ItemsFromPaymentProgramBlocked_XPYPR, BSEG.KIDNO AS PaymentReference_KIDNO,
  BSEG.ABSBT AS CreditManagement_HedgedAmount_ABSBT, BSEG.IDXSP AS InflationIndex_IDXSP,
  BSEG.LINFV AS LastAdjustmentDate_LINFV, BSEG.KONTT AS AccountAssignmentCategoryForIndustrySolution_KONTT, BSEG.KONTL AS AcctAssignmentStringForIndustrySpecificAcctAssignmnts_KONTL,
  BSEG.TXDAT AS DateForDefiningTaxRates_TXDAT, BSEG.AGZEI AS ClearingItem_AGZEI,
  BSEG.PYCUR AS CurrencyForAutomaticPayment_PYCUR, BSEG.PYAMT AS AmountInPaymentCurrency_PYAMT, BSEG.BUPLA AS BusinessPlace_BUPLA,
  BSEG.SECCO AS SectionCode_SECCO, BSEG.LSTAR AS ActivityType_LSTAR, BSEG.CESSION_KZ AS AccountsReceivablePledgingIndicator_CESSION_KZ,
  BSEG.PRZNR AS BusinessProcess_PRZNR, BSEG.PENLC2 AS PenaltyChargeAmountInSecondLocalCurrency_PENLC2, BSEG.PENLC3 AS PenaltyChargeAmountInThirdLocalCurrency_PENLC3,
  BSEG.PENFC AS PenaltyChargeAmountInDocumentCurrency_PENFC, BSEG.PENDAYS AS NumberOfDaysForPenaltyChargeCalculation_PENDAYS,
  BSEG.PENRC AS ReasonForLatePayment_PENRC, BSEG.GRANT_NBR AS Grant_GRANT_NBR,
  BSEG.SCTAX AS TaxPortionFiCaLocalCurrency_SCTAX, BSEG.FKBER_LONG AS FunctionalArea_FKBER_LONG, BSEG.GMVKZ AS ItemIsInExecution_GMVKZ,
  BSEG.SRTYPE AS TypeOfAdditionalReceivable_SRTYPE, BSEG.INTRENO AS InternalRealEstateMasterDataCode_INTRENO, BSEG.MEASURE AS FundedProgram_MEASURE,
  BSEG.AUGGJ AS FiscalYearOfClearingDocument_AUGGJ, BSEG.PPA_EX_IND AS PpaExcludeIndicator_PPA_EX_IND,
  BSEG.DOCLN AS SixCharacterPostingItemForLedger_DOCLN, BSEG.SEGMENT AS SegmentForSegmentalReporting_SEGMENT,
  BSEG.PSEGMENT AS PartnerSegmentForSegmentalReporting_PSEGMENT, BSEG.PFKBER AS PartnerFunctionalArea_PFKBER,
  BSEG.HKTID AS IdForAccountDetails_HKTID, BSEG.KSTAR AS CostElement_KSTAR,
  BSEG.XLGCLR AS ClearingSpecificToLedgerGroups_XLGCLR, BSEG.TAXPS AS TaxDocumentItemNumber_TAXPS, BSEG.PAYS_PROV AS PaymentServiceProvider_PAYS_PROV,
  BSEG.PAYS_TRAN AS PaymentReferenceOfPaymentServiceProvider_PAYS_TRAN, BSEG.MNDID AS UniqueReferenceToMandateForEachPayee_MNDID,
  BSEG.XFRGE_BSEG AS PaymentIsReleased_XFRGE_BSEG, BSEG.SQUAN AS QuantitySign_SQUAN,
  BSEG.RE_BUKRS AS CashLedger_CompanyCodeForExpenserevenue_RE_BUKRS,
  BSEG.RE_ACCOUNT AS CashLedger_ExpenseOrRevenueAccount_RE_ACCOUNT, BSEG.PGEBER AS PartnerFund_PGEBER,
  BSEG.PGRANT_NBR AS PartnerGrant_PGRANT_NBR, BSEG.BUDGET_PD AS Fm_BudgetPeriod_BUDGET_PD,
  BSEG.PBUDGET_PD AS Fm_PartnerBudgetPeriod_PBUDGET_PD, BSEG.J_1TPBUPL AS BranchCode_J_1TPBUPL, BSEG.PEROP_BEG AS BillingPeriodOfPerformanceStartDate_PEROP_BEG,
  BSEG.PEROP_END AS BillingPeriodOfPerformanceEndDate_PEROP_END, BSEG.FASTPAY AS PpaFastPayIndicator_FASTPAY,
  BSEG.IGNR_IVREF AS Fmfg_IgnoreTheInvoiceReferenceDuringFiDocSplitting_IGNR_IVREF, BSEG.FMFGUS_KEY AS UnitedStatesFederalGovernmentFields_FMFGUS_KEY,
  BSEG.FMXDOCNR AS FmReferenceDocumentNumber_FMXDOCNR,
  BSEG.FMXYEAR AS FmReferenceYear_FMXYEAR, BSEG.FMXDOCLN AS FmReferenceLineItem_FMXDOCLN,
  BSEG.FMXZEKKN AS FmReferenceSequenceAccountAssignment_FMXZEKKN, BSEG.PRODPER AS ProductionMonth__dateToFindPeriodAndYear___PRODPER,
  CASE
    WHEN BSEG.shkzg = 'S' THEN BSEG.dmbtr  -- S = Debit | Amount in document currency
    WHEN BSEG.shkzg = 'H' THEN BSEG.dmbtr * -1  -- H = Credit | Amount in document currency
    ELSE BSEG.dmbtr
  END AS AmountInLocalCurrency_DMBTR,
  CASE
    WHEN BSEG.shkzg = 'S' THEN BSEG.wrbtr  -- S = Debit | Amount in document currency
    WHEN BSEG.shkzg = 'H' THEN BSEG.wrbtr * -1  -- H = Credit | Amount in document currency
    ELSE BSEG.wrbtr
  END AS AmountInDocumentCurrency_WRBTR
FROM `{{ project_id_src }}.{{ dataset_cdc_processed }}.bkpf` AS bkpf
LEFT OUTER JOIN `{{ project_id_src }}.{{ dataset_cdc_processed }}.bseg` AS bseg
  ON bkpf.mandt = bseg.mandt
    AND bkpf.bukrs = bseg.bukrs
    AND bkpf.gjahr = bseg.gjahr
    AND bkpf.belnr = bseg.belnr
WHERE bseg.koart = 'D'
  AND bseg.augbl IS NULL
  {% if sql_flavour.upper() == 'S4' %}
    AND bseg.h_bstat != 'D'
    AND bseg.h_bstat != 'M'
  {% endif %}


