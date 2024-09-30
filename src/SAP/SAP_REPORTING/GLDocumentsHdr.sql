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
SELECT
  FAGLFLEXA.RCLNT AS Client_RCLNT, FAGLFLEXA.RYEAR AS FiscalYear_RYEAR,
  FAGLFLEXA.DOCNR AS AccountingDocumentNumber_DOCNR,
  FAGLFLEXA.RLDNR AS LedgerInGeneralLedgerAccounting_RLDNR,
  FAGLFLEXA.RBUKRS AS CompanyCode_RBUKRS,
  FAGLFLEXA.DOCLN AS SixCharacterPostingItemForLedger_DOCLN,
  FAGLFLEXA.ACTIV AS FiSlBusinessTransaction_ACTIV, FAGLFLEXA.RMVCT AS TransactionType_RMVCT, FAGLFLEXA.RTCUR AS CurrencyKey_RTCUR,
  FAGLFLEXA.RUNIT AS BaseUnitOfMeasure_RUNIT,
  FAGLFLEXA.AWTYP AS ReferenceProcedure_AWTYP,
  FAGLFLEXA.RRCTY AS RecordType_RRCTY,
  FAGLFLEXA.RVERS AS Version_RVERS,
  FAGLFLEXA.LOGSYS AS LogicalSystem_LOGSYS,
  FAGLFLEXA.RACCT AS AccountNumber_RACCT,
  FAGLFLEXA.COST_ELEM AS CostElement_COST_ELEM,
  FAGLFLEXA.RCNTR AS CostCenter_RCNTR, FAGLFLEXA.PRCTR AS ProfitCenter_PRCTR,
  FAGLFLEXA.RFAREA AS FunctionalArea_RFAREA,
  FAGLFLEXA.RBUSA AS BusinessArea_RBUSA,
  FAGLFLEXA.KOKRS AS ControllingArea_KOKRS,
  FAGLFLEXA.SEGMENT AS SegmentForSegmentalReporting_SEGMENT,
  FAGLFLEXA.SCNTR AS SenderCostCenter_SCNTR,
  FAGLFLEXA.PPRCTR AS PartnerProfitCenter_PPRCTR,
  FAGLFLEXA.SFAREA AS PartnerFunctionalArea_SFAREA,
  FAGLFLEXA.SBUSA AS TradingPartnerBusinessArea_SBUSA,
  FAGLFLEXA.RASSC AS CompanyIdOfTradingPartner_RASSC,
  FAGLFLEXA.PSEGMENT AS PartnerSegmentForSegmentalReporting_PSEGMENT,
  FAGLFLEXA.TSL AS ValueInTransactionCurrency_TSL,
  FAGLFLEXA.HSL AS ValueInLocalCurrency_HSL,
  FAGLFLEXA.KSL AS ValueInGroupCurrency_KSL,
  FAGLFLEXA.OSL AS ValueInAnotherCurrency_OSL,
  FAGLFLEXA.MSL AS Quantity_MSL,
  FAGLFLEXA.WSL AS ValueInOriginalTransactionCurrency__documentCurrency___WSL,
  FAGLFLEXA.DRCRK AS DebitcreditIndicator_DRCRK,
  FAGLFLEXA.POPER AS PostingPeriod_POPER,
  FAGLFLEXA.RWCUR AS CurrencyKeyOfTheOriginalTransactionCurrency_RWCUR,
  FAGLFLEXA.GJAHR AS FiscalYear_GJAHR,
  FAGLFLEXA.BUDAT AS PostingDateInTheDocument_BUDAT,
  FAGLFLEXA.BELNR AS AccountingDocumentNumber_BELNR,
  FAGLFLEXA.BUZEI AS NumberOfLineItemWithinAccountingDocument_BUZEI,
  FAGLFLEXA.BSCHL AS PostingKey_BSCHL,
  FAGLFLEXA.BSTAT AS DocumentStatus_BSTAT,
  FAGLFLEXA.LINETYPE AS ItemCategory_LINETYPE,
  FAGLFLEXA.XSPLITMOD AS ItemChangedByDocumentSplitting_XSPLITMOD,
  FAGLFLEXA.TIMESTAMP AS UtcTimeStampInShortForm_TIMESTAMP,
  BKPF.BLART AS DocumentType_BLART,
  BKPF.BLDAT AS DocumentDateInDocument_BLDAT, BKPF.MONAT AS FiscalPeriod_MONAT,
  BKPF.CPUDT AS DayOnWhichAccountingDocumentWasEntered_CPUDT,
  BKPF.CPUTM AS TimeOfEntry_CPUTM,
  BKPF.AEDAT AS DateOfTheLastDocumentChangeByTransaction_AEDAT,
  BKPF.UPDDT AS DateOfTheLastDocumentUpdate_UPDDT,
  BKPF.WWERT AS TranslationDate_WWERT,
  BKPF.USNAM AS UserName_USNAM, BKPF.TCODE AS TransactionCode_TCODE,
  BKPF.XBLNR AS ReferenceDocumentNumber_XBLNR,
  BKPF.STBLG AS ReverseDocumentNumber_STBLG,
  BKPF.STJAH AS ReverseDocumentFiscalYear_STJAH,
  BKPF.BKTXT AS DocumentHeaderText_BKTXT, BKPF.WAERS AS CurrencyKey_WAERS,
  BKPF.KURSF AS ExchangeRate_KURSF, BKPF.KZWRS AS CurrencyKeyForTheGroupCurrency_KZWRS,
  BKPF.KZKRS AS GroupCurrencyExchangeRate_KZKRS,
  BKPF.XNETB AS Indicator_DocumentPostedNet_XNETB,
  IF(faglflexa.GJAHR != "0000", faglflexa.GJAHR, faglflexa.RYEAR ) AS FISCAL_YEAR
FROM `{{ project_id_src }}.{{ dataset_cdc_processed }}.bkpf` AS bkpf
INNER JOIN `{{ project_id_src }}.{{ dataset_cdc_processed }}.faglflexa` AS faglflexa
  ON bkpf.mandt = faglflexa.RCLNT AND bkpf.bukrs = faglflexa.rbukrs
    AND bkpf.belnr = faglflexa.belnr AND bkpf.gjahr = faglflexa.gjahr
LEFT OUTER JOIN `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.LedgersMD` AS Ledger
  ON Ledger.Client_MANDT = faglflexa.RCLNT
    AND Ledger.Ledger_RLDNR = faglflexa.RLDNR
WHERE Ledger.LedgerApplication_APPL = 'FI'
  AND Ledger.LedgerSubapplication_SUBAPPL = 'GLF'
  AND Ledger.LeadingLedgerFlag_XLEADING = 'X'
