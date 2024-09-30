SELECT
  finsc_ledger.MANDT AS Client_MANDT,
  finsc_ledger.RLDNR AS Ledger_RLDNR,
  finsc_ledger_t.LANGU AS Language_LANGU,
  finsc_ledger_t.NAME AS Name_NAME,
  finsc_ledger.LEDGER_TYPE AS LedgerType,
  finsc_ledger.EXT_LEDGER_TYPE AS ExtensionLedgerType_EXT_LEDGER_TYPE,
  finsc_ledger.APPL AS OwnerApplication_APPL,
  finsc_ledger.SUBAPPL AS SubApplication_SUBAPPL,
  finsc_ledger.XLEADING AS LeadingLedgerIndicator_XLEADING,
  finsc_ledger.CORE AS LedgerOfExtensionLedger_CORE,
  finsc_ledger.valutyp AS ValuationView_VALUTYP,
  finsc_ledger.VALUSUBTYP AS ValuationViewSubtype_VALUSUBTYP,
  finsc_ledger.MAN_POST_NOT_ALLWD AS ManualPostingsNotAllowed_MAN_POST_NOT_ALLWD,
  finsc_ledger.acc_PRINCIPLE AS CorporateAccountingPrinciple_ACC_PRINCIPLE,
  finsc_ledger.FALLBACK_LEDGER AS FallbackLedger_FALLBACK_LEDGER,
  finsc_ledger.TECH_LEDGER AS TechnicalLedger_TECH_LEDGER,
  finsc_ledger.XCASH_LEDGER AS CashLedgerIndicator_XCASH_LEDGER
FROM
  `{{ project_id_src }}.{{ dataset_cdc_processed_s4 }}.finsc_ledger` AS finsc_ledger
INNER JOIN
  `{{ project_id_src }}.{{ dataset_cdc_processed_s4 }}.finsc_ledger_t` AS finsc_ledger_t
  ON
    finsc_ledger.MANDT = finsc_ledger_t.MANDT
    AND finsc_ledger.RLDNR = finsc_ledger_t.RLDNR
