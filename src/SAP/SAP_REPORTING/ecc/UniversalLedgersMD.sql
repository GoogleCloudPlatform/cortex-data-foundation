SELECT
  t881.MANDT AS Client_MANDT,
  t881.RLDNR AS Ledger_RLDNR,
  t881t.LANGU AS Language_LANGU,
  t881t.NAME AS Name_NAME,
  t881.TYP AS LedgerType,
  t881.APPL AS OwnerApplication_APPL,
  t881.SUBAPPL AS SubApplication_SUBAPPL,
  t881.XLEADING AS LeadingLedgerIndicator_XLEADING,
  t881.VALUTYP AS ValuationView_VALUTYP,
  t881.XCASH_LEDGER AS CashLedgerIndicator_XCASH_LEDGER,
  t881.GCURR AS Currency_GCURR,
  t881.CLASS AS CompanyCurrencyRole_CLASS,
  t881.LOGSYS AS LogicalSystem_LOGSYS,
  t881.CURT1 AS AddlCurrRole1_CURT1,
  t881.CURT2 AS AddlCurrRole2_CURT2,
  t881.CURT3 AS AddlCurrRole3_CURT3
FROM
  `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.t881` AS t881
INNER JOIN
  `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.t881t` AS t881t
  ON
    t881.MANDT = t881t.MANDT
    AND t881.RLDNR = t881t.RLDNR
