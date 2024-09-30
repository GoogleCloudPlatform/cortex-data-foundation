SELECT
  fagl_011qt.MANDT AS Client_MANDT,
  fagl_011qt.VERSN AS FinancialStatementVersion_VERSN,
  fagl_011qt.SPRAS AS LanguageKey_SPRAS,
  fagl_011qt.ERGSL AS FinancialStatementItem_ERGSL,
  fagl_011qt.TXTYP AS TextType_TXTYP,
  fagl_011qt.ZEILE AS FinancialStatementItemLineNumber_ZEILE,
  fagl_011qt.TXT45 AS FinancialStatementItemText_TXT45
FROM
  `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.fagl_011qt` AS fagl_011qt
