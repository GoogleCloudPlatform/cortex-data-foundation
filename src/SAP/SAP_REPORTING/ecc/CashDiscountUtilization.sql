WITH AccountingDocumentsKPI AS (
  SELECT
    AccountsPayable.Client_MANDT,
    AccountsPayable.CompanyCode_BUKRS,
    AccountsPayable.CompanyText_BUTXT,
    AccountingDocumentsCDU.AccountNumberOfVendorOrCreditor_LIFNR,
    AccountsPayable.NAME1,
    AccountingDocumentsCDU.AccountingDocumentNumber_BELNR,
    AccountingDocumentsCDU.AmountInLocalCurrency_DMBTR,
    AccountingDocumentsCDU.ClearingDate_AUGDT,
    AccountingDocumentsCDU.DocumentNumberOfTheClearingDocument_AUGBL,
    AccountsPayable.CashDiscountReceivedInSourceCurrency,
    AccountsPayable.CashDiscountReceivedInTargetCurrency,
    AccountsPayable.TargetCurrency_TCURR,
    AccountingDocumentsCDU.CurrencyKey_WAERS,
    AccountingDocumentsCDU.PostingDateInTheDocument_BUDAT,

    IF(
      ## CORTEX-CUSTOMER: Please add relevant Posting Key. Value '31' represents 'Vendor Invoice'
      AccountingDocumentsCDU.PostingKey_BSCHL = '31'
      AND
      AccountingDocumentsCDU.ClearingDate_AUGDT < DATE_ADD(
        AccountingDocumentsCDU.BaselineDateForDueDateCalculation_ZFBDT,
        INTERVAL CAST(AccountingDocumentsCDU.CashDiscountDays1_ZBD1T AS INT64) DAY
      ),
      (AccountingDocumentsCDU.AmountEligibleForCashDiscountInDocumentCurrency_SKFBT * AccountingDocumentsCDU.CashDiscountPercentage1_ZBD1P) / 100,
      0) AS TargetCashDiscountInSourceCurrency,

    IF(
      ## CORTEX-CUSTOMER: Please add relevant Posting Key. Value '31' represents 'Vendor Invoice'
      AccountingDocumentsCDU.PostingKey_BSCHL = '31'
      AND
      AccountingDocumentsCDU.ClearingDate_AUGDT < DATE_ADD(
        AccountingDocumentsCDU.BaselineDateForDueDateCalculation_ZFBDT,
        INTERVAL CAST(AccountingDocumentsCDU.CashDiscountDays1_ZBD1T AS INT64) DAY
      ),
      (AccountingDocumentsCDU.AmountEligibleForCashDiscountInDocumentCurrency_SKFBT * AccountsPayable.ExchangeRate_UKURS * AccountingDocumentsCDU.CashDiscountPercentage1_ZBD1P) / 100,
      0) AS TargetCashDiscountInTargetCurrency

  FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.AccountsPayable` AS AccountsPayable
  INNER JOIN `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.AccountingDocuments` AS AccountingDocumentsCDU
    ON AccountsPayable.Client_MANDT = AccountingDocumentsCDU.Client_MANDT
      AND AccountsPayable.CompanyCode_BUKRS = AccountingDocumentsCDU.CompanyCode_BUKRS
      AND AccountsPayable.AccountingDocumentNumber_BELNR = AccountingDocumentsCDU.DocumentNumberOfTheClearingDocument_AUGBL
      ## CORTEX-CUSTOMER: Please add relevant Posting Key. Value '31' represents 'Vendor Invoice'
      AND AccountingDocumentsCDU.AccountType_KOART = 'K'
      AND AccountingDocumentsCDU.PostingKey_BSCHL = '31'
)

SELECT
  AccountingDocumentsKPI.Client_MANDT,
  AccountingDocumentsKPI.CompanyCode_BUKRS,
  AccountingDocumentsKPI.CompanyText_BUTXT,
  AccountingDocumentsKPI.AccountNumberOfVendorOrCreditor_LIFNR,
  VendorsMD.NAME1,
  AccountingDocumentsKPI.AccountingDocumentNumber_BELNR,
  AccountingDocumentsKPI.TargetCurrency_TCURR,
  ANY_VALUE(AccountingDocumentsKPI.CurrencyKey_WAERS) AS CurrencyKey_WAERS,
  ANY_VALUE(AccountingDocumentsKPI.AmountInLocalCurrency_DMBTR) AS AmountInLocalCurrency_DMBTR,
  ANY_VALUE(AccountingDocumentsKPI.ClearingDate_AUGDT) AS ClearingDate_AUGDT,
  ANY_VALUE(AccountingDocumentsKPI.DocumentNumberOfTheClearingDocument_AUGBL) AS DocumentNumberOfTheClearingDocument_AUGBL,
  ANY_VALUE(AccountingDocumentsKPI.PostingDateInTheDocument_BUDAT) AS PostingDateInTheDocument_BUDAT,
  SUM(AccountingDocumentsKPI.CashDiscountReceivedInSourceCurrency) AS CashDiscountReceivedInSourceCurrency,
  SUM(AccountingDocumentsKPI.CashDiscountReceivedInTargetCurrency) AS CashDiscountReceivedInTargetCurrency,
  AVG(AccountingDocumentsKPI.TargetCashDiscountInSourceCurrency) AS TargetCashDiscountInSourceCurrency,
  AVG(AccountingDocumentsKPI.TargetCashDiscountInTargetCurrency) AS TargetCashDiscountInTargetCurrency
FROM AccountingDocumentsKPI
LEFT OUTER JOIN (
  /* Vendors may contain multiple addresses that may produce multiple VendorsMD records, pick the name agaist latest entry */
  SELECT Client_MANDT, AccountNumberOfVendorOrCreditor_LIFNR, NAME1
  FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.VendorsMD`
  WHERE ValidToDate_DATE_TO = '9999-12-31'
) AS VendorsMD
ON AccountingDocumentsKPI.Client_MANDT = VendorsMD.Client_MANDT
  AND AccountingDocumentsKPI.AccountNumberOfVendorOrCreditor_LIFNR = VendorsMD.AccountNumberOfVendorOrCreditor_LIFNR
WHERE
  AccountingDocumentsKPI.TargetCashDiscountInSourceCurrency != 0
  AND AccountingDocumentsKPI.CashDiscountReceivedInSourceCurrency != 0
  AND AccountingDocumentsKPI.TargetCashDiscountInTargetCurrency != 0
  AND AccountingDocumentsKPI.CashDiscountReceivedInTargetCurrency != 0
GROUP BY
  AccountingDocumentsKPI.Client_MANDT,
  AccountingDocumentsKPI.CompanyCode_BUKRS,
  AccountingDocumentsKPI.CompanyText_BUTXT,
  AccountingDocumentsKPI.AccountNumberOfVendorOrCreditor_LIFNR,
  VendorsMD.NAME1,
  AccountingDocumentsKPI.AccountingDocumentNumber_BELNR,
  AccountingDocumentsKPI.TargetCurrency_TCURR
