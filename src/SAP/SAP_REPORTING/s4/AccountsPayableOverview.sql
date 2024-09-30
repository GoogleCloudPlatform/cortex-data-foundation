--'AccountsPayableOverview' - A view built as reference for details of calculations implemented in dashboards.
--It is not designed for extensive reporting or analytical use.
WITH CashDiscountUtilization AS (
  --## select the documents that have non-zero cash-discount-received
  SELECT Client_MANDT,
    CompanyCode_BUKRS,
    AccountingDocumentNumber_BELNR,
    SUM(AmountInLocalCurrency_DMBTR) AS CashDiscountReceived
  FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.AccountingDocuments`
  WHERE Client_MANDT = '{{ mandt }}'
    ## CORTEX-CUSTOMER: Please add relevant Document Type. Value 'KZ' represents 'Vendor Payment' and 'ZP' represents 'Payment Posting'
    AND Documenttype_BLART IN ('KZ', 'ZP')
    ## CORTEX-CUSTOMER: Please add relevant Transaction Key. Value 'SKE' represents 'Cash Discount Received'
    AND TransactionKey_KTOSL = 'SKE'
  GROUP BY Client_MANDT,
    CompanyCode_BUKRS,
    AccountingDocumentNumber_BELNR
  HAVING SUM(AmountInLocalCurrency_DMBTR) != 0
)

SELECT
  AccountsPayable.Client_MANDT,
  AccountsPayable.CompanyCode_BUKRS,
  AccountsPayable.CompanyText_BUTXT,
  AccountsPayable.Name1,
  AccountsPayable.AccountingDocumentNumber_BELNR,
  AccountsPayable.AccountNumberOfVendorOrCreditor_LIFNR,
  AccountsPayable.PostingDateInTheDocument_BUDAT,
  AccountsPayable.CurrencyKey_WAERS,
  AccountsPayable.TargetCurrency_TCURR,
  APT.DocFiscPeriod,
  MAX(AccountsPayable.MonthOfPostingDateInTheDocument_BUDAT) AS MonthOfPostingDateInTheDocument_BUDAT,
  MAX(AccountsPayable.YearOfPostingDateInTheDocument_BUDAT) AS YearOfPostingDateInTheDocument_BUDAT,
  MAX(AccountsPayable.QuarterOfPostingDateInTheDocument_BUDAT) AS QuarterOfPostingDateInTheDocument_BUDAT,
  MAX(AccountsPayable.WeekOfPostingDateInTheDocument_BUDAT) AS WeekOfPostingDateInTheDocument_BUDAT,
  MAX(AccountsPayable.ClearingDate_AUGDT) AS ClearingDate_AUGDT,
  COALESCE(
    SUM((AccountsPayable.OverdueAmountInSourceCurrency * -1) + (AccountsPayable.OutstandingButNotOverdueInSourceCurrency * -1)),
    0) AS TotalDueAmountInSourceCurrency,
  COALESCE(
    SUM((AccountsPayable.OverdueAmountInTargetCurrency * -1) + (AccountsPayable.OutstandingButNotOverdueInTargetCurrency * -1)),
    0) AS TotalDueAmountInTargetCurrency,
  SUM(AccountsPayable.OverdueAmountInSourceCurrency * -1) AS OverdueAmountInSourceCurrency,
  SUM(AccountsPayable.OverdueAmountInTargetCurrency * -1) AS OverdueAmountInTargetCurrency,
  SUM(AccountsPayable.OverdueOnPastDateInSourceCurrency * -1) AS PastDueAmountInSourceCurrency,
  SUM(AccountsPayable.OverdueOnPastDateInTargetCurrency * -1) AS PastDueAmountInTargetCurrency,
  SUM(AccountsPayable.LatePaymentsInSourceCurrency * -1) AS LatePaymentsInSourceCurrency,
  SUM(AccountsPayable.LatePaymentsInTargetCurrency * -1) AS LatePaymentsInTargetCurrency,
  SUM(AccountsPayable.UpcomingPaymentsInSourceCurrency * -1) AS UpcomingPaymentsInSourceCurrency,
  SUM(AccountsPayable.UpcomingPaymentsInTargetCurrency * -1) AS UpcomingPaymentsInTargetCurrency,
  SUM(AccountsPayable.PotentialPenaltyInSourceCurrency * -1) AS PotentialPenaltyInSourceCurrency,
  SUM(AccountsPayable.PotentialPenaltyInTargetCurrency * -1) AS PotentialPenaltyInTargetCurrency,
  COALESCE(MAX(APT.AccountsPayableTurnoverInSourceCurrency * -1), 0) AS AccountsPayableTurnoverInSourceCurrency,
  COALESCE(MAX(APT.AccountsPayableTurnoverInTargetCurrency * -1), 0) AS AccountsPayableTurnoverInTargetCurrency,
  COUNTIF(AccountsPayable.IsParkedInvoice) AS CountParkedInvoice,
  SUM(
    IF(AccountsPayable.IsParkedInvoice,
      AccountsPayable.AmountInLocalCurrency_DMBTR,
      0)
  ) AS ParkedInvoiceAmountInSourceCurrency,
  SUM(
    IF(AccountsPayable.IsParkedInvoice,
      AccountsPayable.AmountInTargetCurrency_DMBTR,
      0)
  ) AS ParkedInvoiceAmountInTargetCurrency,
  COUNTIF(AccountsPayable.IsBlockedInvoice) AS CountBlockedInvoice,
  SUM(
    IF(AccountsPayable.IsBlockedInvoice,
      (AccountsPayable.AmountInLocalCurrency_DMBTR * -1),
      0)
  )AS BlockedInvoiceAmountInSourceCurrency,
  SUM(
    IF(AccountsPayable.IsBlockedInvoice,
      (AccountsPayable.AmountInTargetCurrency_DMBTR * -1),
      0)
  )AS BlockedInvoiceAmountInTargetCurrency,
  SUM(AccountsPayable.TargetCashDiscountInSourceCurrency) AS TargetCashDiscountInSourceCurrency,
  SUM(AccountsPayable.TargetCashDiscountInTargetCurrency) AS TargetCashDiscountInTargetCurrency,
  SUM(CashDiscountUtilization.CashDiscountReceived) AS CashDiscountReceivedInSourceCurrency,
  SUM(CashDiscountUtilization.CashDiscountReceived * AccountsPayable.ExchangeRate_UKURS) AS CashDiscountReceivedInTargetCurrency,
  SAFE_DIVIDE(
    SUM(CashDiscountUtilization.CashDiscountReceived),
    SUM(AccountsPayable.TargetCashDiscountInSourceCurrency)
  ) AS CashDiscountUtilizationInSourceCurrency,
  SAFE_DIVIDE(
    SUM(CashDiscountUtilization.CashDiscountReceived * AccountsPayable.ExchangeRate_UKURS),
    SUM(AccountsPayable.TargetCashDiscountInTargetCurrency)
  ) AS CashDiscountUtilizationInTargetCurrency
FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.AccountsPayable` AS AccountsPayable
LEFT JOIN `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.AccountsPayableTurnover` AS APT
  ON AccountsPayable.Client_MANDT = APT.Client_MANDT
    AND AccountsPayable.CompanyCode_BUKRS = APT.CompanyCode_BUKRS
    AND AccountsPayable.AccountNumberOfVendorOrCreditor_LIFNR = APT.AccountNumberOfVendorOrCreditor_LIFNR
    AND AccountsPayable.AccountingDocumentNumber_BELNR = APT.AccountingDocumentNumber_BELNR
    AND AccountsPayable.TargetCurrency_TCURR = APT.TargetCurrency_TCURR
    AND AccountsPayable.DocFiscPeriod = APT.DocFiscPeriod
LEFT OUTER JOIN CashDiscountUtilization
  ON AccountsPayable.Client_MANDT = CashDiscountUtilization.Client_MANDT
    AND AccountsPayable.CompanyCode_BUKRS = CashDiscountUtilization.CompanyCode_BUKRS
    AND AccountsPayable.DocumentNumberOfTheClearingDocument_AUGBL = CashDiscountUtilization.AccountingDocumentNumber_BELNR
WHERE AccountsPayable.Client_MANDT = '{{ mandt }}'
GROUP BY
  AccountsPayable.Client_MANDT,
  AccountsPayable.CompanyCode_BUKRS,
  AccountsPayable.CompanyText_BUTXT,
  AccountsPayable.Name1,
  AccountsPayable.AccountingDocumentNumber_BELNR,
  AccountsPayable.AccountNumberOfVendorOrCreditor_LIFNR,
  AccountsPayable.CurrencyKey_WAERS,
  AccountsPayable.PostingDateInTheDocument_BUDAT,
  AccountsPayable.TargetCurrency_TCURR,
  APT.DocFiscPeriod
