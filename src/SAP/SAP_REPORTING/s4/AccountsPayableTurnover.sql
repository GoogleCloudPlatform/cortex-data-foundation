WITH AccountsPayable AS (
  SELECT
    AccountsPayable.Client_MANDT,
    AccountsPayable.CompanyCode_BUKRS,
    AccountsPayable.CompanyText_BUTXT,
    AccountsPayable.AccountNumberOfVendorOrCreditor_LIFNR,
    AccountsPayable.NAME1,
    AccountsPayable.AccountingDocumentNumber_BELNR,
    AccountsPayable.NumberOfLineItemWithinAccountingDocument_BUZEI,
    AccountsPayable.PostingDateInTheDocument_BUDAT,
    AccountsPayable.AccountingDocumenttype_BLART,
    AccountsPayable.AmountInLocalCurrency_DMBTR,
    AccountsPayable.AmountInTargetCurrency_DMBTR,
    AccountsPayable.CurrencyKey_WAERS,
    AccountsPayable.TargetCurrency_TCURR,
    AccountsPayable.DocFiscPeriod,

    SUM(
      IF(
        ## CORTEX-CUSTOMER: Please add relevant Movement Types.
        -- Value '101' represents 'GR Goods Receipt' and '501' represents 'Receipt w/o PO'
        AccountsPayable.AccountType_KOART = 'M' AND AccountsPayable.MovementType__inventoryManagement___BWART IN ('101', '501'),
        AccountsPayable.POOrderHistory_AmountInLocalCurrency_DMBTR,
        IF(
          ## CORTEX-CUSTOMER: Please add relevant Movement Types.
          -- Value '102' represents 'GR for PO reversal (full or any one of the line item)'
          -- Value '502' represents 'Return Receipt w/o PO' (Receipt made against movement type 501 document is cancelled.)
          AccountsPayable.AccountType_KOART = 'M' AND AccountsPayable.MovementType__inventoryManagement___BWART IN ('102', '502'),
          AccountsPayable.POOrderHistory_AmountInLocalCurrency_DMBTR * -1, 0
        )
      )
    ) OVER (
      PARTITION BY AccountsPayable.Client_MANDT, AccountsPayable.CompanyCode_BUKRS, AccountsPayable.TargetCurrency_TCURR, AccountsPayable.DocFiscPeriod
    ) AS TotalPurchasesInSourceCurrency,

    SUM(
      IF(
        ## CORTEX-CUSTOMER: Please add relevant Movement Types.
        -- Value '101' represents 'GR Goods Receipt' and '501' represents 'Receipt w/o PO'
        AccountsPayable.AccountType_KOART = 'M' AND AccountsPayable.MovementType__inventoryManagement___BWART IN ('101', '501'),
        AccountsPayable.POOrderHistory_AmountInTargetCurrency_DMBTR,
        IF(
          ## CORTEX-CUSTOMER: Please add relevant Movement Types.
          -- Value '102' represents 'GR for PO reversal (full or any one of the line item)'
          -- Value '502' represents 'Return Receipt w/o PO' (Receipt made against movement type 501 document is cancelled.)
          AccountsPayable.AccountType_KOART = 'M' AND AccountsPayable.MovementType__inventoryManagement___BWART IN ('102', '502'),
          AccountsPayable.POOrderHistory_AmountInTargetCurrency_DMBTR * -1, 0
        )
      )
    ) OVER (
      PARTITION BY AccountsPayable.Client_MANDT, AccountsPayable.CompanyCode_BUKRS, AccountsPayable.TargetCurrency_TCURR, AccountsPayable.DocFiscPeriod
    ) AS TotalPurchasesInTargetCurrency,

    SUM(
      IF(
        ## CORTEX-CUSTOMER: Please add relevant Account Type. Value 'K' represents 'Vendor'
        AccountsPayable.Accounttype_KOART = 'K'
        ## CORTEX-CUSTOMER: Please add relevant Document Type. Value 'RE' represents 'Invoice - Gross'
        AND AccountsPayable.InvoiceDocumenttype_BLART = 'RE',
        AccountsPayable.AmountInLocalCurrency_DMBTR,
        0
      )
    ) OVER (
      PARTITION BY AccountsPayable.Client_MANDT, AccountsPayable.CompanyCode_BUKRS, AccountsPayable.TargetCurrency_TCURR, AccountsPayable.DocFiscPeriod
    ) AS PeriodAPInSourceCurrency,

    SUM(
      IF(
        ## CORTEX-CUSTOMER: Please add relevant Account Type. Value 'K' represents 'Vendor'
        AccountsPayable.Accounttype_KOART = 'K'
        ## CORTEX-CUSTOMER: Please add relevant Document Type. Value 'RE' represents 'Invoice - Gross'
        AND AccountsPayable.InvoiceDocumenttype_BLART = 'RE',
        AccountsPayable.AmountInTargetCurrency_DMBTR,
        0
      )
    ) OVER (
      PARTITION BY AccountsPayable.Client_MANDT, AccountsPayable.CompanyCode_BUKRS, AccountsPayable.TargetCurrency_TCURR, AccountsPayable.DocFiscPeriod
    ) AS PeriodAPInTargetCurrency,

    SUM(
      IF(
        ## CORTEX-CUSTOMER: Please add relevant Account Type. Value 'K' represents 'Vendor'
        AccountsPayable.Accounttype_KOART = 'K'
        ## CORTEX-CUSTOMER: Please add relevant Document Type. Value 'RE' represents 'Invoice - Gross'
        AND AccountsPayable.InvoiceDocumenttype_BLART = 'RE'
        -- AND AccountsPayable.PostingDateInTheDocument_BUDAT <= AccountsPayable.DocFiscPeriodEndDate
        AND AccountsPayable.ClearingDate_AUGDT IS NULL,
        AccountsPayable.AmountInLocalCurrency_DMBTR,
        0
      )
    ) OVER (
      PARTITION BY AccountsPayable.Client_MANDT, AccountsPayable.CompanyCode_BUKRS, AccountsPayable.TargetCurrency_TCURR, AccountsPayable.DocFiscPeriod
    ) AS UnpaidPeriodAPInSourceCurrency,

    SUM(
      IF(
        ## CORTEX-CUSTOMER: Please add relevant Account Type. Value 'K' represents 'Vendor'
        AccountsPayable.Accounttype_KOART = 'K'
        ## CORTEX-CUSTOMER: Please add relevant Document Type. Value 'RE' represents 'Invoice - Gross'
        AND AccountsPayable.InvoiceDocumenttype_BLART = 'RE'
        -- AND AccountsPayable.PostingDateInTheDocument_BUDAT <= AccountsPayable.DocFiscPeriodEndDate
        AND AccountsPayable.ClearingDate_AUGDT IS NULL,
        AccountsPayable.AmountInTargetCurrency_DMBTR,
        0
      )
    ) OVER (
      PARTITION BY AccountsPayable.Client_MANDT, AccountsPayable.CompanyCode_BUKRS, AccountsPayable.TargetCurrency_TCURR, AccountsPayable.DocFiscPeriod
    ) AS UnpaidPeriodAPInTargetCurrency,

    SUM(
      IF(
        ## CORTEX-CUSTOMER: Please add relevant Account Type. Value 'K' represents 'Vendor'
        AccountsPayable.Accounttype_KOART = 'K'
        ## CORTEX-CUSTOMER: Please add relevant Document Type. Value 'RE' represents 'Invoice - Gross'
        AND AccountsPayable.InvoiceDocumenttype_BLART = 'RE'
        AND AccountsPayable.ClearingDate_AUGDT IS NULL,
        AccountsPayable.AmountInLocalCurrency_DMBTR,
        0
      )
    ) OVER (
      PARTITION BY AccountsPayable.Client_MANDT, AccountsPayable.CompanyCode_BUKRS, AccountsPayable.TargetCurrency_TCURR
      ORDER BY AccountsPayable.DocFiscPeriod
    ) AS ClosingAPInSourceCurrency,

    SUM(
      IF(
        ## CORTEX-CUSTOMER: Please add relevant Account Type. Value 'K' represents 'Vendor'
        AccountsPayable.Accounttype_KOART = 'K'
        ## CORTEX-CUSTOMER: Please add relevant Document Type. Value 'RE' represents 'Invoice - Gross'
        AND AccountsPayable.InvoiceDocumenttype_BLART = 'RE'
        AND AccountsPayable.ClearingDate_AUGDT IS NULL,
        AccountsPayable.AmountInTargetCurrency_DMBTR,
        0
      )
    ) OVER (
      PARTITION BY AccountsPayable.Client_MANDT, AccountsPayable.CompanyCode_BUKRS, AccountsPayable.TargetCurrency_TCURR
      ORDER BY AccountsPayable.DocFiscPeriod
    ) AS ClosingAPInTargetCurrency

  FROM
    `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.AccountsPayable` AS AccountsPayable
  WHERE
    AccountsPayable.DocFiscPeriod <= AccountsPayable.KeyFiscPeriod
)

SELECT
  AccountsPayable.Client_MANDT,
  AccountsPayable.CompanyCode_BUKRS,
  AccountsPayable.CompanyText_BUTXT,
  AccountsPayable.AccountNumberOfVendorOrCreditor_LIFNR,
  AccountsPayable.NAME1,
  AccountsPayable.AccountingDocumentNumber_BELNR,
  AccountsPayable.NumberOfLineItemWithinAccountingDocument_BUZEI,
  AccountsPayable.PostingDateInTheDocument_BUDAT,
  AccountsPayable.AccountingDocumenttype_BLART,
  AccountsPayable.AmountInLocalCurrency_DMBTR,
  AccountsPayable.AmountInTargetCurrency_DMBTR,
  AccountsPayable.CurrencyKey_WAERS,
  AccountsPayable.TargetCurrency_TCURR,
  AccountsPayable.DocFiscPeriod,

  COALESCE(AccountsPayable.TotalPurchasesInSourceCurrency, 0) AS TotalPurchasesInSourceCurrency,
  COALESCE(AccountsPayable.TotalPurchasesInTargetCurrency, 0) AS TotalPurchasesInTargetCurrency,
  COALESCE(AccountsPayable.ClosingAPInSourceCurrency, 0) - COALESCE(UnpaidPeriodAPInSourceCurrency, 0) AS OpeningAPInSourceCurrency,
  COALESCE(AccountsPayable.ClosingAPInTargetCurrency, 0) - COALESCE(UnpaidPeriodAPInTargetCurrency, 0) AS OpeningAPInTargetCurrency,
  COALESCE(AccountsPayable.PeriodAPInSourceCurrency, 0) AS PeriodAPInSourceCurrency,
  COALESCE(AccountsPayable.PeriodAPInTargetCurrency, 0) AS PeriodAPInTargetCurrency,
  COALESCE(AccountsPayable.ClosingAPInSourceCurrency, 0) - COALESCE(UnpaidPeriodAPInSourceCurrency, 0) + COALESCE(AccountsPayable.PeriodAPInSourceCurrency, 0) AS ClosingAPInSourceCurrency,
  COALESCE(AccountsPayable.ClosingAPInTargetCurrency, 0) - COALESCE(UnpaidPeriodAPInTargetCurrency, 0) + COALESCE(AccountsPayable.PeriodAPInTargetCurrency, 0) AS ClosingAPInTargetCurrency,

  SAFE_DIVIDE(
    AccountsPayable.TotalPurchasesInSourceCurrency,
    (
      (COALESCE(AccountsPayable.ClosingAPInSourceCurrency, 0) - COALESCE(UnpaidPeriodAPInSourceCurrency, 0) + COALESCE(AccountsPayable.PeriodAPInSourceCurrency, 0)) * 2
      - COALESCE(AccountsPayable.PeriodAPInSourceCurrency, 0)
    ) / 2
  ) AS AccountsPayableTurnoverInSourceCurrency,

  SAFE_DIVIDE(
    AccountsPayable.TotalPurchasesInTargetCurrency,
    (
      (COALESCE(AccountsPayable.ClosingAPInTargetCurrency, 0) - COALESCE(UnpaidPeriodAPInTargetCurrency, 0) + COALESCE(AccountsPayable.PeriodAPInTargetCurrency, 0)) * 2
      - COALESCE(AccountsPayable.PeriodAPInTargetCurrency, 0)
    ) / 2
  ) AS AccountsPayableTurnoverInTargetCurrency
FROM AccountsPayable
