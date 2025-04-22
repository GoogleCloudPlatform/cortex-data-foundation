WITH
  CurrencyConversion AS (
    SELECT
      Client_MANDT, FromCurrency_FCURR, ToCurrency_TCURR, ConvDate, ExchangeRate_UKURS
    FROM
      `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.CurrencyConversion`
    WHERE
      ToCurrency_TCURR IN UNNEST({{ sap_currencies }})
      -- ## CORTEX-CUSTOMER Modify the exchange rate type based on your requirement
      AND ExchangeRateType_KURST = 'M'
  ),

  AccountingInvoices AS (
    SELECT
      AccountingDocuments.Client_MANDT,
      AccountingDocuments.CompanyCode_BUKRS,
      AccountingDocuments.AccountingDocumentNumber_BELNR,
      AccountingDocuments.FiscalYear_GJAHR,
      AccountingDocuments.Documenttype_BLART AS AccountingDocumenttype_BLART,
      InvoiceDocuments.Documenttype_BLART AS InvoiceDocumenttype_BLART,
      AccountingDocuments.DocumentDateInDocument_BLDAT,
      AccountingDocuments.PostingDateInTheDocument_BUDAT,
      InvoiceDocuments.PostingDate_BUDAT,
      AccountingDocuments.FiscalPeriod_MONAT,
      AccountingDocuments.PurchasingDocumentNumber_EBELN,
      AccountingDocuments.NumberOfLineItemWithinAccountingDocument_BUZEI,
      AccountingDocuments.ClearingDate_AUGDT,
      COALESCE(AccountingDocuments.NetPaymentAmount_NEBTR, 0) AS NetPaymentAmount_NEBTR,
      COALESCE(AccountingDocuments.AmountInLocalCurrency_DMBTR, 0) AS AmountInLocalCurrency_DMBTR,
      AccountingDocuments.AccountType_KOART,
      AccountingDocuments.TransactionKey_KTOSL,
      AccountingDocuments.PostingKey_BSCHL,
      AccountingDocuments.CashDiscountDays1_ZBD1T,
      AccountingDocuments.BaselineDateForDueDateCalculation_ZFBDT,
      COALESCE(AccountingDocuments.AmountEligibleForCashDiscountInDocumentCurrency_SKFBT, 0) AS AmountEligibleForCashDiscountInDocumentCurrency_SKFBT,
      AccountingDocuments.AccountNumberOfVendorOrCreditor_LIFNR,
      AccountingDocuments.PaymentBlockKey_ZLSPR,
      AccountingDocuments.SpecialGlIndicator_UMSKZ,
      AccountingDocuments.ItemNumberOfPurchasingDocument_EBELP,
      AccountingDocuments.FollowOnDocumentType_REBZT,
      AccountingDocuments.DocumentNumberOfTheClearingDocument_AUGBL,
      AccountingDocuments.TermsOfPaymentKey_ZTERM,
      AccountingDocuments.ReasonCodeForPayments_RSTGR,
      AccountingDocuments.CashDiscountPercentage1_ZBD1P,
      AccountingDocuments.NetPaymentTermsPeriod_ZBD3T,
      AccountingDocuments.CashDiscountDays2_ZBD2T,
      AccountingDocuments.DebitcreditIndicator_SHKZG,
      AccountingDocuments.InvoiceToWhichTheTransactionBelongs_REBZG,
      AccountingDocuments.CurrencyKey_WAERS,
      AccountingDocuments.SupplyingCountry_LANDL,
      AccountingDocuments.ObjectKey_AWKEY,
      NULL AS InvStatus_RBSTAT,
      AccountingDocuments.YearOfPostingDateInTheDocument_BUDAT,
      AccountingDocuments.MonthOfPostingDateInTheDocument_BUDAT,
      AccountingDocuments.WeekOfPostingDateInTheDocument_BUDAT,
      AccountingDocuments.QuarterOfPostingDateInTheDocument_BUDAT
    FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.AccountingDocuments` AS AccountingDocuments
    LEFT OUTER JOIN `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.InvoiceDocuments_Flow` AS InvoiceDocuments
      ON
        AccountingDocuments.Client_MANDT = InvoiceDocuments.Client_MANDT
        AND AccountingDocuments.CompanyCode_BUKRS = InvoiceDocuments.CompanyCode_BUKRS
        AND LEFT(AccountingDocuments.ObjectKey_AWKEY, 10) = InvoiceDocuments.InvoiceDocNum_BELNR
        AND AccountingDocuments.FiscalYear_GJAHR = InvoiceDocuments.FiscalYear_GJAHR
        AND LTRIM(AccountingDocuments.NumberOfLineItemWithinAccountingDocument_BUZEI, '0') = LTRIM(InvoiceDocuments.InvoiceDocLineNum_BUZEI, '0')
    WHERE
      AccountingDocuments.AccountType_KOART = 'K' -- ## CORTEX-CUSTOMER: Please add relevant Account Type. Value 'K' represents 'Vendor'
      OR AccountingDocuments.PurchasingDocumentNumber_EBELN IS NOT NULL
      OR AccountingDocuments.Documenttype_BLART IN ('KZ', 'ZP') -- ## CORTEX-CUSTOMER: Please add relevant Document Type. Value 'KZ' represents 'Vendor Payment' and 'ZP' represents 'Payment Posting'
      OR AccountingDocuments.TransactionKey_KTOSL = 'SKE' -- ## CORTEX-CUSTOMER: Please add relevant Transaction Key. Value 'SKE' represents 'Cash Discount Received'
      OR AccountingDocuments.PostingKey_BSCHL = '31' -- ## CORTEX-CUSTOMER: Please add relevant Posting Key. Value '31' represents 'Vendor Invoice'

    UNION ALL

    SELECT
      InvoiceDocuments.Client_MANDT,
      InvoiceDocuments.CompanyCode_BUKRS,
      InvoiceDocuments.InvoiceDocNum_BELNR AS AccountingDocumentNumber_BELNR,
      InvoiceDocuments.FiscalYear_GJAHR,
      CAST(NULL AS STRING) AS AccountingDocumenttype_BLART,
      CAST(NULL AS STRING) AS InvoiceDocumenttype_BLART,
      InvoiceDocuments.DocumentDate_BLDAT AS DocumentDateInDocument_BLDAT,
      InvoiceDocuments.PostingDate_BUDAT AS PostingDateInTheDocument_BUDAT,
      InvoiceDocuments.PostingDate_BUDAT,
      CAST(NULL AS STRING) AS FiscalPeriod_MONAT,
      CAST(NULL AS STRING) AS PurchasingDocumentNumber_EBELN,
      CAST(NULL AS STRING) AS NumberOfLineItemWithinAccountingDocument_BUZEI,
      CAST(NULL AS DATE) AS ClearingDate_AUGDT,
      NULL AS NetPaymentAmount_NEBTR,
      InvoiceDocuments.GrossInvAmnt_RMWWR AS AmountInLocalCurrency_DMBTR,
      CAST(NULL AS STRING) AS AccountType_KOART,
      CAST(NULL AS STRING) AS TransactionKey_KTOSL,
      CAST(NULL AS STRING) AS PostingKey_BSCHL,
      CAST(NULL AS NUMERIC) AS CashDiscountDays1_ZBD1T,
      CAST(NULL AS DATE) AS BaselineDateForDueDateCalculation_ZFBDT,
      CAST(NULL AS NUMERIC) AS AmountEligibleForCashDiscountInDocumentCurrency_SKFBT,
      InvoiceDocuments.InvoicingParty_LIFNR AS AccountNumberOfVendorOrCreditor_LIFNR,
      CAST(NULL AS STRING) AS PaymentBlockKey_ZLSPR,
      CAST(NULL AS STRING) AS SpecialGlIndicator_UMSKZ,
      CAST(NULL AS STRING) AS ItemNumberOfPurchasingDocument_EBELP,
      CAST(NULL AS STRING) AS FollowOnDocumentType_REBZT,
      CAST(NULL AS STRING) AS DocumentNumberOfTheClearingDocument_AUGBL,
      CAST(NULL AS STRING) AS TermsOfPaymentKey_ZTERM,
      CAST(NULL AS STRING) AS ReasonCodeForPayments_RSTGR,
      CAST(NULL AS NUMERIC) AS CashDiscountPercentage1_ZBD1P,
      CAST(NULL AS NUMERIC) AS NetPaymentTermsPeriod_ZBD3T,
      CAST(NULL AS NUMERIC) AS CashDiscountDays2_ZBD2T,
      CAST(NULL AS STRING) AS DebitcreditIndicator_SHKZG,
      CAST(NULL AS STRING) AS InvoiceToWhichTheTransactionBelongs_REBZG,
      Currency_WAERS AS CurrencyKey_WAERS,
      CAST(NULL AS STRING) AS SupplyingCountry_LANDL,
      CAST(NULL AS STRING) AS ObjectKey_AWKEY,
      InvoiceDocuments.InvStatus_RBSTAT,
      InvoiceDocuments.YearOfPostingDate_BUDAT,
      InvoiceDocuments.MonthOfPostingDate_BUDAT,
      InvoiceDocuments.WeekOfPostingDate_BUDAT,
      InvoiceDocuments.QuarterOfPostingDate_BUDAT
    FROM
      `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.InvoiceDocuments_Flow` AS InvoiceDocuments
    WHERE
      -- ## CORTEX-CUSTOMER: Please add relevant Invoice Status. Value 'A' represents that the document is Parked and not posted
      InvoiceDocuments.Invstatus_RBSTAT = 'A'
    QUALIFY RANK() OVER (
      PARTITION BY InvoiceDocuments.Client_MANDT, InvoiceDocuments.CompanyCode_BUKRS, InvoiceDocuments.InvoiceDocNum_BELNR
      ORDER BY InvoiceDocuments.InvoiceDocLineNum_BUZEI
    ) = 1
  ),

  AccountingInvoicesKPI AS (
    SELECT
      AccountingInvoices.*,
      CompaniesMD.CompanyText_BUTXT,
      FiscalDateDimension_BUDAT.FiscalYearPeriod AS DocFiscPeriod,
      FiscalDateDimension_KEYDATE.FiscalYearPeriod AS KeyFiscPeriod,

      DATE_ADD(
        IF(
          -- ## CORTEX-CUSTOMER: Please add relevant Account Type. Value 'K' represents 'Vendor'
          AccountingInvoices.AccountType_KOART = 'K' AND AccountingInvoices.BaselineDateForDueDateCalculation_ZFBDT IS NULL,
          AccountingInvoices.DocumentDateInDocument_BLDAT,
          AccountingInvoices.BaselineDateForDueDateCalculation_ZFBDT
        ),
        INTERVAL CAST(
          CASE
            -- ## CORTEX-CUSTOMER: Please add relevant Account Type. Value 'K' represents 'Vendor'
            WHEN AccountingInvoices.AccountType_KOART = 'K' AND AccountingInvoices.NetPaymentTermsPeriod_ZBD3T IS NOT NULL
              THEN AccountingInvoices.NetPaymentTermsPeriod_ZBD3T
            -- ## CORTEX-CUSTOMER: Please add relevant Account Type. Value 'K' represents 'Vendor'
            WHEN AccountingInvoices.AccountType_KOART = 'K' AND AccountingInvoices.CashDiscountDays2_ZBD2T IS NOT NULL
              THEN AccountingInvoices.CashDiscountDays2_ZBD2T
            -- ## CORTEX-CUSTOMER: Please add relevant Account Type. Value 'K' represents 'Vendor'
            WHEN AccountingInvoices.AccountType_KOART = 'K' AND AccountingInvoices.CashDiscountDays1_ZBD1T IS NOT NULL
              THEN AccountingInvoices.CashDiscountDays1_ZBD1T
            WHEN AccountingInvoices.CashDiscountDays1_ZBD1T IS NULL
              THEN 0
            -- ## CORTEX-CUSTOMER: Please add relevant Account Type. Value 'K' represents 'Vendor'
            -- ## CORTEX-CUSTOMER: Please add relevant Debit Credit Indicator. Value 'H' represents 'Credit' ('S' represents 'Debit'))
            WHEN AccountingInvoices.AccountType_KOART = 'K' AND AccountingInvoices.DebitcreditIndicator_SHKZG = 'H'
              AND AccountingInvoices.InvoiceToWhichTheTransactionBelongs_REBZG IS NULL
              THEN 0  -- ## CORTEX-CUSTOMER: Please add relevant Debit Credit Indicator
            ELSE 0
          END
          AS INT64
        ) DAY
      ) AS NetDueDate

    FROM AccountingInvoices
    INNER JOIN
      `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.CompaniesMD` AS CompaniesMD
      ON
        AccountingInvoices.Client_MANDT = CompaniesMD.Client_MANDT
        AND AccountingInvoices.CompanyCode_BUKRS = CompaniesMD.CompanyCode_BUKRS
    LEFT JOIN
      `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.fiscal_date_dim` AS FiscalDateDimension_BUDAT
      ON
        AccountingInvoices.Client_MANDT = FiscalDateDimension_BUDAT.MANDT
        AND CompaniesMD.FiscalyearVariant_PERIV = FiscalDateDimension_BUDAT.PERIV
        AND AccountingInvoices.PostingDateInTheDocument_BUDAT = FiscalDateDimension_BUDAT.DATE
    LEFT JOIN
      `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.fiscal_date_dim` AS FiscalDateDimension_KEYDATE
      ON
        AccountingInvoices.Client_MANDT = FiscalDateDimension_KEYDATE.MANDT
        AND CompaniesMD.FiscalyearVariant_PERIV = FiscalDateDimension_KEYDATE.PERIV
        AND DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY) = FiscalDateDimension_KEYDATE.DATE
  ),
  Vendors AS (
    -- Vendors may contain multiple addresses that may produce multiple VendorsMD records
    -- pick the name against latest entry
    SELECT Client_MANDT, AccountNumberOfVendorOrCreditor_LIFNR, NAME1
    FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.VendorsMD`
    WHERE
      ValidToDate_DATE_TO = '9999-12-31'
      -- ## CORTEX-CUSTOMER Modify the filter according to the configuration, for example, if NATION
      -- has value 'I' for international, it could be used in the filter
      AND COALESCE(VersionIdForInternationalAddresses_NATION, '') = ''
  )

SELECT
  AccountingInvoicesKPI.Client_MANDT,
  AccountingInvoicesKPI.CompanyCode_BUKRS,
  AccountingInvoicesKPI.CompanyText_BUTXT,
  AccountingInvoicesKPI.AccountNumberOfVendorOrCreditor_LIFNR,
  Vendors.NAME1,
  AccountingInvoicesKPI.AmountInLocalCurrency_DMBTR,
  AccountingInvoicesKPI.AccountingDocumentNumber_BELNR,
  AccountingInvoicesKPI.NumberOfLineItemWithinAccountingDocument_BUZEI,
  AccountingInvoicesKPI.DocumentNumberOfTheClearingDocument_AUGBL,
  AccountingInvoicesKPI.TermsOfPaymentKey_ZTERM,
  AccountingInvoicesKPI.AccountType_KOART,
  AccountingInvoicesKPI.ReasonCodeForPayments_RSTGR,
  AccountingInvoicesKPI.PaymentBlockKey_ZLSPR,
  AccountingInvoicesKPI.ClearingDate_AUGDT,
  AccountingInvoicesKPI.PostingDateInTheDocument_BUDAT,
  AccountingInvoicesKPI.FiscalYear_GJAHR,
  AccountingInvoicesKPI.FiscalPeriod_MONAT,
  AccountingInvoicesKPI.DocFiscPeriod,
  AccountingInvoicesKPI.KeyFiscPeriod,
  AccountingInvoicesKPI.NetDueDate,
  AccountingInvoicesKPI.InvStatus_RBSTAT,
  AccountingInvoicesKPI.PostingDate_BUDAT,
  AccountingInvoicesKPI.PurchasingDocumentNumber_EBELN,
  AccountingInvoicesKPI.CurrencyKey_WAERS,
  AccountingInvoicesKPI.SupplyingCountry_LANDL,
  AccountingInvoicesKPI.AccountingDocumenttype_BLART,
  AccountingInvoicesKPI.InvoiceDocumenttype_BLART,
  POOrderHistory.MovementType__inventoryManagement___BWART,
  POOrderHistory.AmountInLocalCurrency_DMBTR AS POOrderHistory_AmountInLocalCurrency_DMBTR,
  POOrderHistory.AmountInLocalCurrency_DMBTR * CurrencyConversion.ExchangeRate_UKURS AS POOrderHistory_AmountInTargetCurrency_DMBTR,
  AccountingInvoicesKPI.YearOfPostingDateInTheDocument_BUDAT,
  AccountingInvoicesKPI.MonthOfPostingDateInTheDocument_BUDAT,
  AccountingInvoicesKPI.WeekOfPostingDateInTheDocument_BUDAT,
  AccountingInvoicesKPI.QuarterOfPostingDateInTheDocument_BUDAT,
  AccountingInvoicesKPI.AmountInLocalCurrency_DMBTR * CurrencyConversion.ExchangeRate_UKURS AS AmountInTargetCurrency_DMBTR,
  CurrencyConversion.ExchangeRate_UKURS,
  CurrencyConversion.ToCurrency_TCURR AS TargetCurrency_TCURR,

  /* Overdue Amount */
  IF(
    -- ## CORTEX-CUSTOMER: Please add relevant Account Type. Value 'K' represents 'Vendor'
    AccountingInvoicesKPI.AccountType_KOART = 'K' AND CURRENT_DATE() > AccountingInvoicesKPI.NetDueDate,
    AccountingInvoicesKPI.AmountInLocalCurrency_DMBTR,
    0
  ) AS OverdueAmountInSourceCurrency,

  IF(
    -- ## CORTEX-CUSTOMER: Please add relevant Account Type. Value 'K' represents 'Vendor'
    AccountingInvoicesKPI.AccountType_KOART = 'K' AND CURRENT_DATE() > AccountingInvoicesKPI.NetDueDate,
    AccountingInvoicesKPI.AmountInLocalCurrency_DMBTR * CurrencyConversion.ExchangeRate_UKURS,
    0
  ) AS OverdueAmountInTargetCurrency,


  /* Outstanding But Not Overdue */
  IF(
    -- ## CORTEX-CUSTOMER: Please add relevant Account Type. Value 'K' represents 'Vendor'
    AccountingInvoicesKPI.AccountType_KOART = 'K' AND CURRENT_DATE() <= AccountingInvoicesKPI.NetDueDate,
    AccountingInvoicesKPI.AmountInLocalCurrency_DMBTR,
    0
  ) AS OutstandingButNotOverdueInSourceCurrency,

  IF(
    -- ## CORTEX-CUSTOMER: Please add relevant Account Type. Value 'K' represents 'Vendor'
    AccountingInvoicesKPI.AccountType_KOART = 'K' AND CURRENT_DATE() <= AccountingInvoicesKPI.NetDueDate,
    AccountingInvoicesKPI.AmountInLocalCurrency_DMBTR * CurrencyConversion.ExchangeRate_UKURS,
    0
  ) AS OutstandingButNotOverdueInTargetCurrency,

  /* Overdue On Past Date */
  IF(
    -- ## CORTEX-CUSTOMER: Please add relevant Account Type. Value 'K' represents 'Vendor'
    AccountingInvoicesKPI.AccountType_KOART = 'K'
    AND AccountingInvoicesKPI.PostingDateInTheDocument_BUDAT < CURRENT_DATE()
    AND AccountingInvoicesKPI.NetDueDate < CURRENT_DATE()
    AND AccountingInvoicesKPI.ClearingDate_AUGDT IS NULL,
    AccountingInvoicesKPI.AmountInLocalCurrency_DMBTR,
    0
  )
  + IF(
    -- ## CORTEX-CUSTOMER: Please add relevant Follow On Document Type. Value 'Z' represents Partial Payment against an open invoice
    AccountingInvoicesKPI.FollowOnDocumentType_REBZT = 'Z',
    AccountingInvoicesKPI.AmountInLocalCurrency_DMBTR,
    0
  ) AS OverdueOnPastDateInSourceCurrency,

  IF(
    -- ## CORTEX-CUSTOMER: Please add relevant Account Type. Value 'K' represents 'Vendor'
    AccountingInvoicesKPI.AccountType_KOART = 'K'
    AND AccountingInvoicesKPI.PostingDateInTheDocument_BUDAT < CURRENT_DATE()
    AND AccountingInvoicesKPI.NetDueDate < CURRENT_DATE()
    AND AccountingInvoicesKPI.ClearingDate_AUGDT IS NULL,
    AccountingInvoicesKPI.AmountInLocalCurrency_DMBTR * CurrencyConversion.ExchangeRate_UKURS,
    0
  )
  + IF(
    -- ## CORTEX-CUSTOMER: Please add relevant Follow On Document Type. Value 'Z' represents Partial Payment against an open invoice
    AccountingInvoicesKPI.FollowOnDocumentType_REBZT = 'Z',
    AccountingInvoicesKPI.AmountInLocalCurrency_DMBTR * CurrencyConversion.ExchangeRate_UKURS,
    0
  ) AS OverdueOnPastDateInTargetCurrency,

  /* Partial Payments */
  IF(
    -- ## CORTEX-CUSTOMER: Please add relevant Follow On Document Type. Value 'Z' represents Partial Payment against an open invoice
    AccountingInvoicesKPI.FollowOnDocumentType_REBZT = 'Z',
    AccountingInvoicesKPI.AmountInLocalCurrency_DMBTR,
    0
  ) AS PartialPaymentsInSourceCurrency,

  IF(
    -- ## CORTEX-CUSTOMER: Please add relevant Follow On Document Type. Value 'Z' represents Partial Payment against an open invoice
    AccountingInvoicesKPI.FollowOnDocumentType_REBZT = 'Z',
    AccountingInvoicesKPI.AmountInLocalCurrency_DMBTR * CurrencyConversion.ExchangeRate_UKURS,
    0
  ) AS PartialPaymentsInTargetCurrency,

  /* Late Payments */
  IF(
    -- ## CORTEX-CUSTOMER: Please add relevant Account Type. Value 'K' represents 'Vendor'
    AccountingInvoicesKPI.AccountType_KOART = 'K' AND AccountingInvoicesKPI.ClearingDate_AUGDT > AccountingInvoicesKPI.NetDueDate,
    AccountingInvoicesKPI.AmountInLocalCurrency_DMBTR,
    0
  ) AS LatePaymentsInSourceCurrency,

  IF(
    -- ## CORTEX-CUSTOMER: Please add relevant Account Type. Value 'K' represents 'Vendor'
    AccountingInvoicesKPI.AccountType_KOART = 'K' AND AccountingInvoicesKPI.ClearingDate_AUGDT > AccountingInvoicesKPI.NetDueDate,
    AccountingInvoicesKPI.AmountInLocalCurrency_DMBTR * CurrencyConversion.ExchangeRate_UKURS,
    0
  ) AS LatePaymentsInTargetCurrency,

  /* Upcoming Payments */
  IF(
    -- ## CORTEX-CUSTOMER: Please add relevant Account Type. Value 'K' represents 'Vendor'
    -- ## CORTEX-CUSTOMER: Please adjust the number of days for upcoming payments
    AccountingInvoicesKPI.AccountType_KOART = 'K'
    AND AccountingInvoicesKPI.NetDueDate BETWEEN CURRENT_DATE() AND DATE_ADD(CURRENT_DATE(), INTERVAL 14 DAY)
    AND AccountingInvoicesKPI.ClearingDate_AUGDT IS NULL,
    AccountingInvoicesKPI.AmountInLocalCurrency_DMBTR,
    0
  ) AS UpcomingPaymentsInSourceCurrency,

  IF(
    -- ## CORTEX-CUSTOMER: Please add relevant Account Type. Value 'K' represents 'Vendor'
    -- ## CORTEX-CUSTOMER: Please adjust the number of days for upcoming payments
    AccountingInvoicesKPI.AccountType_KOART = 'K'
    AND AccountingInvoicesKPI.NetDueDate BETWEEN CURRENT_DATE() AND DATE_ADD(CURRENT_DATE(), INTERVAL 14 DAY)
    AND AccountingInvoicesKPI.ClearingDate_AUGDT IS NULL,
    AccountingInvoicesKPI.AmountInLocalCurrency_DMBTR * CurrencyConversion.ExchangeRate_UKURS,
    0
  ) AS UpcomingPaymentsInTargetCurrency,

  /* Potential Penalty */
  IF(
    -- ## CORTEX-CUSTOMER: Please add relevant Account Type. Value 'K' represents 'Vendor'
    AccountingInvoicesKPI.AccountType_KOART = 'K' AND AccountingInvoicesKPI.ClearingDate_AUGDT > AccountingInvoicesKPI.NetDueDate,
    AccountingInvoicesKPI.AmountInLocalCurrency_DMBTR,
    0
  ) * COALESCE(SAFE_CAST(VendorConfig.LowField_LOW AS INT64), 0) AS PotentialPenaltyInSourceCurrency,

  IF(
    -- ## CORTEX-CUSTOMER: Please add relevant Account Type. Value 'K' represents 'Vendor'
    AccountingInvoicesKPI.AccountType_KOART = 'K' AND AccountingInvoicesKPI.ClearingDate_AUGDT > AccountingInvoicesKPI.NetDueDate,
    AccountingInvoicesKPI.AmountInLocalCurrency_DMBTR * CurrencyConversion.ExchangeRate_UKURS,
    0
  ) * COALESCE(SAFE_CAST(VendorConfig.LowField_LOW AS INT64), 0) AS PotentialPenaltyInTargetCurrency,

  /* Purchase */
  IF(
    -- ## CORTEX-CUSTOMER: Please add relevant Movement Types.
    -- Value '101' represents 'GR Goods Receipt' and '501' represents 'Receipt w/o PO'
    AccountingInvoicesKPI.AccountType_KOART = 'M' AND POOrderHistory.MovementType__inventoryManagement___BWART IN ('101', '501'),
    AccountingInvoicesKPI.AmountInLocalCurrency_DMBTR,
    IF(
      -- ## CORTEX-CUSTOMER: Please add relevant Movement Types.
      -- Value '102' represents 'GR for PO reversal (full or any one of the line item)'
      -- Value '502' represents 'Return Receipt w/o PO' (Receipt made against movement type 501 document is cancelled.)
      AccountingInvoicesKPI.AccountType_KOART = 'M' AND POOrderHistory.MovementType__inventoryManagement___BWART IN ('102', '502'),
      AccountingInvoicesKPI.AmountInLocalCurrency_DMBTR * -1, 0
    )
  ) AS PurchaseInSourceCurrency,

  IF(
    -- ## CORTEX-CUSTOMER: Please add relevant Movement Types.
    -- Value '101' represents 'GR Goods Receipt' and '501' represents 'Receipt w/o PO'
    AccountingInvoicesKPI.AccountType_KOART = 'M' AND POOrderHistory.MovementType__inventoryManagement___BWART IN ('101', '501'),
    AccountingInvoicesKPI.AmountInLocalCurrency_DMBTR * CurrencyConversion.ExchangeRate_UKURS,
    IF(
      -- ## CORTEX-CUSTOMER: Please add relevant Movement Types.
      -- Value '102' represents 'GR for PO reversal (full or any one of the line item)'
      -- Value '502' represents 'Return Receipt w/o PO' (Receipt made against movement type 501 document is cancelled.)
      AccountingInvoicesKPI.AccountType_KOART = 'M' AND POOrderHistory.MovementType__inventoryManagement___BWART IN ('102', '502'),
      AccountingInvoicesKPI.AmountInLocalCurrency_DMBTR * CurrencyConversion.ExchangeRate_UKURS * -1, 0
    )
  ) AS PurchaseInTargetCurrency,

  /* Parked Invoices */
  -- ## CORTEX-CUSTOMER: Please add relevant Invoice Status. Value 'A' represents that the document is Parked and not posted
  IF(AccountingInvoicesKPI.Invstatus_RBSTAT = 'A', TRUE, FALSE) AS IsParkedInvoice,

  /* Blocked Invoices */
  -- ## CORTEX-CUSTOMER: Please add relevant Payment Block Keys. Value 'A' represents 'Locked for Payment' and 'B' represents 'Blocked for Payment'
  IF(AccountingInvoicesKPI.PaymentBlockKey_ZLSPR IN ('A', 'B'), TRUE, FALSE) AS IsBlockedInvoice,

  /* Cash Discount Received */
  IF(
    -- ## CORTEX-CUSTOMER: Please add relevant Document Types. Value 'KZ' represents 'Vendor Payment' and 'ZP' represents 'Payment Posting'
    -- ## CORTEX-CUSTOMER: Please add relevant Transaction Key. Value 'SKE' represents 'Cash Discount Received'
    AccountingInvoicesKPI.AccountingDocumenttype_BLART IN ('KZ', 'ZP') AND AccountingInvoicesKPI.TransactionKey_KTOSL = 'SKE',
    AccountingInvoicesKPI.AmountInLocalCurrency_DMBTR,
    0
  ) AS CashDiscountReceivedInSourceCurrency,

  IF(
    -- ## CORTEX-CUSTOMER: Please add relevant Document Types. Value 'KZ' represents 'Vendor Payment' and 'ZP' represents 'Payment Posting'
    -- ## CORTEX-CUSTOMER: Please add relevant Transaction Key. Value 'SKE' represents 'Cash Discount Received'
    AccountingInvoicesKPI.AccountingDocumenttype_BLART IN ('KZ', 'ZP') AND AccountingInvoicesKPI.TransactionKey_KTOSL = 'SKE',
    AccountingInvoicesKPI.AmountInLocalCurrency_DMBTR * CurrencyConversion.ExchangeRate_UKURS,
    0
  ) AS CashDiscountReceivedInTargetCurrency,

  /* Target Cash Discount */
  IF(
    -- ## CORTEX-CUSTOMER: Please add relevant Posting Key. Value '31' represents 'Vendor Invoice'
    AccountingInvoicesKPI.PostingKey_BSCHL = '31',
    (AccountingInvoicesKPI.AmountEligibleForCashDiscountInDocumentCurrency_SKFBT * AccountingInvoicesKPI.CashDiscountPercentage1_ZBD1P) / 100,
    0
  ) AS TargetCashDiscountInSourceCurrency,

  IF(
    -- ## CORTEX-CUSTOMER: Please add relevant Posting Key. Value '31' represents 'Vendor Invoice'
    AccountingInvoicesKPI.PostingKey_BSCHL = '31',
    (AccountingInvoicesKPI.AmountEligibleForCashDiscountInDocumentCurrency_SKFBT * CurrencyConversion.ExchangeRate_UKURS * AccountingInvoicesKPI.CashDiscountPercentage1_ZBD1P) / 100,
    0
  ) AS TargetCashDiscountInTargetCurrency,

  /* Amount Of Open Debit Items */
  IF(
    -- ## CORTEX-CUSTOMER: Please add relevant Account Type. Value 'K' represents 'Vendor'
    -- ## CORTEX-CUSTOMER: Please add relevant Special GI Indicator. Value 'A' represents 'Down Payment'
    AccountingInvoicesKPI.Accounttype_KOART = 'K' AND AccountingInvoicesKPI.SpecialGlIndicator_UMSKZ = 'A',
    AccountingInvoicesKPI.AmountInLocalCurrency_DMBTR,
    0
  ) AS AmountOfOpenDebitItemsInSourceCurrency,

  IF(
    -- ## CORTEX-CUSTOMER: Please add relevant Account Type. Value 'K' represents 'Vendor'
    -- ## CORTEX-CUSTOMER: Please add relevant Special GI Indicator. Value 'A' represents 'Down Payment'
    AccountingInvoicesKPI.Accounttype_KOART = 'K' AND AccountingInvoicesKPI.SpecialGlIndicator_UMSKZ = 'A',
    AccountingInvoicesKPI.AmountInLocalCurrency_DMBTR * CurrencyConversion.ExchangeRate_UKURS,
    0
  ) AS AmountOfOpenDebitItemsInTargetCurrency,

  /* Amount Of Return */
  IF(
    -- ## CORTEX-CUSTOMER: Please add relevant Movement Type. Value '122' represents 'RE return to Vendor'
    POOrderHistory.MovementType__inventoryManagement___BWART = '122',
    POOrderHistory.AmountInLocalCurrency_DMBTR * POOrderHistory.Quantity_MENGE,
    0
  ) AS AmountOfReturnInSourceCurrency,

  IF(
    -- ## CORTEX-CUSTOMER: Please add relevant Movement Type. Value '122' represents 'RE return to Vendor'
    POOrderHistory.MovementType__inventoryManagement___BWART = '122',
    POOrderHistory.AmountInLocalCurrency_DMBTR * CurrencyConversion.ExchangeRate_UKURS * POOrderHistory.Quantity_MENGE,
    0
  ) AS AmountOfReturnInTargetCurrency

FROM AccountingInvoicesKPI
LEFT OUTER JOIN Vendors
  ON
    AccountingInvoicesKPI.Client_MANDT = Vendors.Client_MANDT
    AND AccountingInvoicesKPI.AccountNumberOfVendorOrCreditor_LIFNR = Vendors.AccountNumberOfVendorOrCreditor_LIFNR
LEFT OUTER JOIN
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.VendorConfig` AS VendorConfig
  -- ## CORTEX-CUSTOMER Vendor Name in the config follows the format 'Z_VENDOR_{VendorId}'. Please change the logic if the name follows a different format.
  ON Vendors.AccountNumberOfVendorOrCreditor_LIFNR = ARRAY_REVERSE(SPLIT(VendorConfig.NameOfVariantVariable_NAME, '_'))[SAFE_OFFSET(0)]
LEFT JOIN `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.PurchaseDocumentsHistory` AS POOrderHistory
  ON
    AccountingInvoicesKPI.Client_MANDT = POOrderHistory.Client_MANDT
    AND AccountingInvoicesKPI.PurchasingDocumentNumber_EBELN = POOrderHistory.PurchasingDocumentNumber_EBELN
    AND AccountingInvoicesKPI.ItemNumberOfPurchasingDocument_EBELP = POOrderHistory.ItemNumberOfPurchasingDocument_EBELP
    AND AccountingInvoicesKPI.FiscalYear_GJAHR = POOrderHistory.MaterialDocumentYear_GJAHR
    AND AccountingInvoicesKPI.ObjectKey_AWKEY = CONCAT(POOrderHistory.NumberOfMaterialDocument_BELNR, POOrderHistory.MaterialDocumentYear_GJAHR)
LEFT JOIN CurrencyConversion
  ON
    AccountingInvoicesKPI.Client_MANDT = CurrencyConversion.Client_MANDT
    AND AccountingInvoicesKPI.CurrencyKey_WAERS = CurrencyConversion.FromCurrency_FCURR
    AND AccountingInvoicesKPI.PostingDateInTheDocument_BUDAT = CurrencyConversion.ConvDate
