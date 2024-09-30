SELECT
  AccountingDocuments.Client_MANDT,
  AccountingDocuments.ExchangeRateType_KURST,
  AccountingDocuments.CompanyCode_BUKRS,
  CompaniesMD.CompanyText_BUTXT,
  AccountingDocuments.CustomerNumber_KUNNR,
  AccountingDocuments.FiscalYear_GJAHR,
  CustomersMD.NAME1_NAME1,
  CompaniesMD.Country_LAND1 AS Company_Country,
  CompaniesMD.CityName_ORT01 AS Company_City,
  CustomersMD.CountryKey_LAND1,
  CustomersMD.City_ORT01,
  AccountingDocuments.AccountingDocumentNumber_BELNR,
  AccountingDocuments.NumberOfLineItemWithinAccountingDocument_BUZEI,
  AccountingDocuments.CurrencyKey_WAERS,
  AccountingDocuments.LocalCurrency_HWAER,
  CompaniesMD.FiscalyearVariant_PERIV,
  FiscalDateDimension_BUDAT.FiscalYearPeriod AS Period,
  FiscalDateDimension_CURRENTDATE.FiscalYearPeriod AS Current_Period,
  AccountingDocuments.AccountType_KOART,
  AccountingDocuments.PostingDateInTheDocument_BUDAT,
  AccountingDocuments.DocumentDateInDocument_BLDAT,
  AccountingDocuments.InvoiceToWhichTheTransactionBelongs_REBZG,
  AccountingDocuments.BillingDocument_VBELN,
  AccountingDocuments.WrittenOffAmount_DMBTR,
  AccountingDocuments.BadDebt_DMBTR,
  AccountingDocuments.netDueDateCalc AS NetDueDate,
  AccountingDocuments.sk2dtCalc AS CashDiscountDate1,
  AccountingDocuments.sk1dtCalc AS CashDiscountDate2,
  AccountingDocuments.OpenAndNotDue,
  AccountingDocuments.ClearedAfterDueDate,
  AccountingDocuments.ClearedOnOrBeforeDueDate,
  AccountingDocuments.OpenAndOverDue,
  AccountingDocuments.DoubtfulReceivables,
  AccountingDocuments.DaysInArrear,
  AccountingDocuments.AccountsReceivable,
  AccountingDocuments.Sales
FROM
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.AccountingDocuments` AS AccountingDocuments
LEFT JOIN
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.CustomersMD` AS CustomersMD
  ON
    AccountingDocuments.Client_MANDT = CustomersMD.Client_MANDT
    AND AccountingDocuments.CustomerNumber_KUNNR = CustomersMD.CustomerNumber_KUNNR
LEFT JOIN
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.CompaniesMD` AS CompaniesMD
  ON
    AccountingDocuments.Client_MANDT = CompaniesMD.Client_MANDT
    AND AccountingDocuments.CompanyCode_BUKRS = CompaniesMD.CompanyCode_BUKRS
LEFT JOIN
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.fiscal_date_dim` AS FiscalDateDimension_BUDAT
  ON AccountingDocuments.Client_MANDT = FiscalDateDimension_BUDAT.MANDT
    AND CompaniesMD.FiscalyearVariant_PERIV = FiscalDateDimension_BUDAT.PERIV
    AND AccountingDocuments.PostingDateInTheDocument_BUDAT = FiscalDateDimension_BUDAT.DATE
LEFT JOIN
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.fiscal_date_dim` AS FiscalDateDimension_CURRENTDATE
  ON AccountingDocuments.Client_MANDT = FiscalDateDimension_CURRENTDATE.MANDT
    AND CompaniesMD.FiscalyearVariant_PERIV = FiscalDateDimension_CURRENTDATE.PERIV
    AND CURRENT_DATE() = FiscalDateDimension_CURRENTDATE.DATE
WHERE AccountingDocuments.AccountType_KOART = 'D'
