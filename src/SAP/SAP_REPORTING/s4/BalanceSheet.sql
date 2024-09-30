--The granularity of this query is Client,Company,ChartOfAccounts,HierarchyName,BusinessArea,
--LedgerInGeneralLedgerAccounting,FiscalYear,FiscalPeriod,Hierarchy Node,Language,TargetCurrency.
WITH
  LanguageKey AS (
    SELECT
      LanguageKey_SPRAS
    FROM
      `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.Languages_T002`
    WHERE LanguageKey_SPRAS IN UNNEST({{ sap_languages }})
  ),

  CurrencyConversion AS (
    SELECT
      Companies.Client_MANDT,
      MAX(Companies.FiscalYearVariant_PERIV) AS periv,
      Companies.CompanyCode_BUKRS,
      FiscalDateDimension.FiscalYear,
      FiscalDateDimension.FiscalPeriod,
      Currency.FromCurrency_FCURR,
      Currency.ToCurrency_TCURR,
      MAX_BY(Currency.ExchangeRate_UKURS, FiscalDateDimension.Date) AS ExchangeRate,
      MAX(Currency.ExchangeRate_UKURS) AS MaxExchangeRate,
      AVG(Currency.ExchangeRate_UKURS) AS AvgExchangeRate
    FROM
      `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.fiscal_date_dim` AS FiscalDateDimension
    INNER JOIN
      `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.CompaniesMD` AS Companies
      ON
        Companies.Client_MANDT = FiscalDateDimension.MANDT
        AND Companies.FiscalYearVariant_PERIV = FiscalDateDimension.periv
        AND FiscalDateDimension.Date <= CURRENT_DATE()
    INNER JOIN
      `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.CurrencyConversion` AS Currency
      ON
        FiscalDateDimension.MANDT = Currency.Client_MANDT
        AND FiscalDateDimension.Date = Currency.ConvDate
        AND Companies.CurrencyCode_WAERS = Currency.FromCurrency_FCURR
    WHERE
      Currency.Client_MANDT = '{{ mandt }}'
      AND Currency.ToCurrency_TCURR IN UNNEST({{ sap_currencies }})
      --## CORTEX-CUSTOMER Modify the exchange rate type based on your requirement
      AND Currency.ExchangeRateType_KURST = 'M'
    GROUP BY
      Companies.Client_MANDT,
      Companies.CompanyCode_BUKRS,
      FiscalDateDimension.FiscalYear,
      FiscalDateDimension.FiscalPeriod,
      Currency.FromCurrency_FCURR,
      Currency.ToCurrency_TCURR
  ),

  ParentId AS (
    SELECT DISTINCT
      fsv_parent.Client,
      fsv_parent.CompanyCode,
      fsv_parent.Parent,
      fsv_child.FinancialStatementItem
    FROM
      (
        SELECT Client, CompanyCode, Parent
        FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.FinancialStatement`
        -- BalanceSheetAccountIndicator = 'X'represents GLAccount for BalanceSheet
        WHERE BalanceSheetAccountIndicator = 'X'
      ) AS fsv_parent
    INNER JOIN
      `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.FinancialStatement` AS fsv_child
      ON
        fsv_parent.Client = fsv_child.Client
        AND fsv_parent.Parent = fsv_child.Node
        AND fsv_parent.CompanyCode = fsv_child.CompanyCode
  )

SELECT
  FSV.Client,
  FSV.CompanyCode,
  FSV.FiscalYear,
  FSV.FiscalPeriod,
  FSV.ChartOfAccounts,
  FSV.HierarchyName,
  FSV.BusinessArea,
  FSV.LedgerInGeneralLedgerAccounting,
  FSV.Node,
  LanguageKey.LanguageKey_SPRAS,
  MAX(FSV.Parent) AS Parent,
  --The following text columns are language dependent.
  MAX(
    COALESCE(NodeText.HierarchyNodeDescription_NODETXT, GLText.GlAccountLongText_TXT50)
  ) AS NodeText,
  --For parent as root node, hierarchy name is printed as parent text.
  MAX(ParentText.HierarchyNodeDescription_NODETXT) AS ParentText,
  MAX(FSV.Level) AS Level,
  MAX(FSV.FiscalQuarter) AS FiscalQuarter,
  MAX(FSV.IsLeafNode) AS IsLeafNode,
  --The following text column is language independent.
  MAX(FSV.CompanyText) AS CompanyText,
  SUM(FSV.AmountInLocalCurrency) AS AmountInLocalCurrency,
  SUM(SUM(FSV.AmountInLocalCurrency))
    OVER ( --noqa: disable=L003
      PARTITION BY
        FSV.Client, FSV.CompanyCode, FSV.BusinessArea, FSV.LedgerInGeneralLedgerAccounting,
        FSV.Node, LanguageKey.LanguageKey_SPRAS, CurrencyConversion.ToCurrency_TCURR
      ORDER BY
        FSV.FiscalYear ASC, FSV.FiscalPeriod ASC
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS CumulativeAmountInLocalCurrency,
  MAX(FSV.CurrencyKey) AS CurrencyKey,
  -- The following columns are having amount/prices in target currency.
  MAX(CurrencyConversion.ExchangeRate) AS ExchangeRate,
  MAX(CurrencyConversion.MaxExchangeRate) AS MaxExchangeRate,
  AVG(CurrencyConversion.AvgExchangeRate) AS AvgExchangeRate,
  SUM(FSV.AmountInLocalCurrency * CurrencyConversion.ExchangeRate) AS AmountInTargetCurrency,
  SUM(SUM(FSV.AmountInLocalCurrency * CurrencyConversion.ExchangeRate))
    OVER (
      PARTITION BY
        FSV.Client, FSV.CompanyCode, FSV.BusinessArea, FSV.LedgerInGeneralLedgerAccounting,
        FSV.Node, LanguageKey.LanguageKey_SPRAS, CurrencyConversion.ToCurrency_TCURR
      ORDER BY
        FSV.FiscalYear ASC, FSV.FiscalPeriod ASC
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS CumulativeAmountInTargetCurrency, --noqa: enable=all
  CurrencyConversion.ToCurrency_TCURR AS TargetCurrency_TCURR

FROM
  (
    SELECT
      Client,
      CompanyCode,
      FiscalYear,
      FiscalPeriod,
      FiscalQuarter,
      ChartOfAccounts,
      HierarchyName,
      HierarchyVersion,
      BusinessArea,
      LedgerInGeneralLedgerAccounting,
      Node,
      Parent,
      FinancialStatementItem,
      Level,
      IsLeafNode,
      CompanyText,
      AmountInLocalCurrency,
      CurrencyKey
    FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.FinancialStatement`
    -- BalanceSheetAccountIndicator = 'X'represents GLAccount for BalanceSheet
    WHERE BalanceSheetAccountIndicator = 'X'
  ) AS FSV

LEFT JOIN ParentId
  ON
    FSV.Client = ParentId.Client
    AND FSV.Parent = ParentId.Parent
    AND FSV.CompanyCode = ParentId.CompanyCode
LEFT JOIN CurrencyConversion
  ON
    FSV.Client = CurrencyConversion.Client_MANDT
    AND FSV.CompanyCode = CurrencyConversion.CompanyCode_BUKRS
    AND FSV.CurrencyKey = CurrencyConversion.FromCurrency_FCURR
    AND FSV.FiscalYear = CurrencyConversion.FiscalYear
    AND FSV.FiscalPeriod = CurrencyConversion.FiscalPeriod
CROSS JOIN LanguageKey
LEFT JOIN
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.FSVTextsMD` AS NodeText
  ON
    FSV.Client = NodeText.Client_MANDT
    AND FSV.HierarchyName = NodeText.HierarchyID_HRYID
    AND FSV.HierarchyVersion = NodeText.HierarchyVersion_HRYVER
    AND FSV.Node = NodeText.HierarchyNode_HRYNODE
    AND NodeText.LanguageKey_SPRAS = LanguageKey.LanguageKey_SPRAS
LEFT JOIN
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.GLAccountsMD` AS GLText
  ON
    FSV.Client = GLText.Client_MANDT
    AND FSV.ChartOfAccounts = GLText.ChartOfAccounts_KTOPL
    AND FSV.FinancialStatementItem = GLText.GlAccountNumber_SAKNR
    AND GLText.Language_SPRAS = LanguageKey.LanguageKey_SPRAS
LEFT JOIN
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.FSVTextsMD` AS ParentText
  ON
    FSV.Client = ParentText.Client_MANDT
    AND FSV.HierarchyName = ParentText.HierarchyID_HRYID
    AND FSV.HierarchyVersion = ParentText.HierarchyVersion_HRYVER
    AND FSV.Parent = ParentText.HierarchyNode_HRYNODE
    AND ParentText.LanguageKey_SPRAS = LanguageKey.LanguageKey_SPRAS
GROUP BY
  FSV.Client,
  FSV.CompanyCode,
  FSV.ChartOfAccounts,
  FSV.HierarchyName,
  FSV.BusinessArea,
  FSV.LedgerInGeneralLedgerAccounting,
  FSV.FiscalYear,
  FSV.FiscalPeriod,
  FSV.Node,
  LanguageKey.LanguageKey_SPRAS,
  CurrencyConversion.ToCurrency_TCURR
