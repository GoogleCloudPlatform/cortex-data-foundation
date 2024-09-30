--'ProfitAndLossOverview' - A view built as reference for details of calculations implemented
--in dashboards.
--It is not designed for extensive reporting or analytical use.
--The granularity of this query is Client,Company,ChartOfAccounts,HierarchyName,BusinessArea,
--ProfitCenter,CostCenter,LedgerInGeneralLedgerAccounting,FiscalYear,FiscalPeriod,Hierarchy Node,
--Language,TargetCurrency.
SELECT
  ProfitAndLoss.Client,
  ProfitAndLoss.CompanyCode,
  ProfitAndLoss.FiscalYear,
  ProfitAndLoss.FiscalPeriod,
  ProfitAndLoss.FiscalQuarter,
  ProfitAndLoss.ChartOfAccounts,
  ProfitAndLoss.GLHierarchy,
  ProfitAndLoss.BusinessArea,
  ProfitAndLoss.LedgerInGeneralLedgerAccounting,
  ProfitAndLoss.ProfitCenter,
  ProfitAndLoss.CostCenter,
  ProfitAndLoss.GLNode,
  ProfitAndLoss.LanguageKey_SPRAS,
  ProfitAndLoss.GLParent,
  ProfitAndLoss.GLFinancialItem,
  ProfitAndLoss.GLNodeText,
  ProfitAndLoss.GLParentText,
  ProfitAndLoss.GLLevel,
  ProfitAndLoss.GLIsLeafNode,
  ProfitAndLoss.CompanyText,
  ProfitAndLoss.AmountInLocalCurrency,
  ProfitAndLoss.CumulativeAmountInLocalCurrency,
  ProfitAndLoss.CurrencyKey,
  ProfitAndLoss.ExchangeRate,
  ProfitAndLoss.MaxExchangeRate,
  ProfitAndLoss.AvgExchangeRate,
  ProfitAndLoss.AmountInTargetCurrency,
  ProfitAndLoss.CumulativeAmountInTargetCurrency,
  ProfitAndLoss.TargetCurrency_TCURR
FROM
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.ProfitAndLoss` AS ProfitAndLoss
WHERE
  --## CORTEX-CUSTOMER Following Hierarchy Nodes are being used in the view,
  --Update the GLNodes as per your requirement
  --013 - Net Revenue
  --021 - Cost of Goods Sold
  --015 - Employee Expense
  --020 - Building Expense
  --026 - Depreciation & Amortization
  --076 - Other Operating Expense
  --070 - Secondary Accounts
  --069 - Interest Expense
  --037 - Non-Operating Expense
  --035 - Foreign Currency Expense
  --036 - Non-Operating Revenue
  --010 - Foreign Currency Income
  --068 - Interest Income
  --018 - Operating Expense
  --017 - Gross Margin
  --075 - Interest
  --09 - Taxes
  --053 - Other Income and Expense
  --08 - Net Income (P&L)
  --022 - Gross Revenue
  --023 - Sales Deductions
  GLNode IN (
    '013', '021', '015', '020', '026', '076', '070', '018', '069', '037',
    '010', '036', '068', '017', '075', '09', '053', '08', '022', '023', '035'
  )
