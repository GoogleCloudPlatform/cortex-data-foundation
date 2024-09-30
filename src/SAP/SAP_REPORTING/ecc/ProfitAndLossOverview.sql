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
  --000266 - Cost of Goods Sold
  --000239 - Net Revenue
  --000308 - Employee Expense
  --000333 - Building Expense
  --000344 - Depreciation & Amortization
  --000357 - Miscellaneous and Other Operating Expense
  --000364 - Advertising and Third Party Expenses
  --000412 - Interest Expense
  --000387 - Non-Operating Expense
  --000402 - Foreign Currency Expense
  --000385 - Non-Operating Revenue
  --000397 - Foreign Currency Income
  --000409 - Interest Income
  --000307 - Operating Expense
  --000238 - Gross Margin
  --000408 - Interest
  --000415 - Taxes
  --000383 - Other Income and Expense
  --000235 - Net Income (P&L)
  --000240 - Gross Revenue
  --000255 - Sales Deductions
  GLNode IN (
    '000266', '000239', '000308', '000333', '000344', '000357', '000364',
    '000412', '000387', '000402', '000385', '000397', '000409', '000307',
    '000238', '000408', '000415', '000383', '000235', '000240', '000255'
  )
