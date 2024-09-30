--'NetProfitOverview' - A view built as reference for details of calculations
--implemented in dashboards.
--It is not designed for extensive reporting or analytical use.
--The granularity of this query is Client,Company,ChartOfAccounts,HierarchyName,BusinessArea,
--ProfitCenter,LedgerInGeneralLedgerAccounting,FiscalYear,FiscalPeriod,TargetCurrency.
WITH
  Expenses AS (
    SELECT
      Client,
      CompanyCode,
      FiscalYear,
      FiscalPeriod,
      MAX(FiscalQuarter) AS FiscalQuarter,
      ChartOfAccounts,
      GLHierarchy,
      BusinessArea,
      LedgerInGeneralLedgerAccounting,
      ProfitCenter,
      MAX(CompanyText) AS CompanyText,
      SUM(AmountInLocalCurrency) AS AmountInLocalCurrency,
      SUM(CumulativeAmountInLocalCurrency) AS CumulativeAmountInLocalCurrency,
      MAX(CurrencyKey) AS CurrencyKey,
      SUM(AmountInTargetCurrency) AS AmountInTargetCurrency,
      SUM(CumulativeAmountInTargetCurrency) AS CumulativeAmountInTargetCurrency,
      TargetCurrency_TCURR
    FROM
      `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.ProfitAndLoss`
    WHERE
      --## CORTEX-CUSTOMER Following Hierarchy Nodes are being used in the view,
      --Update the GLNodes as per your requirement
      --018 - Operating Expense
      --037 - Non-Operating Expense
      --069 - Interest Expense
      --035 - Foreign Currency Expense
      GLNode IN (
        '018',
        '037',
        '069',
        '035'
      )
      --ProfitAndLoss view is language dependent. However the final view is language indpendent.
      --The where clause makes sure that language dependency is handled and
      --the amount is aggregated at correct granularity.
      AND LanguageKey_SPRAS = (
        SELECT MAX(LanguageKey_SPRAS)
        FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.ProfitAndLoss`
      )
    GROUP BY
      Client,
      CompanyCode,
      FiscalYear,
      FiscalPeriod,
      ChartOfAccounts,
      GLHierarchy,
      BusinessArea,
      LedgerInGeneralLedgerAccounting,
      ProfitCenter,
      TargetCurrency_TCURR
  ),

  OtherIncome AS (
    SELECT
      Client,
      CompanyCode,
      FiscalYear,
      FiscalPeriod,
      MAX(FiscalQuarter) AS FiscalQuarter,
      ChartOfAccounts,
      GLHierarchy,
      BusinessArea,
      LedgerInGeneralLedgerAccounting,
      ProfitCenter,
      MAX(CompanyText) AS CompanyText,
      SUM(AmountInLocalCurrency) AS AmountInLocalCurrency,
      SUM(CumulativeAmountInLocalCurrency) AS CumulativeAmountInLocalCurrency,
      MAX(CurrencyKey) AS CurrencyKey,
      SUM(AmountInTargetCurrency) AS AmountInTargetCurrency,
      SUM(CumulativeAmountInTargetCurrency) AS CumulativeAmountInTargetCurrency,
      TargetCurrency_TCURR
    FROM
      `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.ProfitAndLoss`
    WHERE
      --## CORTEX-CUSTOMER Following Hierarchy Nodes are being used in the view,
      --Update the GLNodes as per your requirement
      --036 - Non-Operating Revenue
      --010 - Foreign Currency Income
      --068 - Interest Income
      GLNode IN (
        '036',
        '010',
        '068'
      )
      --ProfitAndLoss view is language dependent. However the final view is language indpendent.
      --The where clause makes sure that language dependency is handled and
      --the amount is aggregated at correct granularity.
      AND LanguageKey_SPRAS = (
        SELECT MAX(LanguageKey_SPRAS)
        FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.ProfitAndLoss`
      )
    GROUP BY
      Client,
      CompanyCode,
      FiscalYear,
      FiscalPeriod,
      ChartOfAccounts,
      GLHierarchy,
      BusinessArea,
      LedgerInGeneralLedgerAccounting,
      ProfitCenter,
      TargetCurrency_TCURR
  )

SELECT
  GrossProfit.Client,
  GrossProfit.CompanyCode,
  GrossProfit.FiscalYear,
  GrossProfit.FiscalPeriod,
  GrossProfit.FiscalQuarter,
  GrossProfit.ChartOfAccounts,
  GrossProfit.GLHierarchy,
  GrossProfit.BusinessArea,
  GrossProfit.LedgerInGeneralLedgerAccounting,
  GrossProfit.ProfitCenter,
  GrossProfit.CompanyText,
  GrossProfit.AmountInLocalCurrency,
  GrossProfit.CumulativeAmountInLocalCurrency,
  GrossProfit.CurrencyKey,
  GrossProfit.AmountInTargetCurrency,
  GrossProfit.CumulativeAmountInTargetCurrency,
  GrossProfit.TargetCurrency_TCURR,
  GrossProfit.GrossProfitInLocalCurrency,
  GrossProfit.GrossProfitInTargetCurrency,
  (
    GrossProfit.GrossProfitInLocalCurrency - Expenses.AmountInLocalCurrency
    + OtherIncome.AmountInLocalCurrency
  ) AS NetProfitInLocalCurrency,
  (
    GrossProfit.GrossProfitInTargetCurrency - Expenses.AmountInTargetCurrency
    + OtherIncome.AmountInTargetCurrency
  ) AS NetProfitInTargetCurrency

FROM
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.GrossProfitOverview` AS GrossProfit
INNER JOIN
  Expenses
  ON
    Expenses.Client = GrossProfit.CLient
    AND Expenses.CompanyCode = GrossProfit.CompanyCode
    AND Expenses.FiscalYear = GrossProfit.FiscalYear
    AND Expenses.FiscalPeriod = GrossProfit.FiscalPeriod
    AND Expenses.ChartOfAccounts = GrossProfit.ChartOfAccounts
    AND Expenses.GLHierarchy = GrossProfit.GLHierarchy
    AND Expenses.BusinessArea = GrossProfit.BusinessArea
    AND Expenses.LedgerInGeneralLedgerAccounting = GrossProfit.LedgerInGeneralLedgerAccounting
    AND Expenses.ProfitCenter = GrossProfit.ProfitCenter
    AND Expenses.TargetCurrency_TCURR = GrossProfit.TargetCurrency_TCURR
INNER JOIN
  OtherIncome
  ON
    OtherIncome.Client = GrossProfit.CLient
    AND OtherIncome.CompanyCode = GrossProfit.CompanyCode
    AND OtherIncome.FiscalYear = GrossProfit.FiscalYear
    AND OtherIncome.FiscalPeriod = GrossProfit.FiscalPeriod
    AND OtherIncome.ChartOfAccounts = GrossProfit.ChartOfAccounts
    AND OtherIncome.GLHierarchy = GrossProfit.GLHierarchy
    AND OtherIncome.BusinessArea = GrossProfit.BusinessArea
    AND OtherIncome.LedgerInGeneralLedgerAccounting = GrossProfit.LedgerInGeneralLedgerAccounting
    AND OtherIncome.ProfitCenter = GrossProfit.ProfitCenter
    AND OtherIncome.TargetCurrency_TCURR = GrossProfit.TargetCurrency_TCURR
