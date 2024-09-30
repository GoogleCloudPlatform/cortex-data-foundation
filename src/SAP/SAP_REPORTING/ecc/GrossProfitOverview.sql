--'GrossProfitOverview' - A view built as reference for details of calculations
--implemented in dashboards.
--It is not designed for extensive reporting or analytical use.
--The granularity of this query is Client,Company,ChartOfAccounts,HierarchyName,BusinessArea,
--ProfitCenter,LedgerInGeneralLedgerAccounting,FiscalYear,FiscalPeriod,TargetCurrency.
WITH
  NetRevenue AS (
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
    FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.ProfitAndLoss`
    WHERE
      --## CORTEX-CUSTOMER Following Hierarchy Node is being used in the view,
      --Update the GLNode as per your requirement
      --000239 - Net Revenue
      GLNode = '000239'
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

  CostOfGoodsSold AS (
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
    FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.ProfitAndLoss`
    WHERE
      --## CORTEX-CUSTOMER Following Hierarchy Node is being used in the view,
      --Update the GLNode as per your requirement
      --000266 - Cost of Goods Sold
      GLNode = '000266'
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
  NetRevenue.Client,
  NetRevenue.CompanyCode,
  NetRevenue.FiscalYear,
  NetRevenue.FiscalPeriod,
  NetRevenue.FiscalQuarter,
  NetRevenue.ChartOfAccounts,
  NetRevenue.GLHierarchy,
  NetRevenue.BusinessArea,
  NetRevenue.LedgerInGeneralLedgerAccounting,
  NetRevenue.ProfitCenter,
  NetRevenue.CompanyText,
  NetRevenue.AmountInLocalCurrency,
  NetRevenue.CumulativeAmountInLocalCurrency,
  NetRevenue.CurrencyKey,
  NetRevenue.AmountInTargetCurrency,
  NetRevenue.CumulativeAmountInTargetCurrency,
  NetRevenue.TargetCurrency_TCURR,
  (
    NetRevenue.AmountInLocalCurrency - CostOfGoodsSold.AmountInLocalCurrency
  ) AS GrossProfitInLocalCurrency,
  (
    NetRevenue.AmountInTargetCurrency - CostOfGoodsSold.AmountInTargetCurrency
  ) AS GrossProfitInTargetCurrency
FROM
  NetRevenue
INNER JOIN
  CostOfGoodsSold
  ON
    NetRevenue.Client = CostOfGoodsSold.Client
    AND NetRevenue.CompanyCode = CostOfGoodsSold.CompanyCode
    AND NetRevenue.FiscalYear = CostOfGoodsSold.FiscalYear
    AND NetRevenue.FiscalPeriod = CostOfGoodsSold.FiscalPeriod
    AND NetRevenue.ChartOfAccounts = CostOfGoodsSold.ChartOfAccounts
    AND NetRevenue.GLHierarchy = CostOfGoodsSold.GLHierarchy
    AND COALESCE(NetRevenue.BusinessArea, '') = COALESCE(CostOfGoodsSold.BusinessArea, '')
    AND NetRevenue.LedgerInGeneralLedgerAccounting = CostOfGoodsSold.LedgerInGeneralLedgerAccounting
    AND NetRevenue.ProfitCenter = CostOfGoodsSold.ProfitCenter
    AND NetRevenue.TargetCurrency_TCURR = CostOfGoodsSold.TargetCurrency_TCURR
