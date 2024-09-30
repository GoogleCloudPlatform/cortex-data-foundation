--This query outputs FinancialStatement by Client,Company,ChartOfAccounts,HierarchyName,
--BusinessArea,LedgerInGeneralLedgerAccounting,ProfitCenter,CostCenter,FiscalYear,
--FiscalPeriod,Hierarchy Node.
WITH
  FSVGLAccounts AS (
    SELECT
      --This query joins transaction data with hierarchy node at GLAccount level.
      AccountingDocument.client AS Client,
      AccountingDocument.glaccount AS GeneralLedgerAccount,
      FinancialStatementVersion.chartofaccounts AS ChartOfAccounts,
      FinancialStatementVersion.hiername AS HierarchyName,
      FinancialStatementVersion.hierarchyversion AS HierarchyVersion,
      AccountingDocument.companycode AS CompanyCode,
      AccountingDocument.businessarea AS BusinessArea,
      AccountingDocument.ledger AS LedgerInGeneralLedgerAccounting,
      AccountingDocument.profitcenter AS ProfitCenter,
      AccountingDocument.costcenter AS CostCenter,
      IF(
        AccountingDocument.balancesheetandplaccountindicator = 'X',
        AccountingDocument.balancesheetandplaccountindicator, NULL
      ) AS BalanceSheetAccountIndicator,
      IF(
        AccountingDocument.balancesheetandplaccountindicator IN ('P', 'N'),
        AccountingDocument.balancesheetandplaccountindicator, NULL
      ) AS PLAccountIndicator,
      AccountingDocument.fiscalyear AS FiscalYear,
      AccountingDocument.fiscalperiod AS FiscalPeriod,
      AccountingDocument.fiscalquarter AS FiscalQuarter,
      FinancialStatementVersion.parent AS Parent,
      FinancialStatementVersion.node AS Node,
      FinancialStatementVersion.nodevalue AS FinancialStatementItem,
      FinancialStatementVersion.level AS Level,
      FinancialStatementVersion.isleafnode AS IsLeafNode,
      AccountingDocument.amount AS AmountInLocalCurrency,
      AccountingDocument.currency AS CurrencyKey,
      AccountingDocument.companytext AS CompanyText
    FROM
      `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.financial_statement` AS AccountingDocument
    INNER JOIN
      `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.fsv_glaccounts` AS FinancialStatementVersion
      ON
        AccountingDocument.client = FinancialStatementVersion.mandt
        AND AccountingDocument.glaccount = FinancialStatementVersion.glaccount
  )

SELECT
  --This query outputs transaction data at hierarchy(fsv) node level.
  Client,
  CompanyCode,
  FiscalYear,
  FiscalPeriod,
  ChartOfAccounts,
  HierarchyName,
  BusinessArea,
  LedgerInGeneralLedgerAccounting,
  ProfitCenter,
  CostCenter,
  Node,
  ANY_VALUE(HierarchyVersion) AS HierarchyVersion,
  ANY_VALUE(Parent) AS Parent,
  ANY_VALUE(FiscalQuarter) AS FiscalQuarter,
  ANY_VALUE(FinancialStatementItem) AS FinancialStatementItem,
  ANY_VALUE(Level) AS Level,
  ANY_VALUE(IsLeafNode) AS IsLeafNode,
  ANY_VALUE(BalanceSheetAccountIndicator) AS BalanceSheetAccountIndicator,
  ANY_VALUE(PLAccountIndicator) AS PLAccountIndicator,
  ANY_VALUE(CompanyText) AS CompanyText,
  ANY_VALUE(CurrencyKey) AS CurrencyKey,
  SUM(AmountInLocalCurrency) AS AmountInLocalCurrency,
  SUM(SUM(AmountInLocalCurrency))
    OVER ( --noqa: disable=L003
      PARTITION BY
        Client, CompanyCode, BusinessArea, LedgerInGeneralLedgerAccounting,
        ProfitCenter, CostCenter, Node
      ORDER BY
        FiscalYear ASC, FiscalPeriod ASC
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS CumulativeAmountInLocalCurrency --noqa: enable=all

FROM
  FSVGLAccounts

GROUP BY
  Client,
  CompanyCode,
  ChartOfAccounts,
  HierarchyName,
  BusinessArea,
  LedgerInGeneralLedgerAccounting,
  ProfitCenter,
  CostCenter,
  FiscalYear,
  FiscalPeriod,
  Node
