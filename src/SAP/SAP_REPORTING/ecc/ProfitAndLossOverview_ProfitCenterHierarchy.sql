--'ProfitAndLossOverview_ProfitCenterHierarchy' - A view built as reference to show how fsv
-- and profit center hierarchy can be joined.
--It is not designed for extensive reporting or analytical use.

--The granularity of this query is Client,Company,ChartOfAccounts,HierarchyName,BusinessArea,
--ProfitCenter,CostCenter,LedgerInGeneralLedgerAccounting,FiscalYear,FiscalPeriod, GL Node,
--ProfitCenter Hierarchy Node,Language,TargetCurrency.
SELECT
  ProfitAndLoss.Client,
  ProfitAndLoss.CompanyCode,
  ProfitAndLoss.FiscalYear,
  ProfitAndLoss.FiscalPeriod,
  ProfitAndLoss.ChartOfAccounts,
  ProfitAndLoss.GLHierarchy,
  ProfitAndLoss.BusinessArea,
  ProfitAndLoss.LedgerInGeneralLedgerAccounting,
  ProfitAndLoss.GLNode,
  ProfitAndLoss.LanguageKey_SPRAS,
  ProfitAndLoss.GLParent,
  ProfitAndLoss.GLFinancialItem,
  ProfitAndLoss.GLNodeText,
  ProfitAndLoss.GLParentText,
  ProfitAndLoss.GLLevel,
  ProfitAndLoss.FiscalQuarter,
  ProfitAndLoss.GLIsLeafNode,
  ProfitAndLoss.CompanyText,
  ProfitAndLoss.ProfitCenter,
  ProfitAndLoss.CostCenter,
  ProfitCenters.hiername AS PCHierarchy,
  ProfitCenters.parent AS PCParent,
  ProfitCenters.node AS PCNode,
  ProfitCenters.level AS PCLevel,
  ProfitCenters.isleafnode AS PCIsLeafNode,
  PCParentText.ShortDescriptionOfSet_DESCRIPT AS PCParentText,
  COALESCE(PCNodeText.ShortDescriptionOfSet_DESCRIPT, PCText.LongText_LTEXT) AS PCNodeText,
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
LEFT JOIN
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.profit_centers` AS ProfitCenters
  ON
    ProfitAndLoss.Client = ProfitCenters.mandt
    AND ProfitAndLoss.ProfitCenter = ProfitCenters.profitcenter
    -- ##CORTEX-CUSTOMER Update the profit center hierarchy name
    AND ProfitCenters.hiername = 'C001P'
LEFT JOIN
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.ProfitCenterHierarchiesMD` AS PCParentText
  ON
    ProfitAndLoss.Client = PCParentText.Client_MANDT
    AND ProfitCenters.setclass = PCParentText.SetClass_SETCLASS
    AND ProfitCenters.subclass = PCParentText.OrganizationalUnit_SUBCLASS
    AND PCParentText.LanguageKey_LANGU = ProfitAndLoss.LanguageKey_SPRAS
    AND PCParentText.SetName_SETNAME = ProfitCenters.parent
LEFT JOIN
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.ProfitCenterHierarchiesMD` AS PCNodeText
  ON
    ProfitAndLoss.Client = PCNodeText.Client_MANDT
    AND ProfitCenters.setclass = PCNodeText.SetClass_SETCLASS
    AND ProfitCenters.subclass = PCNodeText.OrganizationalUnit_SUBCLASS
    AND PCNodeText.LanguageKey_LANGU = ProfitAndLoss.LanguageKey_SPRAS
    AND PCNodeText.SetName_SETNAME = ProfitCenters.node
LEFT JOIN
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.ProfitCentersMD` AS PCText
  ON
    ProfitAndLoss.Client = PCText.Client_MANDT
    AND ProfitCenters.subclass = PCText.ControllingArea_KOKRS
    AND ProfitCenters.node = PCText.ProfitCenter_PRCTR
    AND PCText.Language_SPRAS = ProfitAndLoss.LanguageKey_SPRAS
    AND PCText.ValidToDate_DATBI >= '9999-12-31'
