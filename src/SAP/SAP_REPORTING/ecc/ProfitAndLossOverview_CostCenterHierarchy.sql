--'ProfitAndLossOverview_CostCenterHierarchy' - A view built as reference to show how fsv
-- and cost center hierarchy can be joined.
--It is not designed for extensive reporting or analytical use.

--The granularity of this query is Client,Company,ChartOfAccounts,HierarchyName,BusinessArea,
--ProfitCenter,CostCenter,LedgerInGeneralLedgerAccounting,FiscalYear,FiscalPeriod, GL Node,
--CostCenter Hierarchy Node,Language,TargetCurrency.
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
  CostCenters.hiername AS CCHierarchy,
  CostCenters.parent AS CCParent,
  CostCenters.node AS CCNode,
  CostCenters.level AS CCLevel,
  CostCenters.isleafnode AS CCIsLeafNode,
  CCParentText.ShortDescriptionOfSet_DESCRIPT AS CCParentText,
  COALESCE(CCNodeText.ShortDescriptionOfSet_DESCRIPT, CCText.Description_LTEXT) AS CCNodeText,
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
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.cost_centers` AS CostCenters
  ON
    ProfitAndLoss.Client = CostCenters.mandt
    AND ProfitAndLoss.CostCenter = CostCenters.costcenter
    -- ##CORTEX-CUSTOMER Update the cost center hierarchy name as per your requirement
    AND CostCenters.hiername = 'C001C'
LEFT JOIN
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.CostCenterHierarchiesMD` AS CCParentText
  ON
    ProfitAndLoss.Client = CCParentText.Client_MANDT
    AND CostCenters.setclass = CCParentText.SetClass_SETCLASS
    AND CostCenters.subclass = CCParentText.OrganizationalUnit_SUBCLASS
    AND ProfitAndLoss.LanguageKey_SPRAS = CCParentText.LanguageKey_LANGU
    AND CCParentText.SetName_SETNAME = CostCenters.parent
LEFT JOIN
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.CostCenterHierarchiesMD` AS CCNodeText
  ON
    ProfitAndLoss.Client = CCNodeText.Client_MANDT
    AND CostCenters.setclass = CCNodeText.SetClass_SETCLASS
    AND CostCenters.subclass = CCNodeText.OrganizationalUnit_SUBCLASS
    AND ProfitAndLoss.LanguageKey_SPRAS = CCNodeText.LanguageKey_LANGU
    AND CCNodeText.SetName_SETNAME = CostCenters.node
LEFT JOIN
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.CostCentersMD` AS CCText
  ON
    ProfitAndLoss.Client = CCText.Client_MANDT
    AND CostCenters.subclass = CCText.ControllingArea_KOKRS
    AND CostCenters.node = CCText.CostCenter_KOSTL
    AND ProfitAndLoss.LanguageKey_SPRAS = CCText.Language_SPRAS
    AND CCText.ValidTo_DATBI >= '9999-12-31'
