--The granularity of this query is Client,Setclass,Subclass,HierarchyName(hierabase),
--ProfitCenterNode,ProfitCenter(prctr),Language Key.
--## CORTEX-CUSTOMER Please filter on Hierbase in case of multiple hierarchies flattened in your system.
WITH
  LanguageKey AS (
    SELECT
      LanguageKey_SPRAS
    FROM
      `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.Languages_T002`
    WHERE LanguageKey_SPRAS IN UNNEST({{ sap_languages }})
  )
SELECT
  ProfitCenters.mandt AS Client_MANDT,
  ProfitCenters.setclass AS HierarchyClass_SETCLASS,
  ProfitCenters.subclass AS HierarchySubClass_SUBCLASS,
  ProfitCenters.hiername AS HierarchyType_HIERBASE,
  LanguageKey.LanguageKey_SPRAS,
  ProfitCenters.profitcenter AS ProfitCenter_PRCTR,
  ProfitCenters.node AS ProfitCenterNode,
  ProfitCenters.parent AS ParentNode,
  --The following text fields are language dependent.
  PCParentText.ShortDescriptionOfSet_DESCRIPT AS ParentNodeText,
  COALESCE(PCNodeText.ShortDescriptionOfSet_DESCRIPT, PCText.LongText_LTEXT) AS ProfitCenterNodeText,
  ProfitCenters.level AS Level,
  ProfitCenters.isleafnode AS IsLeafNode
FROM
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.profit_centers` AS ProfitCenters
CROSS JOIN LanguageKey
LEFT JOIN
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.ProfitCenterHierarchiesMD` AS PCParentText
  ON
    ProfitCenters.mandt = PCParentText.Client_MANDT
    AND ProfitCenters.setclass = PCParentText.SetClass_SETCLASS
    AND ProfitCenters.subclass = PCParentText.OrganizationalUnit_SUBCLASS
    AND PCParentText.LanguageKey_LANGU = LanguageKey.LanguageKey_SPRAS
    AND PCParentText.SetName_SETNAME = ProfitCenters.parent
LEFT JOIN
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.ProfitCenterHierarchiesMD` AS PCNodeText
  ON
    ProfitCenters.mandt = PCNodeText.Client_MANDT
    AND ProfitCenters.setclass = PCNodeText.SetClass_SETCLASS
    AND ProfitCenters.subclass = PCNodeText.OrganizationalUnit_SUBCLASS
    AND PCNodeText.LanguageKey_LANGU = LanguageKey.LanguageKey_SPRAS
    AND PCNodeText.SetName_SETNAME = ProfitCenters.node
LEFT JOIN
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.ProfitCentersMD` AS PCText
  ON
    ProfitCenters.mandt = PCText.Client_MANDT
    AND ProfitCenters.subclass = PCText.ControllingArea_KOKRS
    AND ProfitCenters.node = PCText.ProfitCenter_PRCTR
    AND PCText.Language_SPRAS = LanguageKey.LanguageKey_SPRAS
    AND PCText.ValidToDate_DATBI = '9999-12-31'
