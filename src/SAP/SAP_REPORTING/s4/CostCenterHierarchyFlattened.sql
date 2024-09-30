--The granularity of this query is Client,Setclass,Subclass,HierarchyName(hierabase),
--CostCenterNode,CostCenter(kostl),Language Key.
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
  CostCenters.mandt AS Client_MANDT,
  CostCenters.setclass AS HierarchyClass_SETCLASS,
  CostCenters.subclass AS HierarchySubClass_SUBCLASS,
  CostCenters.hiername AS HierarchyType_HIERBASE,
  LanguageKey.LanguageKey_SPRAS,
  CostCenters.costcenter AS CostCenter_KOSTL,
  CostCenters.node AS CostCenterNode,
  CostCenters.parent AS ParentNode,
  CCParentText.SetName_SETNAME AS ParentNodeText,
  --CostCenterNodeText(Description_LTEXT) is a language dependent field.
  COALESCE(CCNodeText.SetName_SETNAME, CCText.Description_LTEXT) AS CostCenterNodeText,
  CostCenters.level AS Level,
  CostCenters.isleafnode AS IsLeafNode
FROM
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.cost_centers` AS CostCenters
LEFT JOIN
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.CostCenterHierarchiesMD` AS CCParentText
  ON
    CostCenters.mandt = CCParentText.Client_MANDT
    AND CostCenters.setclass = CCParentText.SetClass_SETCLASS
    AND CostCenters.subclass = CCParentText.OrganizationalUnit_SUBCLASS
    AND CAST(CCParentText.NodeNumber_SUCC AS STRING) = CostCenters.parent
    AND CostCenters.hiername = CCParentText.SetName_HIERBASE
LEFT JOIN
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.CostCenterHierarchiesMD` AS CCNodeText
  ON
    CostCenters.mandt = CCNodeText.Client_MANDT
    AND CostCenters.setclass = CCNodeText.SetClass_SETCLASS
    AND CostCenters.subclass = CCNodeText.OrganizationalUnit_SUBCLASS
    AND CAST(CCNodeText.NodeNumber_SUCC AS STRING) = CostCenters.node
    AND CostCenters.hiername = CCNodeText.SetName_HIERBASE
CROSS JOIN LanguageKey
LEFT JOIN
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.CostCentersMD` AS CCText
  ON
    CostCenters.mandt = CCText.Client_MANDT
    AND CostCenters.subclass = CCText.ControllingArea_KOKRS
    AND CostCenters.node = CCText.CostCenter_KOSTL
    AND CCText.Language_SPRAS = LanguageKey.LanguageKey_SPRAS
    AND CCText.ValidTo_DATBI = '9999-12-31'
