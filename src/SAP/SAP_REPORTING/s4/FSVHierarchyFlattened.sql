--The granularity of this query is Client,Chart of accounts,Hierarchy name,
--GLNode,GLAccount,Language Key.
--## CORTEX-CUSTOMER Please filter on HierarchyName in case of multiple hierarchies
--flattened in your system.
WITH
  LanguageKey AS (
    SELECT
      LanguageKey_SPRAS
    FROM
      `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.Languages_T002`
    WHERE LanguageKey_SPRAS IN UNNEST({{ sap_languages }})
  )

SELECT
  FSV.mandt AS Client_MANDT,
  FSV.chartofaccounts AS ChartOfAccounts,
  FSV.hiername AS HierarchyName,
  LanguageKey.LanguageKey_SPRAS,
  FSV.glaccount AS GeneralLedgerAccount,
  FSV.node AS GLNode,
  FSV.parent AS ParentNode,
  --The following text fields are language dependent.
  GLParentText.HierarchyNodeDescription_NODETXT AS ParentNodeText,
  COALESCE(GLNodeText.HierarchyNodeDescription_NODETXT, GLText.GlAccountLongText_TXT50) AS GLNodeText,
  FSV.level AS Level,
  FSV.isleafnode AS IsLeafNode
FROM
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.fsv_glaccounts` AS FSV
CROSS JOIN LanguageKey
LEFT JOIN
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.FSVTextsMD` AS GLNodeText
  ON
    FSV.mandt = GLNodeText.Client_MANDT
    AND FSV.hiername = GLNodeText.HierarchyID_HRYID
    AND FSV.HierarchyVersion = GLNodeText.HierarchyVersion_HRYVER
    AND FSV.Node = GLNodeText.HierarchyNode_HRYNODE
    AND GLNodeText.LanguageKey_SPRAS = LanguageKey.LanguageKey_SPRAS
LEFT JOIN
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.GLAccountsMD` AS GLText
  ON
    FSV.mandt = GLText.Client_MANDT
    AND FSV.chartofaccounts = GLText.ChartOfAccounts_KTOPL
    AND FSV.nodevalue = GLText.GlAccountNumber_SAKNR
    AND GLText.Language_SPRAS = LanguageKey.LanguageKey_SPRAS
LEFT JOIN
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.FSVTextsMD` AS GLParentText
  ON
    FSV.mandt = GLParentText.Client_MANDT
    AND FSV.hiername = GLParentText.HierarchyID_HRYID
    AND FSV.HierarchyVersion = GLParentText.HierarchyVersion_HRYVER
    AND FSV.Parent = GLParentText.HierarchyNode_HRYNODE
    AND GLParentText.LanguageKey_SPRAS = LanguageKey.LanguageKey_SPRAS
