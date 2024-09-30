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
  ),

  ParentId AS (
    SELECT DISTINCT
      fsv_parent.mandt,
      fsv_parent.parent,
      fsv_child.ergsl
    FROM
      `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.fsv_glaccounts` AS fsv_parent
    INNER JOIN
      `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.fsv_glaccounts` AS fsv_child
      ON
        fsv_parent.mandt = fsv_child.mandt
        AND fsv_parent.parent = fsv_child.node
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
  IF(FSV.Level = '02', FSV.hiername, GLParentText.FinancialStatementItemText_TXT45) AS ParentNodeText,
  COALESCE(GLNodeText.FinancialStatementItemText_TXT45, GLText.GlAccountLongText_TXT50) AS GLNodeText,
  FSV.level AS Level,
  FSV.isleafnode AS IsLeafNode
FROM
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.fsv_glaccounts` AS FSV
LEFT JOIN ParentId
  ON
    FSV.mandt = ParentId.mandt
    AND FSV.Parent = ParentId.Parent
CROSS JOIN LanguageKey
LEFT JOIN
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.FSVTextsMD` AS GLNodeText
  ON
    FSV.mandt = GLNodeText.Client_MANDT
    AND FSV.hiername = GLNodeText.FinancialStatementVersion_VERSN
    AND FSV.ergsl = GLNodeText.FinancialStatementItem_ERGSL
    AND GLNodeText.LanguageKey_SPRAS = LanguageKey.LanguageKey_SPRAS
    -- TextType_TXTYP = 'K' represents text for the node
    AND GLNodeText.TextType_TXTYP = 'K'
LEFT JOIN
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.GLAccountsMD` AS GLText
  ON
    FSV.mandt = GLText.Client_MANDT
    AND FSV.chartofaccounts = GLText.ChartOfAccounts_KTOPL
    AND FSV.ergsl = GLText.GlAccountNumber_SAKNR
    AND GLText.Language_SPRAS = LanguageKey.LanguageKey_SPRAS
LEFT JOIN
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.FSVTextsMD` AS GLParentText
  ON
    FSV.mandt = GLParentText.Client_MANDT
    AND FSV.hiername = GLParentText.FinancialStatementVersion_VERSN
    AND ParentID.ergsl = GLParentText.FinancialStatementItem_ERGSL
    AND GLParentText.LanguageKey_SPRAS = LanguageKey.LanguageKey_SPRAS
    -- TextType_TXTYP = 'K' represents text for the node
    AND GLParentText.TextType_TXTYP = 'K'
