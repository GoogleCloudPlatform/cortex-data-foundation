SELECT
  h.ParentNode AS ParentHierarchy,
  h.ProfitCenterNode AS ChildHierarchy,
  h.ProfitCenter_PRCTR,
  h.LanguageKey_SPRAS,
  h.ProfitCenterNodeText AS ProfitCenterDescription,
  a.AmountInLocalCurrency_DMBTR,
  a.AmountInDocumentCurrency_WRBTR
FROM `{{ project_id_src }}.{{ dataset_reporting_tgt }}.ProfitCenterHierarchyFlattened` AS h
INNER JOIN `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.AccountingDocuments` AS a
  USING (Client_MANDT, ProfitCenter_PRCTR)
WHERE
  h.HierarchyClass_SETCLASS = '0106'
  AND h.LanguageKey_SPRAS = 'E'
  AND h.IsLeafNode
