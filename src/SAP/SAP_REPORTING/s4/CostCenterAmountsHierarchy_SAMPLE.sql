SELECT
  h.ParentNodeText AS ParentHierarchy,
  h.CostCenterNode AS ChildHierarchy,
  h.CostCenter_KOSTL,
  h.LanguageKey_SPRAS,
  h.CostCenterNodeText AS CostCenterDescription,
  a.AmountInLocalCurrency_DMBTR,
  a.AmountInDocumentCurrency_WRBTR
FROM `{{ project_id_src }}.{{ dataset_reporting_tgt }}.CostCenterHierarchyFlattened` AS h
INNER JOIN `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.AccountingDocuments` AS a
  USING (Client_MANDT, CostCenter_KOSTL)
WHERE
  h.HierarchyClass_SETCLASS = '0101'
  AND h.IsLeafNode
