SELECT
  h.parent AS ParentHierarchy, h.child AS ChildHierarchy,
  ht.descript AS Description, pmd.CostCenter_KOSTL, pmd.Description_LTEXT,
  b.AmountInLocalCurrency_DMBTR, b.AmountInDocumentCurrency_WRBTR
-- cepc_hier is created as part of the hierarchy flattening of cost centers
FROM `{{ project_id_src }}.{{ dataset_reporting_tgt }}.csks_hier` AS h
INNER JOIN `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.setheadert` AS ht
  ON h.mandt = ht.mandt
    AND h.child_org = ht.subclass
    AND h.child = ht.setname
INNER JOIN `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.CostCentersMD` AS pmd
  ON h.mandt = pmd.Client_MANDT AND h.kostl = pmd.CostCenter_KOSTL AND ht.langu = pmd.Language_SPRAS
INNER JOIN `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.AccountingDocuments` AS b
  ON h.mandt = b.Client_MANDT AND h.kostl = b.CostCenter_KOSTL
WHERE ht.langu IN UNNEST({{ sap_languages }}) AND ht.setclass = '0101'
