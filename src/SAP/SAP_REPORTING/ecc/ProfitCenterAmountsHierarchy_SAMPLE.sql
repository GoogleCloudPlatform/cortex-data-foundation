SELECT h.parent AS ParentHierarchy,
  h.child AS ChildHierarchy,
  ht.descript AS Description,
  pmd.ProfitCenter_PRCTR, pmd.LongText_LTEXT,
  b.AmountInLocalCurrency_DMBTR, b.AmountInDocumentCurrency_WRBTR
-- cepc_hier is created as part of the hierarchy flattening of cost centers
FROM `{{ project_id_src }}.{{ dataset_reporting_tgt }}.cepc_hier` AS h
INNER JOIN `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.setheadert` AS ht
  ON h.mandt = ht.mandt
    AND h.child_org = ht.subclass
    AND h.child = ht.setname
INNER JOIN `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.ProfitCentersMD` AS pmd
  ON h.mandt = pmd.Client_MANDT AND h.prctr = pmd.ProfitCenter_PRCTR
    AND ht.langu = pmd.Language_SPRAS
INNER JOIN `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.AccountingDocuments` AS b
  ON h.mandt = b.Client_MANDT
    AND h.prctr = b.ProfitCenter_PRCTR
WHERE ht.langu = 'E'
      AND ht.setclass = '0106'
