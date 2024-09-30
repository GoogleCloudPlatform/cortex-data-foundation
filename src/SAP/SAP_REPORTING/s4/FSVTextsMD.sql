SELECT
  hrrp_nodet.MANDT AS Client_MANDT,
  hrrp_nodet.HRYID AS HierarchyID_HRYID,
  hrrp_nodet.HRYVER AS HierarchyVersion_HRYVER,
  hrrp_nodet.SPRAS AS LanguageKey_SPRAS,
  hrrp_nodet.NODECLS AS NodeClass_NODECLS,
  hrrp_nodet.HRYNODE AS HierarchyNode_HRYNODE,
  hrrp_nodet.PARNODE AS HierarchyParentNode_PARNODE,
  hrrp_nodet.HRYVALTO AS ValidTo_HRYVALTO,
  hrrp_nodet.HRYVALFROM AS ValidFrom_HRYVALFROM,
  hrrp_nodet.NODETXT AS HierarchyNodeDescription_NODETXT
FROM
  `{{ project_id_src }}.{{ dataset_cdc_processed_s4 }}.hrrp_nodet` AS hrrp_nodet
