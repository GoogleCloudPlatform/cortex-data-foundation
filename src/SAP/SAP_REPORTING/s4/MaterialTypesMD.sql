SELECT
  T134.MANDT AS Client_MANDT,
  T134.MTART AS MaterialType_MTART,
  T134.MTREF AS ReferenceMaterialType_MTREF,
  T134.MBREF AS ScreenReferenceDependingOnTheMaterialType_MBREF,
  T134T.SPRAS AS LanguageKey_SPRAS, T134T.MTBEZ AS DescriptionOfMaterialType_MTBEZ
#T134.FLREF AS FieldReferenceForMaterialMaster_FLREF,
#T134.NUMKI AS NumberRange_NUMKI,  T134.NUMKE AS NumberRange_NUMKE,  T134.ENVOP AS ExternalNumberAssignmentWithoutValidation_ENVOP,  T134.BSEXT AS ExternalPurchaseOrdersAllowed_BSEXT,
#T134.BSINT AS InternalPurchaseOrdersAllowed_BSINT,  T134.PSTAT AS MaintenanceStatus_PSTAT,  T134.KKREF AS AccountCategoryReference_KKREF,  T134.VPRSV AS PriceControlIndicator_VPRSV,  T134.KZVPR AS PriceControlMandatory_KZVPR,
#T134.VMTPO AS DefaultValueForMaterialItemCategoryGroup_VMTPO,  T134.EKALR AS MaterialIsCostedWithQuantityStructure_EKALR,  T134.KZGRP AS GroupingIndicator_KZGRP,  T134.KZKFG AS ConfigurableMaterial_KZKFG,
#T134.BEGRU AS AuthorizationGroupInTheMaterialMaster_BEGRU,  T134.KZPRC AS MaterialMasterRecordForAProcess_KZPRC,  T134.KZPIP AS PipelineHandlingMandatory_KZPIP,  T134.PRDRU AS DisplayPriceOnCashRegisterDisplayAndPrintOnReceipt_PRDRU,
#T134.ARANZ AS DisplayMaterialOnCashRegisterDisplay_ARANZ,  T134.WMAKG AS MaterialTypeId_WMAKG,  T134.IZUST AS InitialStatusOfANewBatch_IZUST,  T134.ARDEL AS TimeInDaysUntilAMaterialIsDeleted_ARDEL,
#T134.KZMPN AS ManufacturerPart_KZMPN,  T134.MSTAE AS CrossPlantMaterialStatus_MSTAE,  T134.CCHIS AS Control__time__OfHistoryRequirement_Material_CCHIS,  T134.CTYPE AS ClassType_CTYPE,  T134.CLASS AS ClassNumber_CLASS,
#T134.CHNEU AS BatchCreationControl__automaticmanual___CHNEU,  T134.VTYPE AS VersionCategory_VTYPE,  T134.VNUMKI AS NumberRange_VNUMKI,  T134.VNUMKE AS NumberRange_VNUMKE,  T134.KZRAC AS ReturnablePackagingLogisticsIsMandatory_KZRAC,
FROM `{{ project_id_src }}.{{ dataset_cdc_processed_s4 }}.t134` AS T134
INNER JOIN `{{ project_id_src }}.{{ dataset_cdc_processed_s4 }}.t134t` AS T134T
  ON T134.MANDT = T134T.MANDT AND T134.MTART = T134T.MTART
