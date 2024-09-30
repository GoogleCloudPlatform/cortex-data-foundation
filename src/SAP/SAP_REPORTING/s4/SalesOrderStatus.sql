SELECT
  vbap.VBELN AS SalesDocument_VBELN,
  vbap.POSNR AS Item_POSNR,
  vbap.MATNR AS MaterialNumber_MATNR,
  vbak.AUART AS SalesDocumentType_AUART,
  vbak.AUGRU AS Reason_AUGRU,
  vbak.KVGR1 AS CustomerGroup1_KVGR1,
  vbak.KVGR2 AS CustomerGroup2_KVGR2,
  vbak.KVGR3 AS CustomerGroup3_KVGR3,
  vbak.KVGR4 AS CustomerGroup4_KVGR4,
  vbak.KVGR5 AS CustomerGroup5_KVGR5,
  vbak.LIFSK AS DeliveryBlock_LIFSK,
  vbap.VPWRK AS PlanningPlant_VPWRK,
  vbap.LGORT AS StorageLocation_LGORT,
  vbap.CHARG AS BatchNumber_CHARG,
  vbap.KWMENG AS CumulativeOrderQuantity_KWMENG,
  lips.VGBEL AS ReferenceDocument_VGBEL,
  lips.VGPOS AS ReferenceItem_VGPOS,
  lips.LFIMG AS ActualQuantityDelivered_LFIMG,
  lips.VGTYP AS DocumentCategory_VGTYP,
  vbap.SERAIL AS SerialNumberProfile_SERAIL,
  vbap.ANZSN AS NumberOfSerialNumbers_ANZSN,
  vbak.ABSTK AS HeaderRejectionStatus_ABSTK,
  vbak.LFGSK AS HeaderDeliveryStatus_LFGSK,
  lips.WBSTA AS GoodsMovementStatus_WBSTA,
  vbak.ERDAT AS CreationDate_ERDAT,
  vbak.ERZET AS CreationTime_ERZET,
  vbak.VDATU AS RequestedDeliveryDate_VDATU,
  vbak.AUTLF AS CompleteDeliveryFlag_AUTLF,
  lips.VBELN AS Delivery_VBELN,
  lips.POSNR AS DeliveryItem_POSNR,
  IF((vbap.KWMENG - lips.LFIMG) < 0, 0, (vbap.KWMENG - lips.LFIMG)) AS OPENQTY
FROM
  `{{ project_id_src }}.{{ dataset_cdc_processed_s4 }}.vbap` AS vbap
INNER JOIN
  `{{ project_id_src }}.{{ dataset_cdc_processed_s4 }}.lips` AS lips
  ON
    vbap.MANDT = lips.MANDT
    AND vbap.VBELN = lips.VGBEL
    AND vbap.POSNR = lips.VGPOS
    AND vbap.MANDT = '{{ mandt }}'
    AND lips.MANDT = '{{ mandt }}'
INNER JOIN
  `{{ project_id_src }}.{{ dataset_cdc_processed_s4 }}.vbak` AS vbak
  ON
    vbap.MANDT = vbak.MANDT
    AND vbap.VBELN = vbak.VBELN
    AND vbap.MANDT = '{{ mandt }}'
    AND vbak.MANDT = '{{ mandt }}'
