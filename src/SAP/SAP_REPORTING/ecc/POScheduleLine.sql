SELECT
  ekko.MANDT AS Client_MANDT,
  eket.EBELN AS PurchasingDocumentNumber_EBELN,
  ekpo.MATNR AS MaterialNumber_MATNR,
  eket.EBELP AS ItemNumberOfPurchasingDocument_EBELP,
  eket.ETENR AS DeliveryScheduleLineCounter_ETENR,
  ekpo.WERKS AS Plant_WERKS,
  ekpo.LGORT AS StorageLocation_LGORT,
  eket.EINDT AS ItemDeliveryDate_EINDT,
  eket.SLFDT AS StatisticsRelevantDeliveryDate_SLFDT,
  eket.MENGE AS ScheduledQuantity_MENGE,
  eket.WEMNG AS QuantityOfGoodsReceived_WEMNG,
  eket.WAMNG AS IssuedQuantity_WAMNG,
  ekko.BSART AS PurchasingDocumentType_BSART,
  ekko.AEDAT AS DateOnWhichRecordWasCreated_AEDAT,
  ekko.LIFNR AS VendorAccountNumber_LIFNR,
  ekko.EKORG AS PurchasingOrganization_EKORG,
  ekko.EKGRP AS PurchasingGroup_EKGRP,
  ekko.BEDAT AS PurchasingDocumentDate_BEDAT,
  ekko.RESWK AS SupplyingPlantInCaseOfStockTransportOrder_RESWK,
  ekpo.LOEKZ AS DeletionIndicatorInPurchasingDocument_LOEKZ,
  ekpo.ELIKZ AS DeliveryCompleted_Indicator_ELIKZ,
  ekpo.BSTYP AS PurchasingDocumentCategory_BSTYP,
  ekpo.BANFN AS PurchaseRequisitionNumber_BANFN,
  ekpo.BNFPO AS ItemNumberOfPurchaseRequisition_BNFPO,
  ekpo.RETPO AS ReturnsItem_RETPO,
  ekpo.RESLO AS IssuingStorageLocationForStockTransportOrder_RESLO,
  ## CORTEX-CUSTOMER:OpenQuantity is scheduled quantity substraction GoodsIssuedQuantity with PurchaseOrder not marked as completed
  IF(((eket.MENGE - eket.WEMNG) < 0 OR ekpo.ELIKZ = 'X'),
    0,
    (eket.MENGE - eket.WEMNG)
  ) AS OpenQuantity,
  eket.WAMNG - eket.WEMNG AS InTransitQuantity
FROM
  `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.eket` AS eket
INNER JOIN
  `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.ekpo` AS ekpo
  ON
    eket.EBELN = ekpo.EBELN
    AND eket.EBELP = ekpo.EBELP
    AND eket.MANDT = ekpo.MANDT
INNER JOIN
  `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.ekko` AS ekko
  ON
    eket.EBELN = ekko.EBELN
    AND eket.MANDT = ekko.MANDT
