(WITH eket AS (
  SELECT Client_MANDT,
    PurchasingDocumentNumber_EBELN,
    ItemNumberOfPurchasingDocument_EBELP, StatisticsRelevantDeliveryDate_SLFDT,
    ItemDeliveryDate_EINDT, sum(ScheduledQuantity_MENGE) AS ScheduledQuantity_MENGE,
    sum(QuantityOfGoodsReceived_WEMNG) AS QuantityOfGoodsReceived_WEMNG
  FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.POSchedule`
  GROUP BY Client_MANDT, PurchasingDocumentNumber_EBELN,
    ItemNumberOfPurchasingDocument_EBELP,
    StatisticsRelevantDeliveryDate_SLFDT, ItemDeliveryDate_EINDT
)

SELECT
  PO.Client_MANDT, PO.DocumentNumber_EBELN, PO.Item_EBELP,
  PO.DocumentCategory_BSTYP, PO.DocumentType_BSART,
  PO.VendorAccountNumber_LIFNR, PO.Language_SPRAS,
  PO.TermsPaymentKey_ZTERM, PO.CashDiscountPercentage1_ZBD1P,
  PO.PurchasingOrganization_EKORG, PO.PurchasingGroup_EKGRP,
  PO.CurrencyKey_WAERS, PO.MaterialNumber_MATNR,
  PO.ShortText_TXZ01,
  PO.MaterialGroup_MATKL,
  PO.StorageLocation_LGORT,
  PO.POQuantity_MENGE,
  PO.UoM_MEINS,
  PO.OrderPriceUnit_BPRME,
  PO.NetPrice_NETPR, PO.NetOrderValueinPOCurrency_NETWR,
  PO.GrossordervalueinPOcurrency_BRTWR,
  PO.DeliveryCompletedFlag_ELIKZ,
  PO.NetWeight_NTGEW,
  PO.ReturnsItem_RETPO,
  delivery.ItemDeliveryDate_EINDT,
  if(PO.ReturnsItem_RETPO IS NULL, delivery.ScheduledQuantity_MENGE, delivery.ScheduledQuantity_MENGE * -1 ) AS TotalScheduledQty,
  if(PO.ReturnsItem_RETPO IS NULL, delivery.QuantityOfGoodsReceived_WEMNG, delivery.QuantityOfGoodsReceived_WEMNG * -1 ) AS TotalReceivedQty,
  (if(PO.ReturnsItem_RETPO IS NULL, delivery.ScheduledQuantity_MENGE, delivery.ScheduledQuantity_MENGE * -1 ) - if(PO.ReturnsItem_RETPO IS NULL, delivery.QuantityOfGoodsReceived_WEMNG, delivery.QuantityOfGoodsReceived_WEMNG * -1 ) ) AS PendingQty
FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.PurchaseDocuments` AS PO
INNER JOIN eket AS delivery
  ON PO.Client_MANDT = delivery.Client_MANDT
    AND PO.DocumentNumber_EBELN = delivery.PurchasingDocumentNumber_EBELN
    AND PO.Item_EBELP = delivery.ItemNumberOfPurchasingDocument_EBELP
)