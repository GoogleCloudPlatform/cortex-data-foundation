SELECT
  SO.Client_MANDT,
  SO.SalesOrder_VBELV,
  SO.SalesItem_POSNV,
  so_status.Delivery_Status,
  SO.DeliveryNumber_VBELV,
  SO.DeliveryItem_POSNV,
  SO.InvoiceNumber_VBELN,
  SO.InvoiceItem_POSNN,
  vbap.CumulativeOrderQuantity_KWMENG AS SalesQty,
  vbap.SalesUnit_VRKME,
  vbap.NetPrice_NETWR,
  vbap.Currency_WAERK,
  SO.DeliveredQty_RFMNG,
  SO.DeliveredUoM_MEINS,
  SO.InvoiceQty_RFMNG,
  SO.InvoiceUoM_MEINS,
  SO.InvoiceValue_RFWRT,
  SO.InvoiceCurrency_WAERS,
  vbap.MaterialNumber_MATNR,
  mat.MaterialText_MAKTX,
  vbap.ProductHierarchy_PRODH,
  mat.Language_SPRAS
FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.SDDocumentFlow` AS SO
INNER JOIN `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.SalesOrders` AS vbap
  ON SO.Client_MANDT = vbap.Client_MANDT
    AND SO.SalesOrder_VBELV = vbap.SalesDocument_VBELN AND SO.SalesItem_POSNV = vbap.Item_POSNR
INNER JOIN `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.MaterialsMD` AS mat
  ON SO.Client_MANDT = mat.Client_MANDT
    AND vbap.MaterialNumber_MATNR = mat.MaterialNumber_MATNR
INNER JOIN `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.SDStatus_Items` AS so_status
  ON SO.Client_MANDT = so_status.Client_MANDT
    AND SO.SalesOrder_VBELV = so_status.SDDocumentNumber_VBELN
    AND SO.SalesItem_POSNV = so_status.ItemNumberOfTheSdDocument_POSNR
