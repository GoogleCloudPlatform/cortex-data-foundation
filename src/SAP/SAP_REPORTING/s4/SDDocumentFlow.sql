SELECT
  SO.mandt AS Client_MANDT,
  SO.VBELV AS SalesOrder_VBELV,
  SO.POSNV AS SalesItem_POSNV,
  Deliveries.VBELV AS DeliveryNumber_VBELV,
  Deliveries.POSNV AS DeliveryItem_POSNV,
  Deliveries.VBELN AS InvoiceNumber_VBELN,
  Deliveries.POSNN AS InvoiceItem_POSNN,
  SO.RFMNG AS DeliveredQty_RFMNG,
  SO.MEINS AS DeliveredUoM_MEINS,
  Deliveries.RFMNG AS InvoiceQty_RFMNG,
  Deliveries.MEINS AS InvoiceUoM_MEINS,
  Deliveries.RFWRT AS InvoiceValue_RFWRT,
  Deliveries.WAERS AS InvoiceCurrency_WAERS
FROM `{{ project_id_src }}.{{ dataset_cdc_processed_s4 }}.vbfa` AS SO
LEFT OUTER JOIN `{{ project_id_src }}.{{ dataset_cdc_processed_s4 }}.vbfa` AS Deliveries
  ON SO.VBELN = Deliveries.VBELV AND SO.mandt = Deliveries.mandt
    AND SO.POSNN = Deliveries.POSNV
WHERE SO.vbtyp_V = 'C'
  AND SO.vbtyp_n IN ('J', 'T')
  AND Deliveries.vbtyp_n IN ('M')
ORDER BY SO.VBELV
