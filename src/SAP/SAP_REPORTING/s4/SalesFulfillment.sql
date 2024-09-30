WITH
  SO AS (
    SELECT
      SO.Client_MANDT,
      SO.InvoiceUoM_MEINS,
      SO.InvoiceCurrency_WAERS,
      SO.MaterialNumber_MATNR,
      SO.MaterialText_MAKTX,
      SO.DeliveredUoM_MEINS,
      SO.ProductHierarchy_PRODH,
      SO.Language_SPRAS,
      SUM(SO.InvoiceQty_RFMNG) AS BilledQty,
      SUM(SO.InvoiceValue_RFWRT) AS InvoicePrice,
      SUM(SO.DeliveredQty_RFMNG) AS DeliveredQty,
      (SUM(SO.DeliveredQty_RFMNG) - SUM(SO.InvoiceQty_RFMNG)) AS DeliveredPendingBilling
    FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.SalesStatus_Items` AS SO
    WHERE Language_SPRAS IN UNNEST({{ sap_languages }})
    GROUP BY
      SO.Client_MANDT, Currency_WAERK, DeliveredUoM_MEINS,
      InvoiceUoM_MEINS, InvoiceCurrency_WAERS,
      SalesUnit_VRKME, MaterialNumber_MATNR, MaterialText_MAKTX,
      ProductHierarchy_PRODH, Language_SPRAS
  )

SELECT
  SO.Client_MANDT,
  SO.MaterialNumber_MATNR,
  SO.MaterialText_MAKTX,
  vbap.SalesUnit_VRKME,
  vbap.Currency_WAERK,
  SO.DeliveredQty,
  SO.DeliveredUoM_MEINS,
  SO.DeliveredPendingBilling,
  vbap.SalesOrganization_VKORG,
  SUM(vbap.CumulativeOrderQuantity_KWMENG) AS SalesQty,
  SUM(vbap.NetPrice_NETWR) AS NetPrice,
  (SUM(vbap.CumulativeOrderQuantity_KWMENG) - SO.DeliveredQty) AS PendingDelivery
FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.SalesOrders` AS vbap
LEFT OUTER JOIN SO
  ON SO.Client_MANDT = vbap.Client_MANDT
    AND SO.MaterialNumber_MATNR = vbap.MaterialNumber_MATNR
    AND SO.InvoiceCurrency_WAERS = vbap.Currency_WAERK
GROUP BY
  SO.Client_MANDT, vbap.SalesUnit_VRKME, vbap.Currency_WAERK,
  SO.MaterialNumber_MATNR, SO.MaterialText_MAKTX, SO.DeliveredUoM_MEINS,
  SO.DeliveredQty, SO.DeliveredUoM_MEINS, SO.DeliveredPendingBilling, vbap.SalesOrganization_VKORG
