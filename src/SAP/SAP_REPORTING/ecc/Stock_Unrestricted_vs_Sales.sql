WITH
  stock AS (
    SELECT
      Client_MANDT,
      MaterialNumber_MATNR,
      MaterialText_MAKTX,
      Plant_WERKS,
      Plant_Name,
      BaseUnitOfMeasure_MEINS,
      ProductHierarchy_PRDHA,
      Plant_Region,
      Plant_Country,
      StorageLocation_LGORT,
      Language_SPRAS,
      SUM(ValuatedUnrestrictedUseStock) AS UnrestrictedStock
    FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.Stock_NonValuated`
    GROUP BY
      Client_MANDT, MaterialNumber_MATNR, MaterialText_MAKTX,
      Plant_WERKS, Plant_Name, BaseUnitOfMeasure_MEINS, ProductHierarchy_PRDHA,
      Plant_Region, Plant_Country, StorageLocation_LGORT, Language_SPRAS
  ),

  sales AS (
    SELECT
      Client_MANDT,
      MaterialNumber_MATNR,
      SalesUnit_VRKME,
      DeliveredUoM_MEINS,
      Plant_WERKS,
      StorageLocation_LGORT, Language_SPRAS,
      SUM(SalesQty) AS SalesQty,
      SUM(DeliveredQty) AS DeliveredQty,
      SUM(PendingDelivery) AS PendingDelivery
    FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.SalesFulfillment_perOrder`
    GROUP BY
      Client_MANDT, MaterialNumber_MATNR, SalesUnit_VRKME, DeliveredUoM_MEINS,
      Plant_WERKS, StorageLocation_LGORT, Language_SPRAS
  )

SELECT
  stock.*,
  sales.SalesQty,
  sales.DeliveredQty,
  sales.DeliveredUoM_MEINS,
  sales.PendingDelivery
FROM sales -- noqa: ST09
LEFT OUTER JOIN stock
  ON stock.Client_MANDT = sales.Client_MANDT
    AND stock.MaterialNumber_MATNR = sales.MaterialNumber_MATNR
    AND stock.Plant_WERKS = sales.Plant_WERKS
    AND stock.StorageLocation_LGORT = sales.StorageLocation_LGORT
    AND stock.Language_SPRAS = sales.Language_SPRAS
    AND stock.BaseUnitOfMeasure_MEINS = sales.DeliveredUoM_MEINS
WHERE stock.Language_SPRAS IN UNNEST({{ sap_languages }})
