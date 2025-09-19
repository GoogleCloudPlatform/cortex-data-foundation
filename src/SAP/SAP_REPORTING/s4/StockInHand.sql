/*
Aggregates unrestricted stock quantity from distribution centers, vendors, customers, and vendor
consignment. MATDOC provides consolidated Stock Qty for various categories
*/

SELECT
  MANDT AS Client_MANDT,
  MATBF AS MaterialNumber_MATNR,
  --##CORTEX-CUSTOMER ArticleNumber_MATNR is marked for removal. Change all references to MaterialNumber_MATNR.
  MATBF AS ArticleNumber_MATNR,
  WERKS AS Plant_WERKS,
  --##CORTEX-CUSTOMER Site_WERKS is marked for removal. Change all references to Plant_WERKS.
  WERKS AS Site_WERKS,
  LGORT_SID AS StorageLocation_LGORT,
  CAST(CHARG_SID AS STRING) AS BatchNumber_CHARG,
  SOBKZ AS SpecialStockIndicator_SOBKZ,
  MAT_KDAUF AS SDDocumentNumber_VBELN,
  MAT_KDPOS AS SDDocumentItemNumber_POSNR,
  LIFNR_SID AS VendorAccountNumber_LIFNR,
  KUNNR_SID AS CustomerNumber_KUNNR,
  SUM(
    CASE SHKZG
      WHEN 'S' THEN MENGE
      WHEN 'H' THEN -MENGE
      ELSE 0
    END
  ) AS Qty,
  CASE BSTAUS_SG
    -- Assigns type to various stock in-house.
    WHEN 'A' THEN 'A-Unrestricted use'
    WHEN 'B' THEN 'B-Quality inspection'
    WHEN 'C' THEN 'C-Blocked stock returns'
    WHEN 'D' THEN 'D-Blocked Stock'
    WHEN 'E' THEN 'E-Stock of All Restricted Batches'
    WHEN 'F' THEN 'F-Stock in transfer'
    -- Assigns type to consignment stock at customer.
    WHEN 'K' THEN 'A-Unrestricted use'
    WHEN 'L' THEN 'B-Quality inspection'
    WHEN 'M' THEN 'E-Stock of All Restricted Batches'
    -- Assigns type to stock provided to vendor.
    WHEN 'Q' THEN 'A-Unrestricted use'
    WHEN 'R' THEN 'B-Quality inspection'
    WHEN 'S' THEN 'E-Stock of All Restricted Batches'
  END AS StockType
FROM
  `{{ project_id_src }}.{{ dataset_cdc_processed_s4 }}.matdoc`
WHERE
  BSTAUS_SG IN ('A', 'B', 'C', 'D', 'E', 'F', 'K', 'L', 'M', 'Q', 'R', 'S')
  AND MANDT = '{{ mandt }}'
GROUP BY
  Client_MANDT,
  MaterialNumber_MATNR,
  Plant_WERKS,
  StorageLocation_LGORT,
  BatchNumber_CHARG,
  SpecialStockIndicator_SOBKZ,
  SDDocumentNumber_VBELN,
  SDDocumentItemNumber_POSNR,
  VendorAccountNumber_LIFNR,
  CustomerNumber_KUNNR,
  StockType
HAVING Qty != 0
