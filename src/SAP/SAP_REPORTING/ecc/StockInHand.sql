/*
Aggregates unrestricted stock quantity from distribution centers, vendors, customers, and vendor
consignment. We are transposing MARD to achieve different stock reasons in a single column.
*/

SELECT
  MANDT AS Client_MANDT,
  MATNR AS MaterialNumber_MATNR,
  --##CORTEX-CUSTOMER ArticleNumber_MATNR is marked for removal. Change all references to MaterialNumber_MATNR.
  MATNR AS ArticleNumber_MATNR,
  WERKS AS Plant_WERKS,
  --##CORTEX-CUSTOMER Site_WERKS is marked for removal. Change all references to Plant_WERKS.
  WERKS AS Site_WERKS,
  LGORT AS StorageLocation_LGORT,
  NULL AS BatchNumber_CHARG,
  NULL AS SpecialStockIndicator_SOBKZ,
  NULL AS SDDocumentNumber_VBELN,
  NULL AS SDDocumentItemNumber_POSNR,
  NULL AS VendorAccountNumber_LIFNR,
  NULL AS CustomerNumber_KUNNR,
  Qty,
  CASE SourceStockColumn
    WHEN 'LABST' THEN 'A-Unrestricted use'
    WHEN 'UMLME' THEN 'F-Stock in transfer'
    WHEN 'INSME' THEN 'B-Quality inspection'
    WHEN 'EINME' THEN 'E-Stock of All Restricted Batches'
    WHEN 'SPEME' THEN 'D-Blocked Stock'
    WHEN 'RETME' THEN 'C-Blocked stock returns'
  END AS StockType
FROM
  `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.mard`
UNPIVOT EXCLUDE NULLS (Qty FOR SourceStockColumn IN (LABST, UMLME, INSME, EINME, SPEME, RETME))
WHERE MANDT = '{{ mandt }}'
UNION ALL
SELECT
  MANDT AS Client_MANDT,
  MATNR AS MaterialNumber_MATNR,
  --##CORTEX-CUSTOMER ArticleNumber_MATNR is marked for removal. Change all references to MaterialNumber_MATNR.
  MATNR AS ArticleNumber_MATNR,
  WERKS AS Plant_WERKS,
  --##CORTEX-CUSTOMER Site_WERKS is marked for removal. Change all references to Plant_WERKS.
  WERKS AS Site_WERKS,
  LGORT AS StorageLocation_LGORT,
  CAST(CHARG AS STRING) AS BatchNumber_CHARG,
  SOBKZ AS SpecialStockIndicator_SOBKZ,
  VBELN AS SDDocumentNumber_VBELN,
  POSNR AS SDDocumentItemNumber_POSNR,
  NULL AS VendorAccountNumber_LIFNR,
  NULL AS CustomerNumber_KUNNR,
  Qty,
  CASE SourceStockColumn
    WHEN 'KALAB' THEN 'A-Unrestricted use'
    WHEN 'KAINS' THEN 'B-Quality inspection'
    WHEN 'KASPE' THEN 'D-Blocked Stock'
  END AS StockType
FROM
  `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.mska`
UNPIVOT EXCLUDE NULLS (Qty FOR SourceStockColumn IN (KALAB, KAINS, KASPE))
WHERE MANDT = '{{ mandt }}'
UNION ALL
SELECT
  MANDT AS Client_MANDT,
  MATNR AS MaterialNumber_MATNR,
  --##CORTEX-CUSTOMER ArticleNumber_MATNR is marked for removal. Change all references to MaterialNumber_MATNR.
  MATNR AS ArticleNumber_MATNR,
  WERKS AS Plant_WERKS,
  --##CORTEX-CUSTOMER Site_WERKS is marked for removal. Change all references to Plant_WERKS.
  WERKS AS Site_WERKS,
  NULL AS StorageLocation_LGORT,
  CAST(CHARG AS STRING) AS BatchNumber_CHARG,
  SOBKZ AS SpecialStockIndicator_SOBKZ,
  VBELN AS SDDocumentNumber_VBELN,
  POSNR AS SDDocumentItemNumber_POSNR,
  LIFNR AS VendorAccountNumber_LIFNR,
  NULL AS CustomerNumber_KUNNR,
  Qty,
  CASE SourceStockColumn
    WHEN 'FDLAB' THEN 'A-Unrestricted use'
    WHEN 'FDINS' THEN 'B-Quality inspection'
    WHEN 'FDEIN' THEN 'E-Stock of All Restricted Batches'
  END AS StockType
FROM
  `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.msfd`
UNPIVOT EXCLUDE NULLS (Qty FOR SourceStockColumn IN (FDLAB, FDINS, FDEIN))
WHERE MANDT = '{{ mandt }}'
UNION ALL
SELECT
  MANDT AS Client_MANDT,
  MATNR AS MaterialNumber_MATNR,
  --##CORTEX-CUSTOMER ArticleNumber_MATNR is marked for removal. Change all references to MaterialNumber_MATNR.
  MATNR AS ArticleNumber_MATNR,
  WERKS AS Plant_WERKS,
  --##CORTEX-CUSTOMER Site_WERKS is marked for removal. Change all references to Plant_WERKS.
  WERKS AS Site_WERKS,
  NULL AS StorageLocation_LGORT,
  CAST(CHARG AS STRING) AS BatchNumber_CHARG,
  SOBKZ AS SpecialStockIndicator_SOBKZ,
  NULL AS SDDocumentNumber_VBELN,
  NULL AS SDDocumentItemNumber_POSNR,
  LIFNR AS VendorAccountNumber_LIFNR,
  NULL AS CustomerNumber_KUNNR,
  Qty,
  CASE SourceStockColumn
    WHEN 'LBLAB' THEN 'A-Unrestricted use'
    WHEN 'LBINS' THEN 'B-Quality inspection'
  END AS StockType
FROM
  `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.mslb`
UNPIVOT EXCLUDE NULLS (Qty FOR SourceStockColumn IN (LBLAB, LBINS))
WHERE MANDT = '{{ mandt }}'
UNION ALL
SELECT
  MANDT AS Client_MANDT,
  MATNR AS MaterialNumber_MATNR,
  --##CORTEX-CUSTOMER ArticleNumber_MATNR is marked for removal. Change all references to MaterialNumber_MATNR.
  MATNR AS ArticleNumber_MATNR,
  WERKS AS Plant_WERKS,
  --##CORTEX-CUSTOMER Site_WERKS is marked for removal. Change all references to Plant_WERKS.
  WERKS AS Site_WERKS,
  NULL AS StorageLocation_LGORT,
  CAST(CHARG AS STRING) AS BatchNumber_CHARG,
  SOBKZ AS SpecialStockIndicator_SOBKZ,
  NULL AS SDDocumentNumber_VBELN,
  NULL AS SDDocumentItemNumber_POSNR,
  NULL AS VendorAccountNumber_LIFNR,
  KUNNR AS CustomerNumber_KUNNR,
  Qty,
  CASE SourceStockColumn
    WHEN 'KULAB' THEN 'A-Unrestricted use'
  END AS StockType
FROM
  `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.msku`
UNPIVOT EXCLUDE NULLS (Qty FOR SourceStockColumn IN (KULAB))
WHERE MANDT = '{{ mandt }}'
UNION ALL
SELECT
  MANDT AS Client_MANDT,
  MATNR AS MaterialNumber_MATNR,
  --##CORTEX-CUSTOMER ArticleNumber_MATNR is marked for removal. Change all references to MaterialNumber_MATNR.
  MATNR AS ArticleNumber_MATNR,
  WERKS AS Plant_WERKS,
  --##CORTEX-CUSTOMER Site_WERKS is marked for removal. Change all references to Plant_WERKS.
  WERKS AS Site_WERKS,
  LGORT AS StorageLocation_LGORT,
  CAST(CHARG AS STRING) AS BatchNumber_CHARG,
  SOBKZ AS SpecialStockIndicator_SOBKZ,
  NULL AS SDDocumentNumber_VBELN,
  NULL AS SDDocumentItemNumber_POSNR,
  LIFNR AS VendorAccountNumber_LIFNR,
  NULL AS CustomerNumber_KUNNR,
  Qty,
  CASE SourceStockColumn
    WHEN 'SLABS' THEN 'A-Unrestricted use'
    WHEN 'SINSM' THEN 'B-Quality inspection'
    WHEN 'SSPEM' THEN 'D-Blocked Stock'
  END AS StockType
FROM
  `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.mkol`
UNPIVOT EXCLUDE NULLS (Qty FOR SourceStockColumn IN (SLABS, SINSM, SSPEM))
WHERE MANDT = '{{ mandt }}'
