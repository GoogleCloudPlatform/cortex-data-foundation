---CORTEX-CUSTOMER: Stock In Hand is the aggregated unrestricted stock Qty
---from various sources like stock at Distribution centers, at Vendor location,
---at Customer Location and special stock at Vendor in Consignment.

## CORTEX-CUSTOMER: We are transposing MARD to achieve different stock reasons in a single column
SELECT
  mard.MANDT AS Client_MANDT,
  mard.MATNR AS ArticleNumber_MATNR,
  mard.WERKS AS Site_WERKS,
  mard.LGORT AS StorageLocation_LGORT,
  NULL AS BatchNumber_CHARG,
  NULL AS SpecialStockIndicator_SOBKZ,
  NULL AS SDDocumentNumber_VBELN,
  NULL AS SDDocumentItemNumber_POSNR,
  NULL AS VendorAccountNumber_LIFNR,
  NULL AS CustomerNumber_KUNNR,
  SPLIT(Qty, '@') [OFFSET(0)] AS Qty,
  SPLIT(Qty, '@') [OFFSET(1)] AS StockType
FROM
  `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.mard` AS mard,
  UNNEST(
    [
      CAST(LABST AS STRING) || '@A-Unrestricted use',
      CAST(UMLME AS STRING) || '@F-Stock in transfer',
      CAST(INSME AS STRING) || '@B-Quality inspection',
      CAST(EINME AS STRING) || '@E-Stock of All Restricted Batches',
      CAST(SPEME AS STRING) || '@D-Blocked Stock',
      CAST(RETME AS STRING) || '@C Blocked stock returns'
    ]
  ) AS Qty
WHERE Qty IS NOT NULL AND mard.MANDT = '{{ mandt }}'
UNION ALL
SELECT
  mska.MANDT AS Client_MANDT,
  mska.MATNR AS ArticleNumber_MATNR,
  mska.WERKS AS Site_WERKS,
  mska.LGORT AS StorageLocation_LGORT,
  CAST(mska.CHARG AS STRING) AS BatchNumber_CHARG,
  mska.SOBKZ AS SpecialStockIndicator_SOBKZ,
  mska.VBELN AS SDDocumentNumber_VBELN,
  mska.POSNR AS SDDocumentItemNumber_POSNR,
  NULL AS VendorAccountNumber_LIFNR,
  NULL AS CustomerNumber_KUNNR,
  SPLIT(Qty, '@') [OFFSET(0)] AS Qty,
  SPLIT(Qty, '@') [OFFSET(1)] AS StockType
FROM
  `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.mska` AS mska,
  UNNEST(
    [
      KALAB || '@A-Unrestricted use',
      KAINS || '@B-Quality inspection',
      KASPE || '@D-Blocked Stock'
    ]
  ) AS Qty
WHERE Qty IS NOT NULL AND mska.MANDT = '{{ mandt }}'
UNION ALL
SELECT
  msfd.MANDT AS Client_MANDT,
  msfd.MATNR AS ArticleNumber_MATNR,
  msfd.WERKS AS Site_WERKS,
  NULL AS StorageLocation_LGORT,
  CAST(msfd.CHARG AS STRING) AS BatchNumber_CHARG,
  msfd.SOBKZ AS SpecialStockIndicator_SOBKZ,
  msfd.VBELN AS SDDocumentNumber_VBELN,
  msfd.POSNR AS SDDocumentItemNumber_POSNR,
  msfd.LIFNR AS VendorAccountNumber_LIFNR,
  NULL AS CustomerNumber_KUNNR,
  SPLIT(Qty, '@') [OFFSET(0)] AS Qty,
  SPLIT(Qty, '@') [OFFSET(1)] AS StockType
FROM
  `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.msfd` AS msfd,
  UNNEST(
    [
      FDLAB || '@A-Unrestricted use',
      FDINS || '@B-Quality inspection',
      FDEIN || '@E-Stock of All Restricted Batches'
    ]
  ) AS Qty
WHERE Qty IS NOT NULL AND msfd.MANDT = '{{ mandt }}'
UNION ALL
SELECT
  mslb.MANDT AS Client_MANDT,
  mslb.MATNR AS ArticleNumber_MATNR,
  mslb.WERKS AS Site_WERKS,
  NULL AS StorageLocation_LGORT,
  CAST(mslb.CHARG AS STRING) AS BatchNumber_CHARG,
  mslb.SOBKZ AS SpecialStockIndicator_SOBKZ,
  NULL AS SDDocumentNumber_VBELN,
  NULL AS SDDocumentItemNumber_POSNR,
  mslb.LIFNR AS VendorAccountNumber_LIFNR,
  NULL AS CustomerNumber_KUNNR,
  SPLIT(Qty, '@') [OFFSET(0)] AS Qty,
  SPLIT(Qty, '@') [OFFSET(1)] AS StockType
FROM
  `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.mslb` AS mslb,
  UNNEST(
    [
      LBLAB || '@A-Unrestricted use',
      LBINS || '@B-Quality inspection'
    ]
  ) AS Qty
WHERE Qty IS NOT NULL AND mslb.MANDT = '{{ mandt }}'
UNION ALL
SELECT
  msku.MANDT AS Client_MANDT,
  msku.MATNR AS ArticleNumber_MATNR,
  msku.WERKS AS Site_WERKS,
  NULL AS StorageLocation_LGORT,
  CAST(msku.CHARG AS STRING) AS BatchNumber_CHARG,
  msku.SOBKZ AS SpecialStockIndicator_SOBKZ,
  NULL AS SDDocumentNumber_VBELN,
  NULL AS SDDocumentItemNumber_POSNR,
  NULL AS VendorAccountNumber_LIFNR,
  msku.KUNNR AS CustomerNumber_KUNNR,
  SPLIT(Qty, '@') [OFFSET(0)] AS Qty,
  SPLIT(Qty, '@') [OFFSET(1)] AS StockType
FROM
  `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.msku` AS msku,
  UNNEST(
    [
      KULAB || '@A-Unrestricted use'
    ]
  ) AS Qty
WHERE Qty IS NOT NULL AND msku.MANDT = '{{ mandt }}'
UNION ALL
SELECT
  mkol.MANDT AS Client_MANDT,
  mkol.MATNR AS ArticleNumber_MATNR,
  mkol.WERKS AS Site_WERKS,
  mkol.LGORT AS StorageLocation_LGORT,
  CAST(mkol.CHARG AS STRING) AS BatchNumber_CHARG,
  mkol.SOBKZ AS SpecialStockIndicator_SOBKZ,
  NULL AS SDDocumentNumber_VBELN,
  NULL AS SDDocumentItemNumber_POSNR,
  mkol.LIFNR AS VendorAccountNumber_LIFNR,
  NULL AS CustomerNumber_KUNNR,
  SPLIT(Qty, '@') [OFFSET(0)] AS Qty,
  SPLIT(Qty, '@') [OFFSET(1)] AS StockType
FROM
  `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.mkol` AS mkol,
  UNNEST(
    [
      SLABS || '@A-Unrestricted use',
      SINSM || '@B-Quality inspection',
      SSPEM || '@D-Blocked Stock'
    ]
  ) AS Qty
WHERE Qty IS NOT NULL AND mkol.MANDT = '{{ mandt }}'
