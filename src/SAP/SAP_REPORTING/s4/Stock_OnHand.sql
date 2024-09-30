WITH weekly_extract AS (
  SELECT mandt,
        matbf, 
        WERKS, 
        SUM(stock_qty_l1) as Quantity
#TODO(kuchhala): Rolling week             
  FROM `{{ project_id_src }}.{{ dataset_cdc_processed }}.matdoc_extract` 
  WHERE LEFT(gjper,4) = CAST(  EXTRACT(YEAR from CURRENT_DATE()) as STRING )
  GROUP BY matbf,WERKS
),
material_docs AS (
  SELECT mandt, 
        MATNR, 
        WERKS, 
        CASE shkzg
            WHEN 'H' THEN
                SUM(MENGE * -1)
            WHEN 'S' THEN
                SUM(menge)
        END as Quantity
  FROM `{{ project_id_src }}.{{ dataset_cdc_processed }}.matdoc`
  WHERE record_type = 'MDOC' 
    AND bstaus_cg = 'A' ## Unrestricted
    AND budat BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) AND (CURRENT_DATE())
  GROUP BY matnr, werks )
SELECT SUM(weekly_extract.Quantity + material_docs.Quantity) AS STOCK_ON_HAND, 
  material_docs.MATNR as Material, 
  material_docs.WERKS as Plant,
FROM weekly_extract 
JOIN material_docs
  ON weekly_extract.matbf = material_docs.MATNR 
  AND material_docs.WERKS = material_docs.WERKS 
  AND material_docs.mandt = material_docs.mandt
GROUP BY material_docs.matnr, material_docs.werks  
