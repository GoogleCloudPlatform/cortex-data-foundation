--## CORTEX-CUSTOMER: In S/4 Hana , SAP has acitvated Material Ledge ML hence they have suggested to use CKMLHD and CKMLCR
--which has the same data compared to older tables and restricted the use of MBEW and MBEWH. This View is Union All
--compatible without currency conversion and date dimension(because of no date in the fields there are only month and year fields in this view)
SELECT
  ckmlhd.MANDT AS Client_MANDT,
  ckmlhd.MATNR AS MaterialNumber_MATNR,
  ckmlhd.BWTAR AS ValuationType_BWTAR,
  ckmlhd.BWKEY AS ValuationArea_BWKEY,
  ckmlcr.PEINH AS PriceUnit_PEINH,
  ckmlcr.POPER AS PostingPeriod,
  ckmlcr.BDATJ AS FiscalYear,
  ckmlcr.VPRSV AS PriceControlIndicator_VPRSV,
  COALESCE(ckmlcr.STPRS * currency_decimal.CURRFIX, ckmlcr.STPRS) AS StandardCost_STPRS,
  COALESCE(ckmlcr.SALK3 * currency_decimal.CURRFIX, ckmlcr.SALK3) AS ValueOfTotalValuatedStock_SALK3,
  COALESCE(ckmlcr.PVPRS * currency_decimal.CURRFIX, ckmlcr.PVPRS) AS MovingAveragePrice,
  t001.WAERS AS CurrencyKey_WAERS
FROM `{{ project_id_src }}.{{ dataset_cdc_processed_s4 }}.ckmlhd` AS ckmlhd
LEFT JOIN `{{ project_id_src }}.{{ dataset_cdc_processed_s4 }}.ckmlcr` AS ckmlcr
  ON ckmlhd.mandt = ckmlcr.mandt
    AND ckmlhd.kalnr = ckmlcr.kalnr
LEFT JOIN
  `{{ project_id_src }}.{{ dataset_cdc_processed_s4 }}.t001k` AS t001k
  ON ckmlhd.MANDT = t001k.MANDT
    AND ckmlhd.BWKEY = t001k.BWKEY
LEFT JOIN
  `{{ project_id_src }}.{{ dataset_cdc_processed_s4 }}.t001` AS t001
  ON t001.MANDT = t001k.MANDT
    AND t001.BUKRS = t001k.BUKRS
LEFT JOIN
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.currency_decimal` AS currency_decimal
  ON t001.WAERS = currency_decimal.CURRKEY
WHERE ckmlcr.CURTP = '10'
