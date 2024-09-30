--## CORTEX-CUSTOMER: This view is made to make UNION ALL compatible with S4 MaterialLedger
--without calendar date dimension and currency conversion
SELECT
  mbew.MANDT AS Client_MANDT,
  mbew.MATNR AS MaterialNumber_MATNR,
  mbew.BWTAR AS ValuationType_BWTAR,
  mbew.BWKEY AS ValuationArea_BWKEY,
  mbew.PEINH AS PriceUnit_PEINH,
  mbew.LFMON AS PostingPeriod,
  mbew.LFGJA AS FiscalYear,
  mbew.VPRSV AS PriceControlIndicator_VPRSV,
  COALESCE(mbew.STPRS * currency_decimal.CURRFIX, mbew.STPRS) AS StandardCost_STPRS,
  COALESCE(mbew.SALK3 * currency_decimal.CURRFIX, mbew.SALK3) AS ValueOfTotalValuatedStock_SALK3,
  COALESCE(mbew.VERPR * currency_decimal.CURRFIX, mbew.VERPR) AS MovingAveragePrice,
  t001.waers AS CurrencyKey_WAERS
FROM `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.mbew` AS mbew
LEFT JOIN
  `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.t001k` AS t001k
  ON mbew.MANDT = t001k.MANDT
    AND mbew.BWKEY = t001k.BWKEY
LEFT JOIN
  `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.t001` AS t001
  ON t001.MANDT = t001k.MANDT
    AND t001.BUKRS = t001k.BUKRS
LEFT JOIN
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.currency_decimal` AS currency_decimal
  ON t001.WAERS = currency_decimal.CURRKEY
UNION ALL
SELECT
  mbewh.MANDT AS Client_MANDT,
  mbewh.MATNR AS MaterialNumber_MATNR,
  mbewh.BWTAR AS ValuationType_BWTAR,
  mbewh.BWKEY AS ValuationArea_BWKEY,
  mbewh.PEINH AS PriceUnit_PEINH,
  mbewh.LFMON AS PostingPeriod,
  mbewh.LFGJA AS FiscalYear,
  mbewh.VPRSV AS PriceControIndicator_VPRSV,
  COALESCE(mbewh.STPRS * currency_decimal.CURRFIX, mbewh.STPRS) AS StandardCost_STPRS,
  COALESCE(mbewh.SALK3 * currency_decimal.CURRFIX, mbewh.SALK3) AS ValueOfTotalValuatedStock_SALK3,
  COALESCE(mbewh.VERPR * currency_decimal.CURRFIX, mbewh.VERPR) AS MovingAveragePrice,
  t001.waers AS CurrencyKey_WAERS
FROM
  `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.mbewh` AS mbewh
LEFT JOIN
  `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.t001k` AS t001k
  ON mbewh.MANDT = t001k.MANDT
    AND mbewh.BWKEY = t001k.BWKEY
LEFT JOIN
  `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.t001` AS t001
  ON t001.MANDT = t001k.MANDT
    AND t001.BUKRS = t001k.BUKRS
LEFT JOIN
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.currency_decimal` AS currency_decimal
  ON t001.WAERS = currency_decimal.CURRKEY
