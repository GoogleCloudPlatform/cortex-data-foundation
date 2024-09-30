SELECT
  tcurc.mandt AS Client_MANDT, tcurc.waers AS CurrencyCode_WAERS, tcurc.isocd AS CurrencyISO_ISOCD,
  tcurx.currdec AS CurrencyDecimals_CURRDEC, tcurt.spras AS Language,
  tcurt.ktext AS CurrShortText_KTEXT, tcurt.ltext AS CurrLongText_LTEXT
FROM `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.tcurc` AS tcurc
INNER JOIN
  `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.tcurx` AS tcurx ON tcurc.waers = tcurx.currkey
INNER JOIN
  `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.tcurt`AS tcurt
  ON tcurc.waers = tcurt.waers AND tcurc.mandt = tcurt.mandt
