SELECT
  TVTW.MANDT AS Client_MANDT,
  TVTW.VTWEG AS DistributionChannel_VTWEG,
  TVTWT.SPRAS AS Language_SPRAS,
  TVTWT.VTEXT AS DistributionChannelName_VTEXT
FROM
  `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.tvtw` AS TVTW
INNER JOIN
  `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.tvtwt` AS TVTWT
  ON
    TVTW.MANDT = TVTWT.MANDT
    AND TVTW.VTWEG = TVTWT.VTWEG
