SELECT
  TVKO.MANDT AS Client_MANDT,
  TVKO.VKORG AS SalesOrg_VKORG,
  TVKO.WAERS AS SalesOrgCurrency_WAERS,
  TVKO.KUNNR AS SalesOrgCustomer_KUNNR,
  TVKO.BUKRS AS CompanyCode_BUKRS,
  T001.LAND1 AS Country_LAND1,
  T001.WAERS AS CoCoCurrency_WAERS,
  T001.PERIV AS FiscalYrVariant_PERIV,
  T001.BUTXT AS Company_BUTXT,
  TVKOT.VTEXT AS SalesOrgName_VTEXT,
  TVKOT.SPRAS AS Language_SPRAS
FROM
  `{{ project_id_src }}.{{ dataset_cdc_processed_s4 }}.tvko` AS TVKO
LEFT OUTER JOIN
  `{{ project_id_src }}.{{ dataset_cdc_processed_s4 }}.t001` AS T001
  ON
    TVKO.MANDT = T001.MANDT
    AND TVKO.BUKRS = T001.BUKRS
INNER JOIN
  `{{ project_id_src }}.{{ dataset_cdc_processed_s4 }}.tvkot` AS TVKOT
  ON
    TVKO.MANDT = TVKOT.MANDT
    AND TVKO.VKORG = TVKOT.VKORG
