SELECT DISTINCT
  /** The following dimensions are used as filters on the dashboard **/
  CountriesMD.CountryName_LANDX AS Country,
  SalesOrders.CreationDate_ERDAT AS CreationDate,
  DistributionChannelMD.DistributionChannelName_VTEXT AS DistributionChannel,
  SalesOrganizationsMD.SalesOrgName_VTEXT AS SalesOrganization,
  DivisionsMD.DivisionName_VTEXT AS Division,
  MaterialsMD.MaterialText_MAKTX AS Product,
  CurrencyConversion.ToCurrency_TCURR AS TargetCurrency,
  /** End of filter dimensions **/
  CustomersMD.Name1_NAME1 AS Customer,
  SalesOrders.SalesDocument_VBELN AS SalesOrder,
  SalesOrders.Item_POSNR AS SalesOrderLineItem,
  Deliveries.DeliveryDate_LFDAT AS ReqDeliveryDate,
  Deliveries.Date__proofOfDelivery___PODAT AS ActualDeliveryDate,
  SalesOrders.BaseUnitofMeasure_MEINS AS BaseUom,
  SalesOrders.Currency_WAERK AS LocalCurrency,
  SalesOrders.NetValueOfTheSalesOrderInDocumentCurrency_NETWR AS SalesOrderNetValueLocalCurrency,
  SalesOrders.CumulativeOrderQuantity_KWMENG AS SalesOrderQty,
  SalesOrders.SalesOrderValueLineItemSourceCurrency AS SalesOrderValueLocalCurrency,
  SalesOrders.NetValueOfTheSalesOrderInDocumentCurrency_NETWR * CurrencyConversion.ExchangeRate_UKURS AS SalesOrderNetValueTargetCurrency,
  SalesOrders.SalesOrderValueLineItemSourceCurrency * CurrencyConversion.ExchangeRate_UKURS AS SalesOrderValueTargetCurrency,
  IF(
    SalesOrders.RejectionReason_ABGRU IS NOT NULL,
    'Canceled',
    IF(
      Deliveries.ActualQuantityDelivered_InSalesUnits_LFIMG = SalesOrders.CumulativeOrderQuantity_KWMENG
      AND SalesOrders.CumulativeOrderQuantity_KWMENG = Billing.ActualBilledQuantity_FKIMG,
      'Closed',
      'Open'
    )
  ) AS OrderStatus,
  (
    Deliveries.DeliveryBlock_documentHeader_LIFSK IS NOT NULL
    OR Deliveries.BillingBlockInSdDocument_FAKSK IS NOT NULL
  ) AS IsOrderBlocked,
  SalesOrders.DocumentCategory_VBTYP = 'C' AS IsIncomingOrder,
  SAFE_DIVIDE(1, CurrencyConversion.ExchangeRate_UKURS) AS ExchangeRate

FROM
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.SalesOrders_V2` AS SalesOrders
LEFT JOIN
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.Deliveries` AS Deliveries
  ON
    SalesOrders.SalesDocument_VBELN = Deliveries.SalesOrderNumber_VGBEL
    AND SalesOrders.Item_POSNR = Deliveries.SalesOrderItem_VGPOS
    AND SalesOrders.Client_MANDT = Deliveries.Client_MANDT
LEFT JOIN
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.Billing` AS Billing
  ON
    SalesOrders.SalesDocument_VBELN = Billing.SalesDocument_AUBEL
    AND SalesOrders.Item_POSNR = Billing.SalesDocumentItem_AUPOS
    AND SalesOrders.Client_MANDT = Billing.Client_MANDT
LEFT JOIN
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.CustomersMD` AS CustomersMD
  ON
    SalesOrders.SoldtoParty_KUNNR = CustomersMD.CustomerNumber_KUNNR
    AND SalesOrders.Client_MANDT = CustomersMD.Client_MANDT
LEFT JOIN
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.MaterialsMD` AS MaterialsMD
  ON
    SalesOrders.MaterialNumber_MATNR = MaterialsMD.MaterialNumber_MATNR
    AND SalesOrders.Client_MANDT = MaterialsMD.Client_MANDT
LEFT JOIN
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.SalesOrganizationsMD` AS SalesOrganizationsMD
  ON
    SalesOrders.Client_MANDT = SalesOrganizationsMD.Client_MANDT
    AND SalesOrders.SalesOrganization_VKORG = SalesOrganizationsMD.SalesOrg_VKORG
    AND SalesOrganizationsMD.Language_SPRAS = MaterialsMD.Language_SPRAS
LEFT JOIN
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.DistributionChannelsMD` AS DistributionChannelMD
  ON
    SalesOrders.Client_MANDT = DistributionChannelMD.Client_MANDT
    AND SalesOrders.DistributionChannel_VTWEG = DistributionChannelMD.DistributionChannel_VTWEG
    AND DistributionChannelMD.Language_SPRAS = MaterialsMD.Language_SPRAS
LEFT JOIN
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.DivisionsMD` AS DivisionsMD
  ON MaterialsMD.Client_MANDT = DivisionsMD.Client_MANDT
    AND MaterialsMD.Division_SPART = DivisionsMD.Division_SPART
    AND DivisionsMD.LanguageKey_SPRAS = MaterialsMD.Language_SPRAS
LEFT JOIN
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.CountriesMD` AS CountriesMD
  ON
    SalesOrders.Client_MANDT = CountriesMD.Client_MANDT
    AND CustomersMD.CountryKey_LAND1 = CountriesMD.CountryKey_LAND1
    AND CountriesMD.Language_SPRAS = MaterialsMD.Language_SPRAS
LEFT JOIN `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.CurrencyConversion` AS CurrencyConversion
  ON SalesOrders.Client_MANDT = CurrencyConversion.Client_MANDT
    AND SalesOrders.CurrencyHdr_WAERK = CurrencyConversion.FromCurrency_FCURR
    AND SalesOrders.DocumentDate_AUDAT = CurrencyConversion.ConvDate
    ##CORTEX-CUSTOMER Modify target currency based on your requirement
    AND CurrencyConversion.ToCurrency_TCURR IN UNNEST({{ sap_currencies }})
    ##CORTEX-CUSTOMER Modify the exchange rate type based on your requirement
    AND CurrencyConversion.ExchangeRateType_KURST = 'M'
##CORTEX-CUSTOMER Modify the below baseline filters based on requirement
WHERE
  (
    SalesOrders.Client_MANDT = '{{ mandt }}'
    AND MaterialsMD.Language_SPRAS IN UNNEST({{ sap_languages }})
  )
