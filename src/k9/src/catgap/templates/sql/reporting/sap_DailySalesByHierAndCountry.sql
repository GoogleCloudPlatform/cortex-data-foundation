#--  Copyright 2023 Google Inc.
#
#--  Licensed under the Apache License, Version 2.0 (the "License");
#--  you may not use this file except in compliance with the License.
#--  You may obtain a copy of the License at
#
#--      http://www.apache.org/licenses/LICENSE-2.0
#
#--  Unless required by applicable law or agreed to in writing, software
#--  distributed under the License is distributed on an "AS IS" BASIS,
#--  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#--  See the License for the specific language governing permissions and
#--  limitations under the License.

#-- Daily sales grouped by product hierarchy and country.

CREATE OR REPLACE VIEW `{{ project_id_tgt }}.{{ dataset_k9_reporting }}.DailySalesByHierAndCountry` AS
(
WITH sales_orders AS (
SELECT DISTINCT
  CountriesMD.CountryIsoCode_INTCA AS CountryCode,
  SalesOrders.CreationDate_ERDAT AS CreationDate,
  SalesOrders.ProductHierarchy_PRODH as ProductHierarchy,
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
  IF(SalesOrders.RejectionReason_ABGRU IS NOT NULL,
     'Canceled',
     IF(Deliveries.ActualQuantityDelivered_InSalesUnits_LFIMG = SalesOrders.CumulativeOrderQuantity_KWMENG
        AND SalesOrders.CumulativeOrderQuantity_KWMENG = Billing.ActualBilledQuantity_FKIMG,
        'Closed',
        'Open')) AS OrderStatus,
  (Deliveries.DeliveryBlock_documentHeader_LIFSK IS NOT NULL
    OR Deliveries.BillingBlockInSdDocument_FAKSK IS NOT NULL) AS IsOrderBlocked,
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
    AND CurrencyConversion.ToCurrency_TCURR = 'USD'
    ##CORTEX-CUSTOMER Modify the exchange rate type based on your requirement
    AND CurrencyConversion.ExchangeRateType_KURST = 'M'
##CORTEX-CUSTOMER Modify the below baseline filters based on requirement
WHERE
  ( SalesOrders.Client_MANDT = '{{ mandt }}'
    AND MaterialsMD.Language_SPRAS in ( 'E' ) # TODO: (vladkol) support multiple languages
    AND SalesOrders.ProductHierarchy_PRODH IS NOT NULL
  )
)

SELECT
  sales_orders.CreationDate AS OrderDate,
  sales_orders.ProductHierarchy,
  fullhier AS ProductHierarchyText,
  sales_orders.CountryCode,
  SUM(SalesOrderValueTargetCurrency) as TotalSalesValue
FROM sales_orders
INNER JOIN `{{ project_id_src }}.{{ dataset_k9_processing }}.prod_hierarchy` ph
ON sales_orders.ProductHierarchy = ph.prodh
WHERE OrderStatus != "Canceled"
GROUP BY 1,2,3,4
)
