#-- Copyright 2024 Google LLC
#--
#-- Licensed under the Apache License, Version 2.0 (the "License");
#-- you may not use this file except in compliance with the License.
#-- You may obtain a copy of the License at
#--
#--     https://www.apache.org/licenses/LICENSE-2.0
#--
#-- Unless required by applicable law or agreed to in writing, software
#-- distributed under the License is distributed on an "AS IS" BASIS,
#-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#-- See the License for the specific language governing permissions and
#-- limitations under the License.

-- ## EXPERIMENTAL

--The granularity of this query is Client,Vendor,Purchase Date,Invoice Date and Target Currency.
WITH
  CurrencyConversion AS (
    SELECT
      Client_MANDT, FromCurrency_FCURR, ToCurrency_TCURR, ConvDate, ExchangeRate_UKURS
    FROM
      `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.CurrencyConversion`
    WHERE
      Client_MANDT = '{{ mandt }}'
      --## CORTEX-CUSTOMER Modify the exchange rate type based on your requirement
      AND ExchangeRateType_KURST = 'M'
      AND ToCurrency_TCURR {{ currency }}
  ),

  POScheduleLine AS (
    SELECT
      PurchaseOrders.Client_MANDT,
      PurchaseOrders.DocumentNumber_EBELN,
      PurchaseOrders.Item_EBELP,
      PurchaseOrders.DeliveryCompletedFlag_ELIKZ,
      PurchaseOrders.PurchasingDocumentDate_BEDAT,
      PurchaseOrders.YearOfPurchasingDocumentDate_BEDAT,
      PurchaseOrders.MonthOfPurchasingDocumentDate_BEDAT,
      PurchaseOrders.CurrencyKey_WAERS,
      PurchaseOrders.POQuantity_MENGE,
      PurchaseOrders.VendorAccountNumber_LIFNR,
      PurchaseOrders.UnderdeliveryToleranceLimit_UNTTO,
      PurchaseOrders.OverdeliveryToleranceLimit_UEBTO,
      POScheduleLine.ItemDeliveryDate_EINDT,
      COALESCE(
        (PurchaseOrders.UnderdeliveryToleranceLimit_UNTTO * PurchaseOrders.POQuantity_MENGE) / 100,
        0) AS UnderdeliveryToleranceLimit,
      COALESCE(
        (PurchaseOrders.OverdeliveryToleranceLimit_UEBTO * PurchaseOrders.POQuantity_MENGE) / 100,
        0) AS OverdeliveryToleranceLimit
    FROM
      (
        SELECT
          Client_MANDT,
          DocumentNumber_EBELN,
          Item_EBELP,
          DeliveryCompletedFlag_ELIKZ,
          PurchasingDocumentDate_BEDAT,
          YearOfPurchasingDocumentDate_BEDAT,
          MonthOfPurchasingDocumentDate_BEDAT,
          CurrencyKey_WAERS,
          POQuantity_MENGE,
          VendorAccountNumber_LIFNR,
          UnderdeliveryToleranceLimit_UNTTO,
          OverdeliveryToleranceLimit_UEBTO
        FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.PurchaseDocuments`
        --DocumentType_BSART='NB' or 'ENB' represents Standard PO
        --ItemCategoryinPurchasingDocument_PSTYP ='2' represents Consignment PO
        WHERE
          Client_MANDT = '{{ mandt }}'
          AND DocumentType_BSART IN ('NB', 'ENB')
          AND ItemCategoryinPurchasingDocument_PSTYP != '2'
      ) AS PurchaseOrders
    --PO Schedule Lines details for PO Item
    LEFT JOIN
      (
        SELECT
          Client_MANDT, PurchasingDocumentNumber_EBELN, ItemNumberOfPurchasingDocument_EBELP,
          MAX(ItemDeliveryDate_EINDT) AS ItemDeliveryDate_EINDT
        FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.POSchedule`
        WHERE Client_MANDT = '{{ mandt }}'
        GROUP BY Client_MANDT, PurchasingDocumentNumber_EBELN, ItemNumberOfPurchasingDocument_EBELP
      ) AS POScheduleLine
      ON
        PurchaseOrders.Client_MANDT = POScheduleLine.Client_MANDT
        AND PurchaseOrders.DocumentNumber_EBELN = POScheduleLine.PurchasingDocumentNumber_EBELN
        AND PurchaseOrders.Item_EBELP = POScheduleLine.ItemNumberOfPurchasingDocument_EBELP
  ),

  --Getting item historical data.
  --This join results in mutiple rows for the same item.
  --This will be aggregated and brought back at Item level in the next CTE.
  POGoodsReceipt AS (
    SELECT
      POScheduleLine.Client_MANDT,
      POScheduleLine.DocumentNumber_EBELN,
      POScheduleLine.Item_EBELP,
      POScheduleLine.DeliveryCompletedFlag_ELIKZ,
      POScheduleLine.PurchasingDocumentDate_BEDAT,
      POScheduleLine.YearOfPurchasingDocumentDate_BEDAT,
      POScheduleLine.MonthOfPurchasingDocumentDate_BEDAT,
      POScheduleLine.CurrencyKey_WAERS,
      POScheduleLine.ItemDeliveryDate_EINDT,
      POScheduleLine.POQuantity_MENGE,
      POScheduleLine.UnderdeliveryToleranceLimit_UNTTO,
      POScheduleLine.OverdeliveryToleranceLimit_UEBTO,
      POScheduleLine.UnderdeliveryToleranceLimit,
      POScheduleLine.OverdeliveryToleranceLimit,
      POScheduleLine.VendorAccountNumber_LIFNR,

      --Vendor Cycle Time in Days
      IF(
        --DeliveryCompletedFlag_ELIKZ = 'X' represents Completed Delivery
        POScheduleLine.DeliveryCompletedFlag_ELIKZ = 'X',
        COALESCE(
          DATE_DIFF(
            IF(
              --MovementType__inventoryManagement___BWART='101' represents Goods Receipt Orders
              POOrderHistory.MovementType__inventoryManagement___BWART = '101',
              MAX(POOrderHistory.PostingDateInTheDocument_BUDAT) OVER (
                PARTITION BY POScheduleLine.Client_MANDT,
                  POScheduleLine.DocumentNumber_EBELN,
                  POScheduleLine.Item_EBELP),
              NULL),
            POScheduleLine.PurchasingDocumentDate_BEDAT,
            DAY),
          0),
        NULL) AS VendorCycleTimeInDays,

      --Vendor Quality (Rejection)
      --MovementType__inventoryManagement___BWART='122' or '161' represents Rejected Orders
      POOrderHistory.MovementType__inventoryManagement___BWART IN ('122', '161') AS IsRejected,

      --Vendor On Time Delivery
      IF(
        POScheduleLine.DeliveryCompletedFlag_ELIKZ = 'X',
        IF(
          POOrderHistory.MovementType__inventoryManagement___BWART = '101',
          POOrderHistory.PostingDateInTheDocument_BUDAT,
          NULL) <= POScheduleLine.ItemDeliveryDate_EINDT,
        NULL) AS IsDeliveredOnTime,

      --Vendor InFull Delivery
      IF(
        POScheduleLine.DeliveryCompletedFlag_ELIKZ = 'X',
        IF(
          POScheduleLine.UnderdeliveryToleranceLimit_UNTTO IS NULL
          AND POScheduleLine.OverdeliveryToleranceLimit_UEBTO IS NULL,
          POOrderHistory.GoodsReceivedQuantity >= POScheduleLine.POQuantity_MENGE,
          POOrderHistory.GoodsReceivedQuantity >= (POScheduleLine.POQuantity_MENGE - POScheduleLine.UnderdeliveryToleranceLimit)
          OR POOrderHistory.GoodsReceivedQuantity <= (POScheduleLine.POQuantity_MENGE + POScheduleLine.OverdeliveryToleranceLimit)
        ),
        NULL) AS IsDeliveredInFull,

      --Vendor Invoice Accuracy (Goods Receipt)
      IF(
        POScheduleLine.DeliveryCompletedFlag_ELIKZ = 'X',
        IF(
          POScheduleLine.UnderdeliveryToleranceLimit_UNTTO IS NULL
          AND POScheduleLine.OverdeliveryToleranceLimit_UEBTO IS NULL,
          POScheduleLine.POQuantity_MENGE = POOrderHistory.GoodsReceivedQuantity,
          POOrderHistory.GoodsReceivedQuantity
          BETWEEN POScheduleLine.POQuantity_MENGE - POScheduleLine.UnderdeliveryToleranceLimit
          AND POScheduleLine.POQuantity_MENGE + POScheduleLine.OverdeliveryToleranceLimit
        ),
        NULL) AS IsGoodsReceiptAccurate
    FROM
      POScheduleLine
    LEFT JOIN
      (
        SELECT
          Client_MANDT,
          PurchasingDocumentNumber_EBELN,
          ItemNumberOfPurchasingDocument_EBELP,
          MovementType__inventoryManagement___BWART,
          AmountInLocalCurrency_DMBTR,
          CurrencyKey_WAERS,
          PostingDateInTheDocument_BUDAT,
          Quantity_MENGE,
          SUM(
            IF(MovementType__inventoryManagement___BWART = '101', Quantity_MENGE, (Quantity_MENGE * -1))
          ) OVER (
            PARTITION BY
              Client_MANDT, PurchasingDocumentNumber_EBELN, ItemNumberOfPurchasingDocument_EBELP
          ) AS GoodsReceivedQuantity
        FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.PurchaseDocumentsHistory`
        WHERE
          Client_MANDT = '{{ mandt }}'
          --TransactioneventType_VGABE='1' represents Goods Receipt
          AND TransactioneventType_VGABE = '1'
          --MovementType__inventoryManagement___BWART='102' represents Returned Orders
          --MovementType__inventoryManagement___BWART='101' represents Goods Receipt Orders
          --MovementType__inventoryManagement___BWART='122', '161' represents Rejected Orders
          AND MovementType__inventoryManagement___BWART IN ('101', '102', '161', '122')
      ) AS POOrderHistory
      ON
        POScheduleLine.Client_MANDT = POOrderHistory.Client_MANDT
        AND POScheduleLine.DocumentNumber_EBELN = POOrderHistory.PurchasingDocumentNumber_EBELN
        AND POScheduleLine.Item_EBELP = POOrderHistory.ItemNumberOfPurchasingDocument_EBELP
  ),

  PurchaseDocuments AS (
    SELECT
      POGoodsReceipt.Client_MANDT,
      POGoodsReceipt.DocumentNumber_EBELN,
      POGoodsReceipt.Item_EBELP,
      MAX(POGoodsReceipt.CurrencyKey_WAERS) AS CurrencyKey_WAERS,
      AVG(POGoodsReceipt.POQuantity_MENGE) AS POQuantity_MENGE,
      MAX(
        POGoodsReceipt.PurchasingDocumentDate_BEDAT
      ) AS PurchasingDocumentDate_BEDAT,
      MAX(
        POGoodsReceipt.YearOfPurchasingDocumentDate_BEDAT
      ) AS YearOfPurchasingDocumentDate_BEDAT,
      MAX(
        POGoodsReceipt.MonthOfPurchasingDocumentDate_BEDAT
      ) AS MonthOfPurchasingDocumentDate_BEDAT,
      MAX(POGoodsReceipt.VendorAccountNumber_LIFNR) AS VendorAccountNumber_LIFNR,
      MAX(POGoodsReceipt.VendorCycleTimeInDays) AS VendorCycleTimeInDays,
      LOGICAL_OR(POGoodsReceipt.IsRejected) AS IsRejected,
      LOGICAL_AND(POGoodsReceipt.IsDeliveredOnTime) AS IsDeliveredOnTime,
      LOGICAL_AND(POGoodsReceipt.IsDeliveredInFull) AS IsDeliveredInFull,
      LOGICAL_AND(POGoodsReceipt.IsGoodsReceiptAccurate) AS IsGoodsReceiptAccurate
    FROM POGoodsReceipt
    GROUP BY
      POGoodsReceipt.Client_MANDT,
      POGoodsReceipt.DocumentNumber_EBELN,
      POGoodsReceipt.Item_EBELP
  ),

  GoodsInvoiceReceipt AS (
    SELECT
      PurchaseDocuments.Client_MANDT,
      PurchaseDocuments.DocumentNumber_EBELN,
      PurchaseDocuments.Item_EBELP,
      PurchaseDocuments.CurrencyKey_WAERS,
      PurchaseDocuments.PurchasingDocumentDate_BEDAT,
      PurchaseDocuments.YearOfPurchasingDocumentDate_BEDAT,
      PurchaseDocuments.MonthOfPurchasingDocumentDate_BEDAT,
      PurchaseDocuments.VendorAccountNumber_LIFNR,
      POInvoiceReceipt.InvoiceAmountInSourceCurrency,
      POInvoiceReceipt.InvoiceCurrencyKey,
      POInvoiceReceipt.InvoiceDate,
      PurchaseDocuments.VendorCycleTimeInDays,
      PurchaseDocuments.IsRejected,
      PurchaseDocuments.IsDeliveredOnTime,
      PurchaseDocuments.IsDeliveredInFull,
      PurchaseDocuments.IsGoodsReceiptAccurate,
      --Vendor Invoice Accuracy
      IF(
        PurchaseDocuments.IsGoodsReceiptAccurate IS NULL
        OR POInvoiceReceipt.InvoiceQuantity IS NULL,
        NULL,
        PurchaseDocuments.IsGoodsReceiptAccurate
        AND PurchaseDocuments.POQuantity_MENGE = POInvoiceReceipt.InvoiceQuantity
      ) AS IsVendorInvoiceAccurate
    FROM PurchaseDocuments
    LEFT JOIN
      (
        SELECT
          Client_MANDT,
          PurchasingDocumentNumber_EBELN,
          ItemNumberOfPurchasingDocument_EBELP,
          SUM(Quantity_MENGE) AS InvoiceQuantity,
          SUM(AmountInLocalCurrency_DMBTR) AS InvoiceAmountInSourceCurrency,
          MAX(CurrencyKey_WAERS) AS InvoiceCurrencyKey,
          --Invoice Date
          MAX(PostingDateInTheDocument_BUDAT) AS InvoiceDate
        FROM
          `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.PurchaseDocumentsHistory`
        WHERE Client_MANDT = '{{ mandt }}'
          --TransactioneventType_VGABE='2' represents Invoice Receipt
          AND TransactioneventType_VGABE = '2'
        GROUP BY Client_MANDT, PurchasingDocumentNumber_EBELN, ItemNumberOfPurchasingDocument_EBELP
      ) AS POInvoiceReceipt
      ON
        PurchaseDocuments.Client_MANDT = POInvoiceReceipt.Client_MANDT
        AND PurchaseDocuments.DocumentNumber_EBELN = POInvoiceReceipt.PurchasingDocumentNumber_EBELN
        AND PurchaseDocuments.Item_EBELP = POInvoiceReceipt.ItemNumberOfPurchasingDocument_EBELP
  ),

  --Aggregating data at Vendor,Purchase and Invoice Date
  VendorPerformance AS (
    SELECT
      MAX(GoodsInvoiceReceipt.Client_MANDT) AS Client_MANDT,
      GoodsInvoiceReceipt.VendorAccountNumber_LIFNR,
      GoodsInvoiceReceipt.PurchasingDocumentDate_BEDAT,
      GoodsInvoiceReceipt.InvoiceDate,
      MAX(
        GoodsInvoiceReceipt.YearOfPurchasingDocumentDate_BEDAT
      ) AS YearOfPurchasingDocumentDate_BEDAT,
      MAX(
        GoodsInvoiceReceipt.MonthOfPurchasingDocumentDate_BEDAT
      ) AS MonthOfPurchasingDocumentDate_BEDAT,
      SUM(
        GoodsInvoiceReceipt.InvoiceAmountInSourceCurrency
      ) AS InvoiceAmountInSourceCurrency,
      MAX(GoodsInvoiceReceipt.CurrencyKey_WAERS) AS CurrencyKey_WAERS,
      AVG(GoodsInvoiceReceipt.VendorCycleTimeInDays) AS VendorLeadTime,
      LOGICAL_OR(GoodsInvoiceReceipt.IsRejected) AS IsRejected,
      COUNTIF(GoodsInvoiceReceipt.IsRejected) AS RejectedOrders,
      COUNT(
        CONCAT(GoodsInvoiceReceipt.DocumentNumber_EBELN, GoodsInvoiceReceipt.Item_EBELP)
      ) AS TotalOrders,
      COUNTIF(GoodsInvoiceReceipt.IsDeliveredOnTime) AS OnTimeOrders,
      COUNTIF(NOT GoodsInvoiceReceipt.IsDeliveredOnTime) AS LateOrders,
      COUNTIF(GoodsInvoiceReceipt.IsDeliveredOnTime IS NULL) AS NotCompletedOrders,
      COUNTIF(
        GoodsInvoiceReceipt.IsDeliveredOnTime AND GoodsInvoiceReceipt.IsDeliveredInFull
      ) AS OTIFOrders,
      COUNTIF(
        NOT GoodsInvoiceReceipt.IsDeliveredOnTime OR NOT GoodsInvoiceReceipt.IsDeliveredInFull
      ) AS NotOTIFOrders,
      COUNTIF(
        GoodsInvoiceReceipt.IsDeliveredOnTime IS NULL OR GoodsInvoiceReceipt.IsDeliveredInFull IS NULL
      ) AS NotCompletedOTIFOrders,
      COUNTIF(GoodsInvoiceReceipt.IsVendorInvoiceAccurate) AS AccurateInvoices,
      COUNTIF(NOT GoodsInvoiceReceipt.IsVendorInvoiceAccurate) AS InaccurateInvoices,
      COUNTIF(GoodsInvoiceReceipt.IsVendorInvoiceAccurate IS NULL) AS NotCompletedInvoices
    FROM
      GoodsInvoiceReceipt
    GROUP BY
      GoodsInvoiceReceipt.VendorAccountNumber_LIFNR,
      GoodsInvoiceReceipt.PurchasingDocumentDate_BEDAT,
      GoodsInvoiceReceipt.InvoiceDate
  )
SELECT
  VendorPerformance.Client_MANDT,
  VendorPerformance.VendorAccountNumber_LIFNR,
  Vendors.CountryKey_LAND1,
  Vendors.NAME1 AS VendorName,
  ESGScore.OrganizationName,
  ESGScore.Industry,
  ESGScore.SectorCategory,
  Vendors.CreditInformationNumber_KRAUS,
  VendorPerformance.PurchasingDocumentDate_BEDAT,
  VendorPerformance.InvoiceDate,
  VendorPerformance.InvoiceAmountInSourceCurrency,
  VendorPerformance.CurrencyKey_WAERS,
  CurrencyConversion.ExchangeRate_UKURS,
  CurrencyConversion.ToCurrency_TCURR AS TargetCurrency_TCURR,
  VendorPerformance.InvoiceAmountInSourceCurrency * CurrencyConversion.ExchangeRate_UKURS AS InvoiceAmountInTargetCurrency,
  ROUND(VendorPerformance.VendorLeadTime, 2) AS VendorLeadTime,
  VendorPerformance.IsRejected,
  VendorPerformance.RejectedOrders,
  VendorPerformance.TotalOrders,
  VendorPerformance.LateOrders,
  VendorPerformance.OnTimeOrders,
  VendorPerformance.NotCompletedOrders,
  VendorPerformance.OTIFOrders,
  VendorPerformance.NotOTIFOrders,
  VendorPerformance.NotCompletedOTIFOrders,
  VendorPerformance.AccurateInvoices,
  VendorPerformance.InaccurateInvoices,
  VendorPerformance.NotCompletedInvoices,
  ESGScore.ESGRanking,
  ESGScore.ESGRankingAveragePeerScore,
  ESGScore.ESGRankingPeerPercentile,
  ESGScore.ESGDataDepth,
  ESGScore.EnvironmentalRanking,
  ESGScore.EnergyManagementScore,
  ESGScore.WaterManagementScore,
  ESGScore.MaterialSourcingAndManagementScore,
  ESGScore.WasteAndHazardsManagementScore,
  ESGScore.LandUseAndBiodiversityScore,
  ESGScore.PollutionPreventionAndManagementScore,
  ESGScore.GHGEmissionsScore,
  ESGScore.ClimateRiskScore,
  ESGScore.EnvironmentalCertificationsScore,
  ESGScore.EnvironmentalComplianceScore,
  ESGScore.EnvironmentalOpportunitiesScore,
  ESGScore.SocialRanking,
  ESGScore.DiversityAndInclusionScore,
  ESGScore.HealthAndSafetyScore,
  ESGScore.HumanRightAbusesScore,
  ESGScore.LaborRelationsScore,
  ESGScore.TrainingAndEducationScore,
  ESGScore.CyberRiskScore,
  ESGScore.ProductManagementAndQualityScore,
  ESGScore.ProductAndServicesScore,
  ESGScore.DataPrivacyScore,
  ESGScore.CorporatePhilanthropyScore,
  ESGScore.CommunityEngagementScore,
  ESGScore.SupplierEngagementScore,
  ESGScore.SocialRelatedCertificatesScore,
  ESGScore.GovernanceRanking,
  ESGScore.BusinessEthicsScore,
  ESGScore.BoardAccountabilityScore,
  ESGScore.BusinessTransparencyScore,
  ESGScore.CorporateComplianceBehaviorsScore,
  ESGScore.GovernanceRelatedCertificationsScore,
  ESGScore.ShareholderRightsScore,
  ESGScore.BusinessResilienceAndSustainabilityScore
FROM VendorPerformance
LEFT JOIN
  CurrencyConversion
  ON
    VendorPerformance.Client_MANDT = CurrencyConversion.Client_MANDT
    AND VendorPerformance.CurrencyKey_WAERS = CurrencyConversion.FromCurrency_FCURR
    AND VendorPerformance.PurchasingDocumentDate_BEDAT = CurrencyConversion.ConvDate
INNER JOIN -- noqa: disable=L042
  (
    SELECT
      Client_MANDT,
      AccountNumberOfVendorOrCreditor_LIFNR,
      CreditInformationNumber_KRAUS,
      CountryKey_LAND1,
      NAME1
    FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.VendorsMD`
    WHERE
      Client_MANDT = '{{ mandt }}'
      AND ValidToDate_DATE_TO = '9999-12-31'
  ) AS Vendors -- noqa: enable=all
  ON
    VendorPerformance.Client_MANDT = Vendors.Client_MANDT
    AND VendorPerformance.VendorAccountNumber_LIFNR = Vendors.AccountNumberOfVendorOrCreditor_LIFNR
LEFT JOIN
  `{{ project_id_tgt }}.{{ k9_datasets_reporting }}.SustainableSourcing` AS ESGScore
  ON
    Vendors.CreditInformationNumber_KRAUS = ESGScore.CreditInformationNumber_KRAUS
    AND VendorPerformance.PurchasingDocumentDate_BEDAT
    BETWEEN ESGScore.LoadDate AND ESGScore.EndDate
