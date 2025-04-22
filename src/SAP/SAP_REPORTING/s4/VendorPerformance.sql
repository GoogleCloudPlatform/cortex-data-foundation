WITH
  LanguageKey AS (
    SELECT LanguageKey_SPRAS
    FROM
      `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.Languages_T002`
    WHERE LanguageKey_SPRAS IN UNNEST({{ sap_languages }})
  ),

  CurrencyConversion AS (
    SELECT
      Client_MANDT, FromCurrency_FCURR, ToCurrency_TCURR, ConvDate, ExchangeRate_UKURS
    FROM
      `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.CurrencyConversion`
    WHERE
      ToCurrency_TCURR IN UNNEST({{ sap_currencies }})
      --##CORTEX-CUSTOMER Modify the exchange rate type based on your requirement
      AND ExchangeRateType_KURST = 'M'
  ),

  -- Purchase Order Item level details
  PurchaseOrderScheduleLine AS (
    SELECT
      PurchaseOrders.Client_MANDT,
      PurchaseOrders.DocumentNumber_EBELN,
      PurchaseOrders.Item_EBELP,
      PurchaseOrders.DeliveryCompletedFlag_ELIKZ,
      PurchaseOrders.PurchasingDocumentDate_BEDAT,
      PurchaseOrders.NetOrderValueinPOCurrency_NETWR,
      PurchaseOrders.CurrencyKey_WAERS,
      PurchaseOrders.POQuantity_MENGE,
      PurchaseOrders.UoM_MEINS,
      PurchaseOrders.NetPrice_NETPR,
      PurchaseOrders.CreatedOn_AEDAT,
      PurchaseOrders.Status_STATU,
      PurchaseOrders.MaterialNumber_MATNR,
      PurchaseOrders.MaterialType_MTART,
      PurchaseOrders.MaterialGroup_MATKL,
      PurchaseOrders.PurchasingOrganization_EKORG,
      PurchaseOrders.PurchasingGroup_EKGRP,
      PurchaseOrders.VendorAccountNumber_LIFNR,
      PurchaseOrders.Company_BUKRS,
      PurchaseOrders.Plant_WERKS,
      PurchaseOrders.UnderdeliveryToleranceLimit_UNTTO,
      PurchaseOrders.OverdeliveryToleranceLimit_UEBTO,
      POScheduleLine.ItemDeliveryDate_EINDT,
      POScheduleLine.OrderDateOfScheduleLine_BEDAT,
      PurchaseOrders.YearOfPurchasingDocumentDate_BEDAT,
      PurchaseOrders.MonthOfPurchasingDocumentDate_BEDAT,
      PurchaseOrders.WeekOfPurchasingDocumentDate_BEDAT,
      PurchaseOrders.Language_SPRAS,
      COALESCE(
        (PurchaseOrders.UnderdeliveryToleranceLimit_UNTTO * PurchaseOrders.POQuantity_MENGE) / 100,
        0
      ) AS UnderdeliveryToleranceLimit,
      COALESCE(
        (PurchaseOrders.OverdeliveryToleranceLimit_UEBTO * PurchaseOrders.POQuantity_MENGE) / 100,
        0
      ) AS OverdeliveryToleranceLimit
    FROM
      `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.PurchaseDocuments` AS PurchaseOrders
    -- PO Schedule Lines details for PO Item
    LEFT JOIN
      (
        SELECT
          Client_MANDT, PurchasingDocumentNumber_EBELN, ItemNumberOfPurchasingDocument_EBELP,
          MAX(ItemDeliveryDate_EINDT) AS ItemDeliveryDate_EINDT,
          MAX(OrderDateOfScheduleLine_BEDAT) AS OrderDateOfScheduleLine_BEDAT
        FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.POSchedule`
        GROUP BY Client_MANDT, PurchasingDocumentNumber_EBELN, ItemNumberOfPurchasingDocument_EBELP
      ) AS POScheduleLine
      ON
        PurchaseOrders.Client_MANDT = POScheduleLine.Client_MANDT
        AND PurchaseOrders.DocumentNumber_EBELN = POScheduleLine.PurchasingDocumentNumber_EBELN
        AND PurchaseOrders.Item_EBELP = POScheduleLine.ItemNumberOfPurchasingDocument_EBELP
    --## DocumentType_BSART='NB' or 'ENB'-> Standrad PO
    --## ItemCategoryinPurchasingDocument_PSTYP ='2'-> Consignment PO
    WHERE PurchaseOrders.DocumentType_BSART IN ('NB', 'ENB')
      AND PurchaseOrders.ItemCategoryinPurchasingDocument_PSTYP != '2'
  ),

  -- Getting item historical data.
  -- This join results in mutiple rows for the same item.
  -- This will be aggreagated and brought back at Item level in the next step.
  PurchaseOrdersGoodsReceipt AS (
    SELECT
      PurchaseOrderScheduleLine.Client_MANDT,
      PurchaseOrderScheduleLine.DocumentNumber_EBELN,
      PurchaseOrderScheduleLine.Item_EBELP,
      PurchaseOrderScheduleLine.DeliveryCompletedFlag_ELIKZ,
      PurchaseOrderScheduleLine.PurchasingDocumentDate_BEDAT,
      PurchaseOrderScheduleLine.NetOrderValueinPOCurrency_NETWR,
      PurchaseOrderScheduleLine.CurrencyKey_WAERS,
      PurchaseOrderScheduleLine.ItemDeliveryDate_EINDT,
      PurchaseOrderScheduleLine.OrderDateOfScheduleLine_BEDAT,
      PurchaseOrderScheduleLine.POQuantity_MENGE,
      PurchaseOrderScheduleLine.UoM_MEINS,
      PurchaseOrderScheduleLine.NetPrice_NETPR,
      PurchaseOrderScheduleLine.CreatedOn_AEDAT,
      PurchaseOrderScheduleLine.Status_STATU,
      PurchaseOrderScheduleLine.MaterialNumber_MATNR,
      PurchaseOrderScheduleLine.MaterialType_MTART,
      PurchaseOrderScheduleLine.MaterialGroup_MATKL,
      PurchaseOrderScheduleLine.PurchasingOrganization_EKORG,
      PurchaseOrderScheduleLine.PurchasingGroup_EKGRP,
      PurchaseOrderScheduleLine.Company_BUKRS,
      PurchaseOrderScheduleLine.UnderdeliveryToleranceLimit_UNTTO,
      PurchaseOrderScheduleLine.OverdeliveryToleranceLimit_UEBTO,
      PurchaseOrderScheduleLine.UnderdeliveryToleranceLimit,
      PurchaseOrderScheduleLine.OverdeliveryToleranceLimit,
      PurchaseOrderScheduleLine.VendorAccountNumber_LIFNR,
      PurchaseOrderScheduleLine.Plant_WERKS,
      PurchaseOrderScheduleLine.YearOfPurchasingDocumentDate_BEDAT,
      PurchaseOrderScheduleLine.MonthOfPurchasingDocumentDate_BEDAT,
      PurchaseOrderScheduleLine.WeekOfPurchasingDocumentDate_BEDAT,
      PurchaseOrderScheduleLine.Language_SPRAS,
      POOrderHistory.AmountInLocalCurrency_DMBTR,
      POOrderHistory.CurrencyKey_WAERS AS POOrderHistoryCurrencyKey_WAERS,
      -- Actual Reciept Date
      IF(
        POOrderHistory.MovementType__inventoryManagement___BWART = '101',
        POOrderHistory.PostingDateInTheDocument_BUDAT,
        NULL
      ) AS PostingDateInTheDocument_BUDAT,

      -- DeliveryStatus
      -- TRUE stands for Delivered Orders and FALSE stands for NotDelivered Orders
      IF(
        PurchaseOrderScheduleLine.DeliveryCompletedFlag_ELIKZ IS NULL,
        FALSE,
        TRUE
      ) AS IsDelivered,

      -- Vendor Cycle Time in Days
      IF(
        PurchaseOrderScheduleLine.DeliveryCompletedFlag_ELIKZ = 'X',
        COALESCE(
          DATE_DIFF(
            IF(
              POOrderHistory.MovementType__inventoryManagement___BWART = '101',
              MAX(POOrderHistory.PostingDateInTheDocument_BUDAT) OVER (
                PARTITION BY
                  PurchaseOrderScheduleLine.Client_MANDT,
                  PurchaseOrderScheduleLine.DocumentNumber_EBELN,
                  PurchaseOrderScheduleLine.Item_EBELP
              ),
              NULL
            ),
            PurchaseOrderScheduleLine.PurchasingDocumentDate_BEDAT,
            DAY
          ),
          0
        ),
        NULL
      ) AS VendorCycleTimeInDays,

      -- Vendor Quality (Rejection)
      -- TRUE stands for Rejected Orders and FALSE stands for NotRejected Orders
      IF(
        POOrderHistory.MovementType__inventoryManagement___BWART IN ('122', '161'),
        TRUE,
        FALSE
      ) AS IsRejected,
      -- Rejected Quantity
      IF(
        POOrderHistory.MovementType__inventoryManagement___BWART IN ('122', '161'),
        POOrderHistory.Quantity_MENGE,
        0
      ) AS RejectedQuantity,

      -- Vendor On Time Delivery
      -- TRUE stands for NotDelayed Orders and FALSE for Delayed Orders
      IF(
        PurchaseOrderScheduleLine.DeliveryCompletedFlag_ELIKZ = 'X',
        IF(
          IF(
            POOrderHistory.MovementType__inventoryManagement___BWART = '101',
            POOrderHistory.PostingDateInTheDocument_BUDAT,
            NULL
          ) <= PurchaseOrderScheduleLine.ItemDeliveryDate_EINDT,
          TRUE,
          FALSE
        ),
        NULL
      ) AS IsDeliveredOnTime,

      -- Vendor InFull Delivery
      -- TRUE stands for DeliveredInFull Orders and FALSE stands for NotDeliveredInFull Orders
      IF(
        PurchaseOrderScheduleLine.DeliveryCompletedFlag_ELIKZ = 'X',
        IF(
          PurchaseOrderScheduleLine.UnderdeliveryToleranceLimit_UNTTO IS NULL AND PurchaseOrderScheduleLine.OverdeliveryToleranceLimit_UEBTO IS NULL,
          IF(
            SUM(
              IF(
                POOrderHistory.MovementType__inventoryManagement___BWART = '101',
                POOrderHistory.Quantity_MENGE,
                (POOrderHistory.Quantity_MENGE * -1)
              )) OVER (
              PARTITION BY
                PurchaseOrderScheduleLine.Client_MANDT,
                PurchaseOrderScheduleLine.DocumentNumber_EBELN,
                PurchaseOrderScheduleLine.Item_EBELP
            ) >= PurchaseOrderScheduleLine.POQuantity_MENGE,
            TRUE,
            FALSE
          ),
          IF(
            SUM(
              IF(
                POOrderHistory.MovementType__inventoryManagement___BWART = '101',
                POOrderHistory.Quantity_MENGE,
                (POOrderHistory.Quantity_MENGE * -1)
              )) OVER (
              PARTITION BY
                PurchaseOrderScheduleLine.Client_MANDT,
                PurchaseOrderScheduleLine.DocumentNumber_EBELN,
                PurchaseOrderScheduleLine.Item_EBELP
            ) >= PurchaseOrderScheduleLine.POQuantity_MENGE - PurchaseOrderScheduleLine.UnderdeliveryToleranceLimit,
            TRUE,
            FALSE
          )
          OR IF(
            SUM(
              IF(
                POOrderHistory.MovementType__inventoryManagement___BWART = '101',
                POOrderHistory.Quantity_MENGE,
                (POOrderHistory.Quantity_MENGE * -1)
              )) OVER (
              PARTITION BY
                PurchaseOrderScheduleLine.Client_MANDT,
                PurchaseOrderScheduleLine.DocumentNumber_EBELN,
                PurchaseOrderScheduleLine.Item_EBELP
            ) <= PurchaseOrderScheduleLine.POQuantity_MENGE + PurchaseOrderScheduleLine.OverdeliveryToleranceLimit,
            TRUE,
            FALSE
          )
        ),
        NULL
      ) AS IsDeliveredInFull,

      -- Vendor Invoice Accuracy
      -- TRUE stands for Accurate Invoices and FALSE stands for Inaccurate Invoices
      IF(
        PurchaseOrderScheduleLine.DeliveryCompletedFlag_ELIKZ = 'X',
        IF(
          PurchaseOrderScheduleLine.UnderdeliveryToleranceLimit_UNTTO IS NULL AND PurchaseOrderScheduleLine.OverdeliveryToleranceLimit_UEBTO IS NULL,
          IF(
            PurchaseOrderScheduleLine.POQuantity_MENGE = SUM(
              IF(
                POOrderHistory.MovementType__inventoryManagement___BWART = '101',
                POOrderHistory.Quantity_MENGE,
                (POOrderHistory.Quantity_MENGE * -1)
              )) OVER (
              PARTITION BY
                PurchaseOrderScheduleLine.Client_MANDT,
                PurchaseOrderScheduleLine.DocumentNumber_EBELN,
                PurchaseOrderScheduleLine.Item_EBELP
            ),
            TRUE,
            FALSE
          ),
          IF(
            SUM(
              IF(
                POOrderHistory.MovementType__inventoryManagement___BWART = '101',
                POOrderHistory.Quantity_MENGE,
                (POOrderHistory.Quantity_MENGE * -1)
              ))
              OVER (
                PARTITION BY
                  PurchaseOrderScheduleLine.Client_MANDT,
                  PurchaseOrderScheduleLine.DocumentNumber_EBELN,
                  PurchaseOrderScheduleLine.Item_EBELP
              )
            BETWEEN PurchaseOrderScheduleLine.POQuantity_MENGE - PurchaseOrderScheduleLine.UnderdeliveryToleranceLimit
            AND PurchaseOrderScheduleLine.POQuantity_MENGE + purchaseOrderScheduleLine.OverdeliveryToleranceLimit,
            TRUE,
            FALSE
          )
          OR IF(
            SUM(
              IF(
                POOrderHistory.MovementType__inventoryManagement___BWART = '101',
                POOrderHistory.Quantity_MENGE,
                (POOrderHistory.Quantity_MENGE * -1)
              ))
              OVER (
                PARTITION BY
                  PurchaseOrderScheduleLine.Client_MANDT,
                  PurchaseOrderScheduleLine.DocumentNumber_EBELN,
                  PurchaseOrderScheduleLine.Item_EBELP
              )
            BETWEEN PurchaseOrderScheduleLine.POQuantity_MENGE - PurchaseOrderScheduleLine.UnderdeliveryToleranceLimit
            AND PurchaseOrderScheduleLine.POQuantity_MENGE + PurchaseOrderScheduleLine.OverdeliveryToleranceLimit,
            TRUE,
            FALSE
          )
        ),
        NULL
      ) AS IsGoodsReceiptAccurate,

      -- Vendor Spend Analysis In Source Currency
      -- Goods Receipt Amount In Source Currency
      IF(
        POOrderHistory.MovementType__inventoryManagement___BWART = '101',
        POOrderHistory.AmountInLocalCurrency_DMBTR,
        (POOrderHistory.AmountInLocalCurrency_DMBTR * -1)
      ) AS GoodsReceiptAmountInSourceCurrency,

      -- Goods Receipt Quantity
      IF(
        POOrderHistory.MovementType__inventoryManagement___BWART = '101',
        POOrderHistory.Quantity_MENGE,
        (POOrderHistory.Quantity_MENGE * -1)
      ) AS GoodsReceiptQuantity
    FROM
      PurchaseOrderScheduleLine
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
          Quantity_MENGE
        FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.PurchaseDocumentsHistory`
        --## TransactioneventType_VGABE='1' -> Goods Receipt
        WHERE TransactioneventType_VGABE = '1'
          --## MovementType__inventoryManagement___BWART='101' -> Goods Receipt
          --## MovementType__inventoryManagement___BWART='102' -> Returns
          --## MovementType__inventoryManagement___BWART='122' or '161' -> Rejections
          AND MovementType__inventoryManagement___BWART IN ('101', '102', '161', '122')
      ) AS POOrderHistory
      ON
        PurchaseOrderScheduleLine.Client_MANDT = POOrderHistory.Client_MANDT
        AND PurchaseOrderScheduleLine.DocumentNumber_EBELN = POOrderHistory.PurchasingDocumentNumber_EBELN
        AND PurchaseOrderScheduleLine.Item_EBELP = POOrderHistory.ItemNumberOfPurchasingDocument_EBELP
  ),

  PurchaseDocuments AS (
    SELECT
      PurchaseOrdersGoodsReceipt.Client_MANDT,
      PurchaseOrdersGoodsReceipt.DocumentNumber_EBELN,
      PurchaseOrdersGoodsReceipt.Item_EBELP,
      PurchaseOrdersGoodsReceipt.Language_SPRAS,
      MAX(PurchaseOrdersGoodsReceipt.PurchasingDocumentDate_BEDAT) AS PurchasingDocumentDate_BEDAT,
      AVG(PurchaseOrdersGoodsReceipt.NetOrderValueinPOCurrency_NETWR) AS NetOrderValueinPOCurrency_NETWR,
      ANY_VALUE(PurchaseOrdersGoodsReceipt.CurrencyKey_WAERS) AS CurrencyKey_WAERS,
      MAX(PurchaseOrdersGoodsReceipt.ItemDeliveryDate_EINDT) AS ItemDeliveryDate_EINDT,
      MAX(PurchaseOrdersGoodsReceipt.OrderDateOfScheduleLine_BEDAT) AS OrderDateOfScheduleLine_BEDAT,
      MAX(PurchaseOrdersGoodsReceipt.PostingDateInTheDocument_BUDAT) AS PostingDateInTheDocument_BUDAT,
      SUM(PurchaseOrdersGoodsReceipt.AmountInLocalCurrency_DMBTR) AS AmountInLocalCurrency_DMBTR,
      ANY_VALUE(PurchaseOrdersGoodsReceipt.POOrderHistoryCurrencyKey_WAERS) AS POOrderHistoryCurrencyKey_WAERS,
      AVG(PurchaseOrdersGoodsReceipt.POQuantity_MENGE) AS POQuantity_MENGE,
      ANY_VALUE(PurchaseOrdersGoodsReceipt.UoM_MEINS) AS UoM_MEINS,
      AVG(PurchaseOrdersGoodsReceipt.NetPrice_NETPR) AS NetPrice_NETPR,
      MAX(PurchaseOrdersGoodsReceipt.CreatedOn_AEDAT) AS CreatedOn_AEDAT,
      ANY_VALUE(PurchaseOrdersGoodsReceipt.Status_STATU) AS Status_STATU,
      ANY_VALUE(PurchaseOrdersGoodsReceipt.MaterialNumber_MATNR) AS MaterialNumber_MATNR,
      ANY_VALUE(PurchaseOrdersGoodsReceipt.MaterialType_MTART) AS MaterialType_MTART,
      ANY_VALUE(PurchaseOrdersGoodsReceipt.MaterialGroup_MATKL) AS MaterialGroup_MATKL,
      ANY_VALUE(PurchaseOrdersGoodsReceipt.PurchasingOrganization_EKORG) AS PurchasingOrganization_EKORG,
      ANY_VALUE(PurchaseOrdersGoodsReceipt.PurchasingGroup_EKGRP) AS PurchasingGroup_EKGRP,
      ANY_VALUE(PurchaseOrdersGoodsReceipt.VendorAccountNumber_LIFNR) AS VendorAccountNumber_LIFNR,
      ANY_VALUE(PurchaseOrdersGoodsReceipt.Company_BUKRS) AS Company_BUKRS,
      ANY_VALUE(PurchaseOrdersGoodsReceipt.Plant_WERKS) AS Plant_WERKS,
      LOGICAL_AND(PurchaseOrdersGoodsReceipt.IsDelivered) AS IsDelivered,
      MAX(PurchaseOrdersGoodsReceipt.VendorCycleTimeInDays) AS VendorCycleTimeInDays,
      LOGICAL_OR(PurchaseOrdersGoodsReceipt.IsRejected) AS IsRejected,
      SUM(PurchaseOrdersGoodsReceipt.RejectedQuantity) AS RejectedQuantity,
      LOGICAL_AND(PurchaseOrdersGoodsReceipt.IsDeliveredOnTime) AS IsDeliveredOnTime,
      LOGICAL_AND(PurchaseOrdersGoodsReceipt.IsDeliveredInFull) AS IsDeliveredInFull,
      LOGICAL_AND(PurchaseOrdersGoodsReceipt.IsGoodsReceiptAccurate) AS IsGoodsReceiptAccurate,
      SUM(PurchaseOrdersGoodsReceipt.GoodsReceiptQuantity) AS GoodsReceiptQuantity,
      SUM(PurchaseOrdersGoodsReceipt.GoodsReceiptAmountInSourceCurrency) AS GoodsReceiptAmountInSourceCurrency,
      MAX(PurchaseOrdersGoodsReceipt.YearOfPurchasingDocumentDate_BEDAT) AS YearOfPurchasingDocumentDate_BEDAT,
      MAX(PurchaseOrdersGoodsReceipt.MonthOfPurchasingDocumentDate_BEDAT) AS MonthOfPurchasingDocumentDate_BEDAT,
      MAX(PurchaseOrdersGoodsReceipt.WeekOfPurchasingDocumentDate_BEDAT) AS WeekOfPurchasingDocumentDate_BEDAT
    FROM PurchaseOrdersGoodsReceipt
    GROUP BY
      PurchaseOrdersGoodsReceipt.Client_MANDT,
      PurchaseOrdersGoodsReceipt.DocumentNumber_EBELN,
      PurchaseOrdersGoodsReceipt.Item_EBELP,
      PurchaseOrdersGoodsReceipt.Language_SPRAS
  ),
  Vendors AS (
    SELECT *
    FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.VendorsMD`
    WHERE
      ValidToDate_DATE_TO = '9999-12-31'
      -- ## CORTEX-CUSTOMER Modify the filter according to the configuration, for example, if NATION
      -- has value 'I' for international, it could be used in the filter
      AND COALESCE(VersionIdForInternationalAddresses_NATION, '') = ''
  )

SELECT
  PurchaseDocuments.Client_MANDT,
  PurchaseDocuments.DocumentNumber_EBELN,
  PurchaseDocuments.Item_EBELP,
  PurchaseDocuments.PurchasingDocumentDate_BEDAT,
  PurchaseDocuments.NetOrderValueinPOCurrency_NETWR,
  PurchaseDocuments.CurrencyKey_WAERS,
  PurchaseDocuments.ItemDeliveryDate_EINDT,
  PurchaseDocuments.OrderDateOfScheduleLine_BEDAT,
  PurchaseDocuments.PostingDateInTheDocument_BUDAT,
  PurchaseDocuments.AmountInLocalCurrency_DMBTR,
  PurchaseDocuments.POOrderHistoryCurrencyKey_WAERS,
  PurchaseDocuments.POQuantity_MENGE,
  PurchaseDocuments.UoM_MEINS,
  PurchaseDocuments.NetPrice_NETPR,
  PurchaseDocuments.CreatedOn_AEDAT,
  PurchaseDocuments.Status_STATU,
  PurchaseDocuments.MaterialNumber_MATNR,
  PurchaseDocuments.MaterialType_MTART,
  PurchaseDocuments.MaterialGroup_MATKL,
  PurchaseDocuments.PurchasingOrganization_EKORG,
  PurchaseDocuments.PurchasingGroup_EKGRP,
  PurchaseDocuments.VendorAccountNumber_LIFNR,
  PurchaseDocuments.Company_BUKRS,
  PurchaseDocuments.Plant_WERKS,
  PurchaseDocuments.YearOfPurchasingDocumentDate_BEDAT,
  PurchaseDocuments.MonthOfPurchasingDocumentDate_BEDAT,
  PurchaseDocuments.WeekOfPurchasingDocumentDate_BEDAT,
  FiscalDateDimension_BEDAT.FiscalYear,
  FiscalDateDimension_BEDAT.FiscalPeriod,
  -- Invoice Quantity
  PurchaseOrdersInvoiceReceipt.InvoiceQuantity,
  -- Vendor Spend Analysis (Invoice Amount in Source Currency)
  PurchaseOrdersInvoiceReceipt.InvoiceAmountInSourceCurrency,
  -- Invoice Date
  PurchaseOrdersInvoiceReceipt.InvoiceDate,
  PurchaseOrdersInvoiceReceipt.YearOfInvoiceDate,
  PurchaseOrdersInvoiceReceipt.MonthOfInvoiceDate,
  PurchaseOrdersInvoiceReceipt.WeekOfInvoiceDate,
  -- Invoice Count
  PurchaseOrdersInvoiceReceipt.InvoiceCount,
  -- The following text fields are language independent.
  PurchasingOrganizations.PurchasingOrganizationText_EKOTX,
  PurchasingGroups.PurchasingGroupText_EKNAM,
  Vendors.CountryKey_LAND1,
  Vendors.NAME1,
  Companies.CompanyText_BUTXT,
  Companies.FiscalyearVariant_PERIV,
  -- The following text fields are language dependent.
  LanguageKey.LanguageKey_SPRAS,
  Materials.MaterialText_MAKTX,
  MaterialTypes.DescriptionOfMaterialType_MTBEZ,
  -- VendorCycleTime In Days
  PurchaseDocuments.VendorCycleTimeInDays,
  -- Rejected Quantity
  PurchaseDocuments.RejectedQuantity,
  -- Goods Receipt Quantity
  PurchaseDocuments.GoodsReceiptQuantity,
  -- Vendor Spend Analysis (Goods Receipt Amount in Source Currency)
  PurchaseDocuments.GoodsReceiptAmountInSourceCurrency,
  -- The following columns are having amount/prices in target currency.
  CurrencyConversion.ExchangeRate_UKURS,
  CurrencyConversion.ToCurrency_TCURR AS TargetCurrency_TCURR,
  PurchaseDocuments.AmountInLocalCurrency_DMBTR * CurrencyConversion.ExchangeRate_UKURS AS AmountInTargetCurrency_DMBTR,
  PurchaseDocuments.NetPrice_NETPR * CurrencyConversion.ExchangeRate_UKURS AS NetPriceInTargetCurrency_NETPR,
  PurchaseDocuments.NetOrderValueinPOCurrency_NETWR * CurrencyConversion.ExchangeRate_UKURS AS NetOrderValueinTargetCurrency_NETWR,
  PurchaseDocuments.GoodsReceiptAmountInSourceCurrency * CurrencyConversion.ExchangeRate_UKURS AS GoodsReceiptAmountInTargetCurrency,
  PurchaseOrdersInvoiceReceipt.InvoiceAmountInSourceCurrency * CurrencyConversion.ExchangeRate_UKURS AS InvoiceAmountInTargetCurrency,
  -- DeliveryStatus
  IF(
    PurchaseDocuments.IsDelivered, TRUE, FALSE
  ) AS IsDelivered,
  -- Vendor Quality (Rejection)
  IF(
    PurchaseDocuments.IsRejected, TRUE, FALSE
  ) AS IsRejected,
  -- Vendor On Time Delivery
  IF(
    PurchaseDocuments.IsDeliveredOnTime IS NULL,
    'NotApplicable',
    IF(
      PurchaseDocuments.IsDeliveredOnTime,
      'NotDelayed',
      'Delayed'
    )
  ) AS VendorOnTimeDelivery,
  -- Vendor InFull Delivery
  IF(
    PurchaseDocuments.IsDeliveredInFull IS NULL,
    'NotApplicable',
    IF(
      PurchaseDocuments.IsDeliveredInFull,
      'DeliveredInFull',
      'NotDeliveredInFull'
    )
  ) AS VendorInFullDelivery,
  -- Vendor On Time In Full Delivery
  IF(
    PurchaseDocuments.IsDeliveredInFull IS NULL OR PurchaseDocuments.IsDeliveredOnTime IS NULL,
    'NotApplicable',
    IF(
      PurchaseDocuments.IsDeliveredInFull AND PurchaseDocuments.IsDeliveredOnTime,
      'OTIF',
      'NotOTIF'
    )
  ) AS VendorOnTimeInFullDelivery,
  -- Vendor Invoice Accuracy
  IF(
    PurchaseDocuments.IsGoodsReceiptAccurate IS NULL OR PurchaseOrdersInvoiceReceipt.InvoiceQuantity IS NULL,
    'NotApplicable',
    IF(
      PurchaseDocuments.IsGoodsReceiptAccurate
      AND PurchaseDocuments.POQuantity_MENGE = PurchaseOrdersInvoiceReceipt.InvoiceQuantity,
      'AccurateInvoice',
      'InaccurateInvoice'
    )
  ) AS VendorInvoiceAccuracy,
  -- Past Due and Open
  IF(
    PurchaseDocuments.IsDelivered,
    'NotApplicable',
    IF(
      CURRENT_DATE() > PurchaseDocuments.ItemDeliveryDate_EINDT,
      'PastDue',
      'Open'
    )
  ) AS PastDueOrOpenItems
FROM PurchaseDocuments
LEFT JOIN
  ( --noqa: disable=ST05
    SELECT
      Client_MANDT,
      PurchasingDocumentNumber_EBELN,
      ItemNumberOfPurchasingDocument_EBELP,
      SUM(Quantity_MENGE) AS InvoiceQuantity,
      SUM(AmountInLocalCurrency_DMBTR) AS InvoiceAmountInSourceCurrency,
      -- Invoice Date
      MAX(PostingDateInTheDocument_BUDAT) AS InvoiceDate,
      MAX(YearOfPostingDateInTheDocument_BUDAT) AS YearOfInvoiceDate,
      MAX(MonthOfPostingDateInTheDocument_BUDAT) AS MonthOfInvoiceDate,
      MAX(WeekOfPostingDateInTheDocument_BUDAT) AS WeekOfInvoiceDate,
      -- Invoice Count
      COUNT(PurchasingDocumentNumber_EBELN) AS InvoiceCount
    FROM
      `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.PurchaseDocumentsHistory`
    --## TransactioneventType_VGABE='2' -> Invoice Receipt
    WHERE TransactioneventType_VGABE = '2'
    GROUP BY Client_MANDT, PurchasingDocumentNumber_EBELN, ItemNumberOfPurchasingDocument_EBELP
  ) AS PurchaseOrdersInvoiceReceipt --noqa: enable = all
  ON
    PurchaseDocuments.Client_MANDT = PurchaseOrdersInvoiceReceipt.Client_MANDT
    AND PurchaseDocuments.DocumentNumber_EBELN = PurchaseOrdersInvoiceReceipt.PurchasingDocumentNumber_EBELN
    AND PurchaseDocuments.Item_EBELP = PurchaseOrdersInvoiceReceipt.ItemNumberOfPurchasingDocument_EBELP
LEFT JOIN CurrencyConversion
  ON
    PurchaseDocuments.Client_MANDT = CurrencyConversion.Client_MANDT
    AND PurchaseDocuments.CurrencyKey_WAERS = CurrencyConversion.FromCurrency_FCURR
    AND PurchaseDocuments.PurchasingDocumentDate_BEDAT = CurrencyConversion.ConvDate

LEFT JOIN
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.PurchasingOrganizationsMD` AS PurchasingOrganizations
  ON
    PurchaseDocuments.Client_MANDT = PurchasingOrganizations.Client_MANDT
    AND PurchaseDocuments.PurchasingOrganization_EKORG = PurchasingOrganizations.PurchasingOrganization_EKORG
LEFT JOIN
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.PurchasingGroupsMD` AS PurchasingGroups
  ON
    PurchaseDocuments.Client_MANDT = PurchasingGroups.Client_MANDT
    AND PurchaseDocuments.PurchasingGroup_EKGRP = PurchasingGroups.PurchasingGroup_EKGRP
LEFT JOIN Vendors
  ON
    PurchaseDocuments.Client_MANDT = Vendors.Client_MANDT
    AND PurchaseDocuments.VendorAccountNumber_LIFNR = Vendors.AccountNumberOfVendorOrCreditor_LIFNR
    AND PurchaseDocuments.Language_SPRAS = Vendors.Language_LANGU
LEFT JOIN
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.CompaniesMD` AS Companies
  ON
    PurchaseDocuments.Client_MANDT = Companies.Client_MANDT
    AND PurchaseDocuments.Company_BUKRS = Companies.CompanyCode_BUKRS
LEFT JOIN
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.fiscal_date_dim` AS FiscalDateDimension_BEDAT
  ON
    PurchaseDocuments.Client_MANDT = FiscalDateDimension_BEDAT.MANDT
    AND Companies.FiscalyearVariant_PERIV = FiscalDateDimension_BEDAT.PERIV
    AND PurchaseDocuments.PurchasingDocumentDate_BEDAT = FiscalDateDimension_BEDAT.DATE
CROSS JOIN LanguageKey
LEFT JOIN
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.MaterialsMD` AS Materials
  ON
    PurchaseDocuments.Client_MANDT = Materials.Client_MANDT
    AND PurchaseDocuments.MaterialNumber_MATNR = Materials.MaterialNumber_MATNR
    AND Materials.Language_SPRAS = LanguageKey.LanguageKey_SPRAS
LEFT JOIN
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.MaterialTypesMD` AS MaterialTypes
  ON
    PurchaseDocuments.Client_MANDT = MaterialTypes.Client_MANDT
    AND PurchaseDocuments.MaterialType_MTART = MaterialTypes.MaterialType_MTART
    AND MaterialTypes.LanguageKey_SPRAS = LanguageKey.LanguageKey_SPRAS
