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
  -- Computing some metrics separately to avoid consuming too many resources in the next step
  MaterialCostAndPrice AS (
    SELECT DISTINCT
      StockWeeklySnapshots.MaterialNumber_MATNR,
      StockWeeklySnapshots.Plant_WERKS,
      StockWeeklySnapshots.WeekEndDate,
      StockWeeklySnapshots.FiscalYear,
      StockWeeklySnapshots.FiscalPeriod,
      -- If StandardCost is null for current week then it picks up the last existing StandardCost
      COALESCE(
        MaterialLedger.StandardCost_STPRS,
        LAST_VALUE(MaterialLedger.StandardCost_STPRS IGNORE NULLS)
          OVER ( --noqa: disable=L003
            PARTITION BY
              StockWeeklySnapshots.MaterialNumber_MATNR, StockWeeklySnapshots.Plant_WERKS
            ORDER BY
              StockWeeklySnapshots.WeekEndDate
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
          )) AS StandardCost_STPRS,
      -- If MovingAveragePrice is null for current week then it picks up the last
      -- existing MovingAveragePrice
      COALESCE(
        MaterialLedger.MovingAveragePrice,
        LAST_VALUE(MaterialLedger.MovingAveragePrice IGNORE NULLS)
          OVER (
            PARTITION BY
              StockWeeklySnapshots.MaterialNumber_MATNR, StockWeeklySnapshots.Plant_WERKS
            ORDER BY
              StockWeeklySnapshots.WeekEndDate
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
          )) AS MovingAveragePrice_VERPR
    FROM --noqa: enable=all
      `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.StockWeeklySnapshots` AS StockWeeklySnapshots
    LEFT JOIN
      `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.MaterialLedger` AS MaterialLedger
      ON
        StockWeeklySnapshots.Client_MANDT = MaterialLedger.Client_MANDT
        AND StockWeeklySnapshots.MaterialNumber_MATNR = MaterialLedger.MaterialNumber_MATNR
        AND StockWeeklySnapshots.Plant_WERKS = MaterialLedger.ValuationArea_BWKEY
        AND StockWeeklySnapshots.FiscalYear = MaterialLedger.FiscalYear
        AND StockWeeklySnapshots.FiscalPeriod = MaterialLedger.PostingPeriod
    WHERE
      MaterialLedger.ValuationType_BWTAR = ''
  ),
  CurrentStock AS (
    SELECT
      StockWeeklySnapshots.Client_MANDT,
      StockWeeklySnapshots.MaterialNumber_MATNR,
      StockWeeklySnapshots.BatchNumber_CHARG,
      StockWeeklySnapshots.Plant_WERKS,
      StockWeeklySnapshots.StorageLocation_LGORT,
      StorageLocationsMD.StorageLocationText_LGOBE,
      StockWeeklySnapshots.CompanyCode_BUKRS,
      StockWeeklySnapshots.CompanyText_BUTXT,
      StockWeeklySnapshots.BaseUnitOfMeasure_MEINS,
      StockWeeklySnapshots.CurrencyKey_WAERS,
      StockWeeklySnapshots.CalYear,
      StockWeeklySnapshots.CalWeek,
      StockWeeklySnapshots.FiscalYear,
      StockWeeklySnapshots.FiscalPeriod,
      StockWeeklySnapshots.StockCharacteristic,
      MaterialsBatchMD.DateOfManufacture_HSDAT,
      MaterialPlantsMD.SafetyStock_EISBE,
      PlantsMD.Name2_NAME2 AS PlantName_NAME2,
      PlantsMD.CountryKey_LAND1,
      PlantsMD.DivisionForIntercompanyBilling_SPART,
      PlantsMD.ValuationArea_BWKEY,
      StockWeeklySnapshots.QuantityWeeklyCumulative,
      StockWeeklySnapshots.AmountWeeklyCumulative,
      -- If weekend date is in future then it updates with current_date
      IF(
        StockWeeklySnapshots.WeekEndDate = LAST_DAY(CURRENT_DATE, WEEK),
        CURRENT_DATE,
        StockWeeklySnapshots.WeekEndDate
      ) AS WeekEndDate,
      MaterialCostAndPrice.StandardCost_STPRS,
      MaterialCostAndPrice.MovingAveragePrice_VERPR
    FROM
      -- TODO: Evaluate if all columns in StockWeeklySnapshots can be moved into
      -- MaterialCostAndPrice so we don't need to query the source table twice.
      `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.StockWeeklySnapshots` AS StockWeeklySnapshots
    LEFT JOIN
      `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.MaterialsBatchMD` AS MaterialsBatchMD
      ON
        StockWeeklySnapshots.Client_MANDT = MaterialsBatchMD.Client_MANDT
        AND StockWeeklySnapshots.MaterialNumber_MATNR = MaterialsBatchMD.MaterialNumber_MATNR
        AND StockWeeklySnapshots.BatchNumber_CHARG = MaterialsBatchMD.BatchNumber_CHARG
    LEFT JOIN
      `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.MaterialPlantsMD` AS MaterialPlantsMD
      ON
        StockWeeklySnapshots.Client_MANDT = MaterialPlantsMD.Client_MANDT
        AND StockWeeklySnapshots.MaterialNumber_MATNR = MaterialPlantsMD.MaterialNumber_MATNR
        AND StockWeeklySnapshots.Plant_WERKS = MaterialPlantsMD.Plant_WERKS
    LEFT JOIN
      `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.PlantsMD` AS PlantsMD
      ON
        StockWeeklySnapshots.Client_MANDT = PlantsMD.Client_MANDT
        AND StockWeeklySnapshots.Plant_WERKS = PlantsMD.Plant_WERKS
    LEFT JOIN
      MaterialCostAndPrice
      ON
        StockWeeklySnapshots.MaterialNumber_MATNR = MaterialCostAndPrice.MaterialNumber_MATNR
        AND StockWeeklySnapshots.Plant_WERKS = MaterialCostAndPrice.Plant_WERKS
        AND StockWeeklySnapshots.WeekEndDate = MaterialCostAndPrice.WeekEndDate
        AND StockWeeklySnapshots.FiscalYear = MaterialCostAndPrice.FiscalYear
        AND StockWeeklySnapshots.FiscalPeriod = MaterialCostAndPrice.FiscalPeriod
    LEFT JOIN
      `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.StorageLocationsMD` AS StorageLocationsMD
      ON
        StockWeeklySnapshots.Client_MANDT = StorageLocationsMD.Client_MANDT
        AND StockWeeklySnapshots.StorageLocation_LGORT = StorageLocationsMD.StorageLocation_LGORT
        AND StockWeeklySnapshots.Plant_WERKS = StorageLocationsMD.Plant_WERKS
  )

SELECT
  CurrentStock.Client_MANDT,
  CurrentStock.MaterialNumber_MATNR,
  CurrentStock.BatchNumber_CHARG,
  CurrentStock.Plant_WERKS,
  CurrentStock.StorageLocation_LGORT,
  CurrentStock.StorageLocationText_LGOBE,
  CurrentStock.CompanyCode_BUKRS,
  CurrentStock.CompanyText_BUTXT,
  CurrentStock.BaseUnitOfMeasure_MEINS,
  CurrentStock.CurrencyKey_WAERS,
  CurrentStock.DateOfManufacture_HSDAT,
  MaterialsMD.MaterialText_MAKTX,
  LanguageKey.LanguageKey_SPRAS,
  MaterialsMD.TotalShelfLife_MHDHB,
  MaterialsMD.MaterialType_MTART,
  MaterialTypesMD.DescriptionOfMaterialType_MTBEZ,
  CurrentStock.StandardCost_STPRS,
  CurrentStock.MovingAveragePrice_VERPR,
  MaterialsMD.MaterialGroup_MATKL,
  MaterialGroupsMD.MaterialGroupName_WGBEZ,
  CurrentStock.SafetyStock_EISBE,
  CurrentStock.PlantName_NAME2,
  CurrentStock.CountryKey_LAND1,
  CurrentStock.DivisionForIntercompanyBilling_SPART,
  CurrentStock.ValuationArea_BWKEY,
  CurrentStock.CalYear,
  CurrentStock.CalWeek,
  CurrentStock.WeekEndDate,
  CurrentStock.FiscalYear,
  CurrentStock.FiscalPeriod,
  CurrentStock.QuantityWeeklyCumulative,
  CurrentStock.AmountWeeklyCumulative,
  CurrentStock.StockCharacteristic,
  -- The following columns are having amount/prices in target currency.
  CurrencyConversion.ExchangeRate_UKURS,
  CurrencyConversion.ToCurrency_TCURR AS TargetCurrency_TCURR,
  CurrentStock.AmountWeeklyCumulative * CurrencyConversion.ExchangeRate_UKURS AS AmountWeeklyCumulativeInTargetCurrency,
  CurrentStock.StandardCost_STPRS * CurrencyConversion.ExchangeRate_UKURS AS StandardCostInTargetCurrency_STPRS,
  CurrentStock.MovingAveragePrice_VERPR * CurrencyConversion.ExchangeRate_UKURS AS MovingAveragePriceInTargetCurrency_VERPR,

  -- Inventory Value In Target Currency
  COALESCE(
    IF(
      MaterialsMD.MaterialType_MTART IN ('FERT', 'HALB'),
      CurrentStock.QuantityWeeklyCumulative * (CurrentStock.StandardCost_STPRS * CurrencyConversion.ExchangeRate_UKURS),
      IF(
        MaterialsMD.MaterialType_MTART IN ('ROH', 'HIBE'),
        CurrentStock.QuantityWeeklyCumulative * (CurrentStock.MovingAveragePrice_VERPR * CurrencyConversion.ExchangeRate_UKURS),
        0
      )
    ), 0
  ) AS InventoryValueInTargetCurrency,

  -- Obsolete Inventory Value In Target Currency
  IF(
    SAFE.DATE_ADD(
      CurrentStock.DateOfManufacture_HSDAT,
      INTERVAL CAST(MaterialsMD.TotalShelfLife_MHDHB AS INT64) DAY
    ) < CURRENT_DATE,
    (CurrentStock.AmountWeeklyCumulative * CurrencyConversion.ExchangeRate_UKURS),
    0
  ) AS ObsoleteInventoryValueInTargetCurrency,

  -- Inventory Value In Source Currency
  COALESCE(
    IF(
      MaterialsMD.MaterialType_MTART IN ('FERT', 'HALB'),
      CurrentStock.QuantityWeeklyCumulative * CurrentStock.StandardCost_STPRS,
      IF(
        MaterialsMD.MaterialType_MTART IN ('ROH', 'HIBE'),
        CurrentStock.QuantityWeeklyCumulative * CurrentStock.MovingAveragePrice_VERPR,
        0
      )
    ), 0
  ) AS InventoryValueInSourceCurrency,

  -- ObsoleteStock
  IF(
    SAFE.DATE_ADD(
      CurrentStock.DateOfManufacture_HSDAT,
      INTERVAL CAST(MaterialsMD.TotalShelfLife_MHDHB AS INT64) DAY
    ) < CURRENT_DATE,
    CurrentStock.QuantityWeeklyCumulative,
    0
  ) AS ObsoleteStock,

  -- Obsolete Inventory Value In Source Currency
  IF(
    SAFE.DATE_ADD(
      CurrentStock.DateOfManufacture_HSDAT,
      INTERVAL CAST(MaterialsMD.TotalShelfLife_MHDHB AS INT64) DAY
    ) < CURRENT_DATE,
    CurrentStock.AmountWeeklyCumulative,
    0
  ) AS ObsoleteInventoryValueInSourceCurrency

FROM
  CurrentStock
LEFT JOIN
  CurrencyConversion
  ON
    CurrentStock.Client_MANDT = CurrencyConversion.Client_MANDT
    AND CurrentStock.CurrencyKey_WAERS = CurrencyConversion.FromCurrency_FCURR
    AND CurrentStock.WeekEndDate = CurrencyConversion.ConvDate
CROSS JOIN
  LanguageKey
LEFT JOIN
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.MaterialsMD` AS MaterialsMD
  ON
    CurrentStock.Client_MANDT = MaterialsMD.Client_MANDT
    AND CurrentStock.MaterialNumber_MATNR = MaterialsMD.MaterialNumber_MATNR
    AND MaterialsMD.Language_SPRAS = LanguageKey.LanguageKey_SPRAS
LEFT JOIN
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.MaterialTypesMD` AS MaterialTypesMD
  ON
    MaterialsMD.Client_MANDT = MaterialTypesMD.Client_MANDT
    AND MaterialsMD.MaterialType_MTART = MaterialTypesMD.MaterialType_MTART
    AND MaterialTypesMD.LanguageKey_SPRAS = LanguageKey.LanguageKey_SPRAS
LEFT JOIN
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.MaterialGroupsMD` AS MaterialGroupsMD
  ON
    MaterialsMD.Client_MANDT = MaterialGroupsMD.Client_MANDT
    AND MaterialsMD.MaterialGroup_MATKL = MaterialGroupsMD.MaterialGroup_MATKL
    AND MaterialGroupsMD.Language_SPRAS = LanguageKey.LanguageKey_SPRAS
