--This query groups the data by  Client_MANDT,MaterialNumber_MATNR,BatchNumber_CHARG,Plant_WERKS,
--StorageLocation_LGORT,CompanyCode_BUKRS,CalYear,CalMonth,StockCharacteristic,LanguageKey_SPRAS
WITH
  LanguageKey AS (
    SELECT LanguageKey_SPRAS
    FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.Languages_T002`
    WHERE LanguageKey_SPRAS IN UNNEST({{ sap_languages }})
  ),

  StockMonthlySnapshots AS (
    SELECT
      StockMonthlySnapshots.MANDT AS Client_MANDT,
      StockMonthlySnapshots.MATNR AS MaterialNumber_MATNR,
      MaterialsMD.MaterialText_MAKTX,
      StockMonthlySnapshots.WERKS AS Plant_WERKS,
      PlantsMD.Name2_NAME2 AS Plant_Name2_NAME2,
      StockMonthlySnapshots.LGORT AS StorageLocation_LGORT,
      StorageLocationsMD.StorageLocationText_LGOBE,
      StockMonthlySnapshots.CHARG AS BatchNumber_CHARG,
      StockMonthlySnapshots.BWART AS MovementType_BWART,
      StockMonthlySnapshots.SHKZG AS Debit_CreditIndicator_SHKZG,
      StockMonthlySnapshots.INSMK AS StockType_INSMK,
      StockMonthlySnapshots.SOBKZ AS SpecialStockIndicator_SOBKZ,
      StockMonthlySnapshots.BUKRS AS CompanyCode_BUKRS,
      MaterialsMD.MaterialType_MTART,
      MaterialTypesMD.DescriptionOfMaterialType_MTBEZ,
      MaterialsMD.MaterialGroup_MATKL,
      MaterialGroupsMD.MaterialGroupName_WGBEZ,
      CompaniesMD.CompanyText_BUTXT,
      PlantsMD.CountryKey_LAND1,
      StockCharacteristicsConfig.StockCharacteristic,
      FiscalDateDimension_MONTHENDDATE.FiscalYear,
      FiscalDateDimension_MONTHENDDATE.FiscalPeriod,
      StockMonthlySnapshots.cal_year AS CalYear,
      StockMonthlySnapshots.cal_month AS CalMonth,
      StockMonthlySnapshots.Month_End_Date AS MonthEndDate,
      StockMonthlySnapshots.quantity_monthly_cumulative AS QuantityMonthlyCumulative,
      StockMonthlySnapshots.MEINS AS BaseUnitOfMeasure_MEINS,
      COALESCE(
        StockMonthlySnapshots.amount_monthly_cumulative * currency_decimal.CURRFIX,
        StockMonthlySnapshots.amount_monthly_cumulative
      ) AS AmountMonthlyCumulative,
      StockMonthlySnapshots.WAERS AS CurrencyKey_WAERS,
      LanguageKey.LanguageKey_SPRAS,
      --Quantity Issued To Delivery
      --601 - Goods issued : Delivery
      --602 - Goods issued : Reversal
      IF(
        StockMonthlySnapshots.BWART IN ('601', '602'),
        (StockMonthlySnapshots.total_monthly_movement_quantity * -1),
        0
      ) AS QuantityIssuedToDelivery,

      -- Stock On Hand
      IF(
        StockCharacteristicsConfig.StockCharacteristic = 'Unrestricted',
        StockMonthlySnapshots.quantity_monthly_cumulative,
        0
      ) AS StockOnHand,

      -- Stock On Hand Value
      IF(
        StockCharacteristicsConfig.StockCharacteristic = 'Unrestricted',
        COALESCE(
          StockMonthlySnapshots.amount_monthly_cumulative * currency_decimal.CURRFIX,
          StockMonthlySnapshots.amount_monthly_cumulative
        ),
        0
      ) AS StockOnHandValue,

      -- Total Consumption Quantity
      --261- Goods issued for order
      --262- Goods issued Reversal
      IF(
        MaterialsMD.MaterialType_MTART IN ('FERT', 'HALB') AND StockMonthlySnapshots.BWART IN ('601', '602'),
        (StockMonthlySnapshots.total_monthly_movement_quantity * -1),
        IF(
          MaterialsMD.MaterialType_MTART IN ('ROH', 'HIBE') AND StockMonthlySnapshots.BWART IN ('261', '262'),
          (StockMonthlySnapshots.total_monthly_movement_quantity * -1),
          0
        )
      ) AS TotalConsumptionQuantity
    FROM
      `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.stock_monthly_snapshots` AS StockMonthlySnapshots
    LEFT JOIN
      `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.StockCharacteristicsConfig` AS StockCharacteristicsConfig
      ON
        StockMonthlySnapshots.MANDT = StockCharacteristicsConfig.Client_MANDT
        AND StockMonthlySnapshots.BWART = StockCharacteristicsConfig.MovementType_BWART
        AND StockMonthlySnapshots.SHKZG = StockCharacteristicsConfig.Debit_CreditIndicator_SHKZG
        AND StockMonthlySnapshots.SOBKZ = StockCharacteristicsConfig.SpecialStockIndicator_SOBKZ
        AND StockMonthlySnapshots.INSMK = StockCharacteristicsConfig.StockType_INSMK
    LEFT JOIN
      `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.currency_decimal` AS currency_decimal
      ON
        StockMonthlySnapshots.WAERS = currency_decimal.CURRKEY
    LEFT JOIN
      `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.StorageLocationsMD` AS StorageLocationsMD
      ON
        StockMonthlySnapshots.MANDT = StorageLocationsMD.Client_MANDT
        AND StockMonthlySnapshots.LGORT = StorageLocationsMD.StorageLocation_LGORT
        AND StockMonthlySnapshots.WERKS = StorageLocationsMD.Plant_WERKS
    LEFT JOIN
      `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.CompaniesMD` AS CompaniesMD
      ON
        StockMonthlySnapshots.MANDT = CompaniesMD.Client_MANDT
        AND StockMonthlySnapshots.BUKRS = CompaniesMD.CompanyCode_BUKRS
    LEFT JOIN
      `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.fiscal_date_dim` AS FiscalDateDimension_MONTHENDDATE
      ON
        StockMonthlySnapshots.MANDT = FiscalDateDimension_MONTHENDDATE.MANDT
        AND CompaniesMD.FiscalyearVariant_PERIV = FiscalDateDimension_MONTHENDDATE.PERIV
        AND StockMonthlySnapshots.MONTH_END_DATE = FiscalDateDimension_MONTHENDDATE.DATE
    LEFT JOIN
      `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.PlantsMD` AS PlantsMD
      ON
        StockMonthlySnapshots.MANDT = PlantsMD.Client_MANDT
        AND StockMonthlySnapshots.WERKS = PlantsMD.Plant_WERKS
    CROSS JOIN
      LanguageKey
    LEFT JOIN
      `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.MaterialsMD` AS MaterialsMD
      ON
        StockMonthlySnapshots.MANDT = MaterialsMD.Client_MANDT
        AND StockMonthlySnapshots.MATNR = MaterialsMD.MaterialNumber_MATNR
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
    WHERE StockCharacteristicsConfig.StockCharacteristic != 'BlockedReturns'
  )

SELECT
  StockMonthlySnapshots.Client_MANDT,
  StockMonthlySnapshots.MaterialNumber_MATNR,
  StockMonthlySnapshots.MaterialType_MTART,
  StockMonthlySnapshots.DescriptionOfMaterialType_MTBEZ,
  StockMonthlySnapshots.MaterialText_MAKTX,
  StockMonthlySnapshots.MaterialGroup_MATKL,
  StockMonthlySnapshots.MaterialGroupName_WGBEZ,
  StockMonthlySnapshots.Plant_WERKS,
  StockMonthlySnapshots.Plant_Name2_NAME2,
  StockMonthlySnapshots.StorageLocation_LGORT,
  StockMonthlySnapshots.StorageLocationText_LGOBE,
  StockMonthlySnapshots.BatchNumber_CHARG,
  StockMonthlySnapshots.CompanyCode_BUKRS,
  StockMonthlySnapshots.CompanyText_BUTXT,
  StockMonthlySnapshots.CountryKey_LAND1,
  StockMonthlySnapshots.LanguageKey_SPRAS,
  StockMonthlySnapshots.BaseUnitOfMeasure_MEINS,
  StockMonthlySnapshots.StockCharacteristic,
  StockMonthlySnapshots.CurrencyKey_WAERS,
  StockMonthlySnapshots.FiscalYear,
  StockMonthlySnapshots.FiscalPeriod,
  StockMonthlySnapshots.CalYear,
  StockMonthlySnapshots.CalMonth,
  StockMonthlySnapshots.MonthEndDate,
  SUM(StockMonthlySnapshots.QuantityMonthlyCumulative) AS QuantityMonthlyCumulative,
  SUM(StockMonthlySnapshots.AmountMonthlyCumulative) AS AmountMonthlyCumulative,
  SUM(StockMonthlySnapshots.StockOnHand) AS StockOnHand,
  SUM(StockMonthlySnapshots.StockOnHandValue) AS StockOnHandValue,
  SUM(StockMonthlySnapshots.QuantityIssuedToDelivery) AS QuantityIssuedToDelivery,
  SUM(StockMonthlySnapshots.TotalConsumptionQuantity) AS TotalConsumptionQuantity
FROM
  StockMonthlySnapshots
GROUP BY
  StockMonthlySnapshots.Client_MANDT,
  StockMonthlySnapshots.MaterialNumber_MATNR,
  StockMonthlySnapshots.MaterialType_MTART,
  StockMonthlySnapshots.DescriptionOfMaterialType_MTBEZ,
  StockMonthlySnapshots.MaterialText_MAKTX,
  StockMonthlySnapshots.MaterialGroup_MATKL,
  StockMonthlySnapshots.MaterialGroupName_WGBEZ,
  StockMonthlySnapshots.Plant_WERKS,
  StockMonthlySnapshots.Plant_Name2_NAME2,
  StockMonthlySnapshots.StorageLocation_LGORT,
  StockMonthlySnapshots.StorageLocationText_LGOBE,
  StockMonthlySnapshots.LanguageKey_SPRAS,
  StockMonthlySnapshots.BatchNumber_CHARG,
  StockMonthlySnapshots.CompanyCode_BUKRS,
  StockMonthlySnapshots.CompanyText_BUTXT,
  StockMonthlySnapshots.CountryKey_LAND1,
  StockMonthlySnapshots.StockCharacteristic,
  StockMonthlySnapshots.FiscalYear,
  StockMonthlySnapshots.FiscalPeriod,
  StockMonthlySnapshots.CalYear,
  StockMonthlySnapshots.CalMonth,
  StockMonthlySnapshots.MonthEndDate,
  StockMonthlySnapshots.BaseUnitOfMeasure_MEINS,
  StockMonthlySnapshots.CurrencyKey_WAERS
