SELECT
  StockWeeklySnapshots.MANDT AS Client_MANDT,
  StockWeeklySnapshots.MATNR AS MaterialNumber_MATNR,
  StockWeeklySnapshots.WERKS AS Plant_WERKS,
  StockWeeklySnapshots.CHARG AS BatchNumber_CHARG,
  StockWeeklySnapshots.LGORT AS StorageLocation_LGORT,
  StockWeeklySnapshots.BUKRS AS CompanyCode_BUKRS,
  CompaniesMD.CompanyText_BUTXT,
  SUBSTRING(
    CASE `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.Fiscal_Period`(
      StockWeeklySnapshots.MANDT,
      CompaniesMD.FiscalyearVariant_PERIV,
      StockWeeklySnapshots.Week_End_Date)
      WHEN 'CASE1' THEN
        `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.Fiscal_Case1`(
          StockWeeklySnapshots.MANDT,
          CompaniesMD.FiscalyearVariant_PERIV,
          StockWeeklySnapshots.Week_End_Date)
      WHEN 'CASE2' THEN
        `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.Fiscal_Case2`(
          StockWeeklySnapshots.MANDT,
          CompaniesMD.FiscalyearVariant_PERIV,
          StockWeeklySnapshots.Week_End_Date)
      WHEN 'CASE3' THEN
        `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.Fiscal_Case3`(
          StockWeeklySnapshots.MANDT,
          CompaniesMD.FiscalyearVariant_PERIV,
          StockWeeklySnapshots.Week_End_Date)
      ELSE 'DATA ISSUE'
    END, 1, 4) AS FiscalYear,
  SUBSTRING(
    CASE `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.Fiscal_Period`(
      StockWeeklySnapshots.MANDT,
      CompaniesMD.FiscalyearVariant_PERIV,
      StockWeeklySnapshots.Week_End_Date)
      WHEN 'CASE1' THEN
        `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.Fiscal_Case1`(
          StockWeeklySnapshots.MANDT,
          CompaniesMD.FiscalyearVariant_PERIV,
          StockWeeklySnapshots.Week_End_Date)
      WHEN 'CASE2' THEN
        `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.Fiscal_Case2`(
          StockWeeklySnapshots.MANDT,
          CompaniesMD.FiscalyearVariant_PERIV,
          StockWeeklySnapshots.Week_End_Date)
      WHEN 'CASE3' THEN
        `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.Fiscal_Case3`(
          StockWeeklySnapshots.MANDT,
          CompaniesMD.FiscalyearVariant_PERIV,
          StockWeeklySnapshots.Week_End_Date)
      ELSE 'DATA ISSUE'
    END, 6, 2) AS FiscalPeriod,
  StockWeeklySnapshots.cal_year AS CalYear,
  StockWeeklySnapshots.cal_week AS CalWeek,
  StockWeeklySnapshots.week_end_date AS WeekEndDate,
  StockWeeklySnapshots.MEINS AS BaseUnitOfMeasure_MEINS,
  StockWeeklySnapshots.WAERS AS CurrencyKey_WAERS,
  StockWeeklySnapshots.total_weekly_movement_quantity AS TotalWeeklyMovementQuantity,
  StockWeeklySnapshots.quantity_weekly_cumulative AS QuantityWeeklyCumulative,
  COALESCE(StockWeeklySnapshots.total_weekly_movement_amount * currency_decimal.CURRFIX,
    StockWeeklySnapshots.total_weekly_movement_amount
  ) AS TotalWeeklyMovementAmount,
  COALESCE(StockWeeklySnapshots.amount_weekly_cumulative * currency_decimal.CURRFIX,
    StockWeeklySnapshots.amount_weekly_cumulative
  ) AS AmountWeeklyCumulative,
  StockWeeklySnapshots.stock_characteristic AS StockCharacteristic
FROM
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.stock_weekly_snapshots` AS StockWeeklySnapshots
LEFT JOIN `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.CompaniesMD` AS CompaniesMD
  ON StockWeeklySnapshots.MANDT = CompaniesMD.Client_MANDT
    AND StockWeeklySnapshots.BUKRS = CompaniesMD.CompanyCode_BUKRS
LEFT JOIN
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.currency_decimal` AS currency_decimal
  ON
    StockWeeklySnapshots.WAERS = currency_decimal.CURRKEY
WHERE StockWeeklySnapshots.Stock_Characteristic != 'BlockedReturns'
