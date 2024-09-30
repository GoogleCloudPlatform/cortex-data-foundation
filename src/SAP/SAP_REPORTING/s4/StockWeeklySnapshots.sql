SELECT
  StockWeeklySnapshots.MANDT AS Client_MANDT,
  StockWeeklySnapshots.MATNR AS MaterialNumber_MATNR,
  StockWeeklySnapshots.WERKS AS Plant_WERKS,
  StockWeeklySnapshots.CHARG AS BatchNumber_CHARG,
  StockWeeklySnapshots.LGORT AS StorageLocation_LGORT,
  StockWeeklySnapshots.BUKRS AS CompanyCode_BUKRS,
  CompaniesMD.CompanyText_BUTXT,
  FiscalDateDimension_WEEKENDDATE.FiscalYear,
  FiscalDateDimension_WEEKENDDATE.FiscalPeriod,
  StockWeeklySnapshots.cal_year AS CalYear,
  StockWeeklySnapshots.cal_week AS CalWeek,
  StockWeeklySnapshots.week_end_date AS WeekEndDate,
  StockWeeklySnapshots.MEINS AS BaseUnitOfMeasure_MEINS,
  StockWeeklySnapshots.WAERS AS CurrencyKey_WAERS,
  StockWeeklySnapshots.total_weekly_movement_quantity AS TotalWeeklyMovementQuantity,
  StockWeeklySnapshots.quantity_weekly_cumulative AS QuantityWeeklyCumulative,
  COALESCE(
    StockWeeklySnapshots.total_weekly_movement_amount * currency_decimal.CURRFIX,
    StockWeeklySnapshots.total_weekly_movement_amount
  ) AS TotalWeeklyMovementAmount,
  COALESCE(
    StockWeeklySnapshots.amount_weekly_cumulative * currency_decimal.CURRFIX,
    StockWeeklySnapshots.amount_weekly_cumulative
  ) AS AmountWeeklyCumulative,
  StockWeeklySnapshots.stock_characteristic AS StockCharacteristic
FROM
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.stock_weekly_snapshots` AS StockWeeklySnapshots
LEFT JOIN
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.CompaniesMD` AS CompaniesMD
  ON
    StockWeeklySnapshots.MANDT = CompaniesMD.Client_MANDT
    AND StockWeeklySnapshots.BUKRS = CompaniesMD.CompanyCode_BUKRS
LEFT JOIN
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.fiscal_date_dim` AS FiscalDateDimension_WEEKENDDATE
  ON
    StockWeeklySnapshots.MANDT = FiscalDateDimension_WEEKENDDATE.MANDT
    AND CompaniesMD.FiscalyearVariant_PERIV = FiscalDateDimension_WEEKENDDATE.PERIV
    AND StockWeeklySnapshots.WEEK_END_DATE = FiscalDateDimension_WEEKENDDATE.DATE
LEFT JOIN
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.currency_decimal` AS currency_decimal
  ON
    StockWeeklySnapshots.WAERS = currency_decimal.CURRKEY
WHERE StockWeeklySnapshots.Stock_Characteristic != 'BlockedReturns'
