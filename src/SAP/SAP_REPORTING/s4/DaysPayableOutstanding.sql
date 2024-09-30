--DaysPayableOutstanding is language independent view.
WITH AccountsPayableAgg AS (
  --## CORTEX-CUSTOMER: Please consider materializing AccountsPayable or this CTE
  SELECT
    AccountsPayable.Client_MANDT,
    AccountsPayable.CompanyCode_BUKRS,
    AccountsPayable.CompanyText_BUTXT,
    AccountsPayable.TargetCurrency_TCURR,
    SUBSTR(AccountsPayable.DocFiscPeriod, 1, 4) AS FiscalYear,
    SUBSTR(AccountsPayable.DocFiscPeriod, 6, 2) AS FiscalPeriod,
    SUM(AccountsPayable.OverdueAmountInSourceCurrency)
    + SUM(AccountsPayable.PartialPaymentsInSourceCurrency) AS OverdueAmountInSourceCurrency,
    SUM(AccountsPayable.OverdueAmountInTargetCurrency)
    + SUM(AccountsPayable.PartialPaymentsInTargetCurrency) AS OverdueAmountInTargetCurrency
  FROM
    `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.AccountsPayable` AS AccountsPayable
  WHERE
    AccountsPayable.Client_MANDT = '{{ mandt }}'
    AND AccountsPayable.DocFiscPeriod <= AccountsPayable.KeyFiscPeriod
  GROUP BY
    AccountsPayable.Client_MANDT,
    AccountsPayable.CompanyCode_BUKRS,
    AccountsPayable.CompanyText_BUTXT,
    AccountsPayable.DocFiscPeriod,
    AccountsPayable.TargetCurrency_TCURR
),

InventoryMetricsAgg AS (
  --This view is language dependent. However the final view is language indpendent. The where clause
  --makes sure that language dependency is handled and the amount is aggregated at correct granularity.
  SELECT
    InventoryKeyMetrics.Client_MANDT,
    InventoryKeyMetrics.CompanyCode_BUKRS,
    InventoryKeyMetrics.FiscalYear,
    InventoryKeyMetrics.FiscalPeriod,
    InventoryKeyMetrics.TargetCurrency_TCURR,
    SUM(InventoryKeyMetrics.CostOfGoodsSoldByMonth) AS COGSInSourceCurrency,
    SUM(InventoryKeyMetrics.CostofGoodsSoldInTargetCurrency) AS COGSInTargetCurrency
  FROM
    `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.InventoryKeyMetrics` AS InventoryKeyMetrics
  WHERE
    InventoryKeyMetrics.Client_MANDT = '{{ mandt }}'
    AND InventoryKeyMetrics.LanguageKey_SPRAS = (SELECT ANY_VALUE(LanguageKey_SPRAS)
      FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.InventoryKeyMetrics`)
  GROUP BY
    InventoryKeyMetrics.Client_MANDT,
    InventoryKeyMetrics.CompanyCode_BUKRS,
    InventoryKeyMetrics.FiscalYear,
    InventoryKeyMetrics.FiscalPeriod,
    InventoryKeyMetrics.TargetCurrency_TCURR
),

AccountsPayableKPI AS (
  SELECT
    AccountsPayableAgg.Client_MANDT,
    AccountsPayableAgg.CompanyCode_BUKRS,
    AccountsPayableAgg.CompanyText_BUTXT,
    AccountsPayableAgg.TargetCurrency_TCURR,
    AccountsPayableAgg.FiscalYear,
    AccountsPayableAgg.FiscalPeriod,
    AccountsPayableAgg.OverdueAmountInSourceCurrency,
    AccountsPayableAgg.OverdueAmountInTargetCurrency,

    DATE(
      CAST(AccountsPayableAgg.FiscalYear AS INT64),
      CAST(AccountsPayableAgg.FiscalPeriod AS INT64),
      1) AS FiscalPeriodStart,

    LAST_DAY(
      DATE(
        CAST(AccountsPayableAgg.FiscalYear AS INT64),
        CAST(AccountsPayableAgg.FiscalPeriod AS INT64),
        1)) AS FiscalPeriodEnd,

    SUM(AccountsPayableAgg.OverdueAmountInSourceCurrency) OVER (
      PARTITION BY
        AccountsPayableAgg.Client_MANDT,
        AccountsPayableAgg.CompanyCode_BUKRS,
        AccountsPayableAgg.TargetCurrency_TCURR
      ORDER BY
        AccountsPayableAgg.FiscalYear,
        AccountsPayableAgg.FiscalPeriod) AS PeriodAPInSourceCurrency,

    SUM(AccountsPayableAgg.OverdueAmountInTargetCurrency) OVER (
      PARTITION BY
        AccountsPayableAgg.Client_MANDT,
        AccountsPayableAgg.CompanyCode_BUKRS,
        AccountsPayableAgg.TargetCurrency_TCURR
      ORDER BY
        AccountsPayableAgg.FiscalYear,
        AccountsPayableAgg.FiscalPeriod) AS PeriodAPInTargetCurrency

  FROM AccountsPayableAgg AS AccountsPayableAgg
)

SELECT
  AccountsPayableKPI.Client_MANDT,
  AccountsPayableKPI.CompanyCode_BUKRS,
  AccountsPayableKPI.CompanyText_BUTXT,
  AccountsPayableKPI.FiscalYear,
  AccountsPayableKPI.FiscalPeriod,
  AccountsPayableKPI.TargetCurrency_TCURR,
  AccountsPayableKPI.PeriodAPInSourceCurrency,
  AccountsPayableKPI.PeriodAPInTargetCurrency,
  InventoryMetricsAgg.COGSInSourceCurrency,
  InventoryMetricsAgg.COGSInTargetCurrency,

  DATE_DIFF(
    AccountsPayableKPI.FiscalPeriodEnd,
    AccountsPayableKPI.FiscalPeriodStart,
    DAY) + 1 AS NumberOfDays,

  SAFE_DIVIDE(
    AccountsPayableKPI.PeriodAPInSourceCurrency
    * (DATE_DIFF(
        AccountsPayableKPI.FiscalPeriodEnd,
        AccountsPayableKPI.FiscalPeriodStart,
        DAY) + 1),
    InventoryMetricsAgg.COGSInSourceCurrency) AS DaysPayableOutstandingInSourceCurrency,

  SAFE_DIVIDE(
    AccountsPayableKPI.PeriodAPInTargetCurrency
    * (DATE_DIFF(
        AccountsPayableKPI.FiscalPeriodEnd,
        AccountsPayableKPI.FiscalPeriodStart,
        DAY) + 1),
    InventoryMetricsAgg.COGSInTargetCurrency) AS DaysPayableOutstandingInTargetCurrency

FROM AccountsPayableKPI AS AccountsPayableKPI
INNER JOIN InventoryMetricsAgg AS InventoryMetricsAgg
  ON AccountsPayableKPI.Client_MANDT = InventoryMetricsAgg.Client_MANDT
    AND AccountsPayableKPI.CompanyCode_BUKRS = InventoryMetricsAgg.CompanyCode_BUKRS
    AND AccountsPayableKPI.FiscalYear = InventoryMetricsAgg.FiscalYear
    AND AccountsPayableKPI.FiscalPeriod = InventoryMetricsAgg.FiscalPeriod
    AND AccountsPayableKPI.TargetCurrency_TCURR = InventoryMetricsAgg.TargetCurrency_TCURR
