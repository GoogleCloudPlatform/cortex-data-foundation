--DaysPayableOutstanding is language independent view.
WITH
  AccountsPayableAgg AS (
  --## CORTEX-CUSTOMER: Please consider materializing AccountsPayable or this CTE
    SELECT
      AccountsPayable.Client_MANDT,
      AccountsPayable.CompanyCode_BUKRS,
      AccountsPayable.CompanyText_BUTXT,
      AccountsPayable.TargetCurrency_TCURR,
      SUBSTR(AccountsPayable.DocFiscPeriod, 1, 4) AS FiscalYear,
      SUBSTR(AccountsPayable.DocFiscPeriod, 5, 3) AS FiscalPeriod,
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
      AND InventoryKeyMetrics.LanguageKey_SPRAS = (
        SELECT ANY_VALUE(LanguageKey_SPRAS)
        FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.InventoryKeyMetrics`
      )
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
      Fiscal.StartDate AS FiscalPeriodStart,
      Fiscal.EndDate AS FiscalPeriodEnd,
      SUM(AccountsPayableAgg.OverdueAmountInSourceCurrency) OVER (
        PARTITION BY
          AccountsPayableAgg.Client_MANDT,
          AccountsPayableAgg.CompanyCode_BUKRS,
          AccountsPayableAgg.TargetCurrency_TCURR
        ORDER BY
          AccountsPayableAgg.FiscalYear,
          AccountsPayableAgg.FiscalPeriod
      ) AS PeriodAPInSourceCurrency,

      SUM(AccountsPayableAgg.OverdueAmountInTargetCurrency) OVER (
        PARTITION BY
          AccountsPayableAgg.Client_MANDT,
          AccountsPayableAgg.CompanyCode_BUKRS,
          AccountsPayableAgg.TargetCurrency_TCURR
        ORDER BY
          AccountsPayableAgg.FiscalYear,
          AccountsPayableAgg.FiscalPeriod
      ) AS PeriodAPInTargetCurrency

    FROM AccountsPayableAgg AS AccountsPayableAgg
    INNER JOIN `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.CompaniesMD` AS Company
      ON
        AccountsPayableAgg.Client_MANDT = Company.Client_MANDT
        AND AccountsPayableAgg.CompanyCode_BUKRS = Company.CompanyCode_BUKRS
    INNER JOIN ( --noqa: disable= ST05
      SELECT
        mandt,
        periv,
        FiscalPeriod,
        FiscalYear,
        MIN(Date) AS StartDate,
        MAX(Date) AS Enddate
      FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.fiscal_date_dim`
      WHERE mandt = '{{ mandt }}'
      GROUP BY mandt, periv, FiscalPeriod, FiscalYear
    ) AS Fiscal --noqa: enable=all
      ON
        AccountsPayableAgg.Client_MANDT = Fiscal.mandt
        AND Company.FiscalyearVariant_PERIV = Fiscal.periv
        AND AccountsPayableAgg.FiscalYear = Fiscal.FiscalYear
        AND AccountsPayableAgg.FiscalPeriod = Fiscal.FiscalPeriod
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
    DAY
  ) + 1 AS NumberOfDays,

  SAFE_DIVIDE(
    AccountsPayableKPI.PeriodAPInSourceCurrency
    * (DATE_DIFF(
      AccountsPayableKPI.FiscalPeriodEnd,
      AccountsPayableKPI.FiscalPeriodStart,
      DAY
    ) + 1),
    InventoryMetricsAgg.COGSInSourceCurrency
  ) AS DaysPayableOutstandingInSourceCurrency,

  SAFE_DIVIDE(
    AccountsPayableKPI.PeriodAPInTargetCurrency
    * (DATE_DIFF(
      AccountsPayableKPI.FiscalPeriodEnd,
      AccountsPayableKPI.FiscalPeriodStart,
      DAY
    ) + 1),
    InventoryMetricsAgg.COGSInTargetCurrency
  ) AS DaysPayableOutstandingInTargetCurrency

FROM AccountsPayableKPI AS AccountsPayableKPI
INNER JOIN InventoryMetricsAgg AS InventoryMetricsAgg
  ON AccountsPayableKPI.Client_MANDT = InventoryMetricsAgg.Client_MANDT
    AND AccountsPayableKPI.CompanyCode_BUKRS = InventoryMetricsAgg.CompanyCode_BUKRS
    AND AccountsPayableKPI.FiscalYear = InventoryMetricsAgg.FiscalYear
    AND AccountsPayableKPI.FiscalPeriod = InventoryMetricsAgg.FiscalPeriod
    AND AccountsPayableKPI.TargetCurrency_TCURR = InventoryMetricsAgg.TargetCurrency_TCURR
