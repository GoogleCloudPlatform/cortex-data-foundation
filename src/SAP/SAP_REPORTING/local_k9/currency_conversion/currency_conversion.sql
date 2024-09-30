CREATE OR REPLACE TABLE `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.currency_conversion`
(
  mandt STRING,
  kurst STRING,
  fcurr STRING,
  tcurr STRING,
  ukurs NUMERIC,
  start_date DATE,
  end_date DATE,
  conv_date DATE
)
PARTITION BY conv_date;

CREATE OR REPLACE FUNCTION `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.gdatu_to_date`(gdatu STRING) AS
(
  (SELECT PARSE_DATE('%Y%m%d', CAST(99999999 - CAST(gdatu AS INT) AS STRING)))
);

INSERT INTO `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.currency_conversion`
WITH
  TCURR AS (
    --This table gives the exchange rate for different currency key combination.
    SELECT
      tcurr.mandt,
      tcurr.kurst,
      tcurr.fcurr,
      tcurr.tcurr,
      IF(
        tcurr.ukurs < 0, SAFE_DIVIDE(1, ABS(tcurr.ukurs)), tcurr.ukurs
      ) AS ukurs,
      `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}`.gdatu_to_date(tcurr.gdatu) AS start_date,
      IF(
        LEAD(`{{ project_id_tgt }}.{{ dataset_reporting_tgt }}`.gdatu_to_date(tcurr.gdatu)) OVER (
          PARTITION BY tcurr.mandt, tcurr.kurst, tcurr.fcurr, tcurr.tcurr
          ORDER BY tcurr.gdatu DESC) IS NULL,
        CURRENT_DATE(),
        DATE_SUB(
          LEAD(`{{ project_id_tgt }}.{{ dataset_reporting_tgt }}`.gdatu_to_date(tcurr.gdatu)) OVER (
            PARTITION BY tcurr.mandt, tcurr.kurst, tcurr.fcurr, tcurr.tcurr
            ORDER BY tcurr.gdatu DESC),
          INTERVAL 1 DAY)
      ) AS end_date
    FROM
      `{{ project_id_src }}.{{ dataset_cdc_processed }}.tcurr` AS tcurr
  ),
  TCURF AS (
    --This table gives the ratio for the "from" and "to" currency units.
    SELECT
      tcurf.mandt,
      tcurf.kurst,
      tcurf.fcurr,
      tcurf.tcurr,
      tcurf.gdatu,
      `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}`.gdatu_to_date(tcurf.gdatu) AS start_date,
      IF(
        LEAD(`{{ project_id_tgt }}.{{ dataset_reporting_tgt }}`.gdatu_to_date(tcurf.gdatu)) OVER (
          PARTITION BY tcurf.mandt, tcurf.kurst, tcurf.fcurr, tcurf.tcurr
          ORDER BY tcurf.gdatu DESC) IS NULL,
        CURRENT_DATE(),
        DATE_SUB(
          LEAD(`{{ project_id_tgt }}.{{ dataset_reporting_tgt }}`.gdatu_to_date(tcurf.gdatu)) OVER (
            PARTITION BY tcurf.mandt, tcurf.kurst, tcurf.fcurr, tcurf.tcurr
            ORDER BY tcurf.gdatu DESC),
          INTERVAL 1 DAY)
      ) AS end_date,
      COALESCE(tcurf.ffact, 1) AS ffact,
      COALESCE(tcurf.tfact, 1) AS tfact
    FROM
      `{{ project_id_src }}.{{ dataset_cdc_processed }}.tcurf` AS tcurf
  ),
  CurrencyConversion AS (
    SELECT
      tcurr.mandt,
      tcurr.kurst,
      tcurr.fcurr,
      tcurr.tcurr,
      tcurr.ukurs * (tcurf.tfact / tcurf.ffact) AS ukurs,
      tcurr.start_date,
      tcurr.end_date
    FROM
      TCURR AS tcurr
    INNER JOIN
      TCURF AS tcurf
      ON tcurr.mandt = tcurf.mandt
        AND tcurr.kurst = tcurf.kurst
        AND tcurr.fcurr = tcurf.fcurr
        AND tcurr.tcurr = tcurf.tcurr
    WHERE tcurr.start_date >= tcurf.start_date
      AND tcurr.end_date <= tcurf.end_date
    UNION ALL
    --The follwing SQL adds same currency to same currency conversion in the table.
    SELECT DISTINCT
      tcurr.mandt,
      tcurr.kurst,
      tcurr.fcurr,
      tcurr.fcurr AS tcurr,
      1 AS ukurs,
      DATE_SUB(CURRENT_DATE(), INTERVAL 10 YEAR) AS start_date,
      CURRENT_DATE() AS end_date
    FROM
      `{{ project_id_src }}.{{ dataset_cdc_processed }}.tcurr` AS tcurr
  )
SELECT
  *
FROM
  CurrencyConversion,
  UNNEST(GENERATE_DATE_ARRAY(start_date, end_date)) AS conv_date
WHERE conv_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 10 YEAR) AND CURRENT_DATE();
