CREATE OR REPLACE FUNCTION
`{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.DueDateForCashDiscount1`
(
  Ip_Koart STRING,
  Ip_ZFBDT DATE,
  Ip_BLDAT DATE,
  Ip_Shkzg STRING,
  Ip_REBZG STRING,
  Ip_ZBD3T NUMERIC,
  Ip_ZBD2T NUMERIC,
  Ip_ZBD1T NUMERIC
)
AS (
  (
    SELECT
      CASE
        WHEN
          Ip_ZBD1T IS NOT NULL OR Ip_ZBD2T IS NOT NULL THEN date_add(
            Ip_ZFBDT, INTERVAL cast(Ip_ZBD1T AS INT64) DAY
          )
        ELSE
          `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.NetDueDateCalc`(
            Ip_Koart, Ip_ZFBDT, Ip_BLDAT, Ip_Shkzg, Ip_REBZG, Ip_ZBD3T, Ip_ZBD2T, Ip_ZBD1T
          )
      END AS SK1DT
  )
);
