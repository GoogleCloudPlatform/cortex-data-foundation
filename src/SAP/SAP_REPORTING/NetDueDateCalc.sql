CREATE OR REPLACE FUNCTION `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.NetDueDateCalc`(
  ip_koart STRING,
  ip_ZFBDT DATE,
  ip_BLDAT DATE,
  ip_shkzg STRING,
  ip_REBZG STRING,
  ip_ZBD3T NUMERIC,
  ip_ZBD2T NUMERIC,
  ip_ZBD1T NUMERIC
) AS (
  (
    SELECT date_add(if(Ip_Koart = 'D' AND Ip_ZFBDT IS NULL, Ip_BLDAT, Ip_ZFBDT),
      INTERVAL cast(if(Ip_Koart = 'D' AND Ip_Shkzg = 'H' AND Ip_REBZG IS NULL, 0, CASE
        WHEN Ip_Koart = 'D' AND Ip_ZBD3T IS NOT NULL THEN Ip_ZBD3T
        WHEN Ip_Koart = 'D' AND Ip_ZBD2T IS NOT NULL THEN Ip_ZBD2T
        WHEN Ip_Koart = 'D' AND Ip_ZBD1T IS NOT NULL THEN Ip_ZBD1T
        WHEN Ip_ZBD1T IS NULL THEN 0
        END) AS INT64) DAY)
  )
);
