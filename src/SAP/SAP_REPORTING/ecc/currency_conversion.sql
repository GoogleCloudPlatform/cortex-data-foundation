((
    SELECT
      CASE
        WHEN ukurs < 0 THEN (1 / ABS(ukurs)) * ip_amount
        ELSE
          ukurs * ip_amount
      END
    FROM (
      SELECT
        mandt,
        kurst,
        fcurr,
        tcurr,
        ukurs,
        PARSE_DATE('%Y%m%d',
          CAST(99999999 - CAST(gdatu AS INT) AS STRING)) AS start_date,
        IF(LEAD(PARSE_DATE('%Y%m%d',
          CAST(99999999 - CAST(gdatu AS INT) AS STRING))) OVER (PARTITION BY mandt, kurst, fcurr, tcurr ORDER BY gdatu DESC) IS NULL,
          DATE_ADD(PARSE_DATE('%Y%m%d',
            CAST(99999999 - CAST(gdatu AS INT) AS STRING)), INTERVAL 1000 YEAR),
          DATE_SUB(LEAD(PARSE_DATE('%Y%m%d',
            CAST(99999999 - CAST(gdatu AS INT) AS STRING))) OVER (PARTITION BY mandt, kurst, fcurr, tcurr ORDER BY gdatu DESC), INTERVAL 1 DAY)) AS end_date
      FROM
        `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.tcurr`)
    WHERE
      mandt = ip_mandt
      AND kurst = ip_kurst
      AND fcurr = ip_fcurr
      AND tcurr = ip_tcurr
      AND ip_date BETWEEN start_date AND end_date
))
