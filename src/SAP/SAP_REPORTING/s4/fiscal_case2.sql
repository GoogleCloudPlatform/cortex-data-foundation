((
    SELECT
      CONCAT(CAST(Bdatj AS INT) + CAST(Reljr AS INT), Poper)
    FROM
      `{{ project_id_src }}.{{ dataset_cdc_processed_s4 }}.t009b`
    WHERE
      Mandt = Ip_Mandt
      AND Periv = Ip_Periv
      AND Bdatj = CAST(EXTRACT(YEAR FROM Ip_Date) AS STRING)
      AND Bumon = CAST(CAST(Ip_Date AS STRING FORMAT('MM')) AS STRING)
))
