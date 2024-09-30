CREATE OR REPLACE FUNCTION `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.Fiscal_Case1`(
  Ip_Mandt STRING, Ip_Periv STRING, Ip_Date DATE) AS
(
  (
    SELECT
    CONCAT(EXTRACT(YEAR FROM Ip_Date), LPAD(CAST(EXTRACT(MONTH FROM Ip_Date) AS STRING), 3, '0')) AS Op_Period
  )
);
