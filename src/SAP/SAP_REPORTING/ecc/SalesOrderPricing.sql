SELECT
  PricingConditions.Client_MANDT,
  PricingConditions.NumberOfTheDocumentCondition_KNUMV,
  PricingConditions.ConditionItemNumber_KPOSN,
  MAX(CurrencyKey_WAERS) AS ConditionValueCurrencyKey_WAERS,
  MAX(Checkbox_KDATU) AS Checkbox_KDATU,
  SUM(
    IF(
      PricingConditions.CalculationTypeForCondition_KRECH = 'C'
        AND PricingConditions.ConditionClass_KOAID = 'B'
        AND PricingConditions.ConditionIsInactive_KINAK IS NULL,
      PricingConditions.ConditionValue_KWERT,
    NULL)
  ) AS ListPrice,
  SUM(
    IF(
      PricingConditions.CalculationTypeForCondition_KRECH = 'C'
        AND PricingConditions.ConditionClass_KOAID = 'B'
        AND PricingConditions.ConditionType_KSCHL = 'PB00',
      PricingConditions.ConditionValue_KWERT,
    NULL)
  ) AS AdjustedPrice,
  SUM(
    IF(
      PricingConditions.ConditionClass_KOAID = 'A'
        AND PricingConditions.ConditionIsInactive_KINAK IS NULL,
      PricingConditions.ConditionValue_KWERT,
    NULL)
  ) AS Discount,
  SUM(
    IF(
      PricingConditions.ConditionForInterCompanyBilling_KFKIV = 'X'
        AND PricingConditions.ConditionClass_KOAID = 'B'
        AND PricingConditions.ConditionIsInactive_KINAK IS NULL,
      PricingConditions.ConditionValue_KWERT,
    NULL)
  ) AS InterCompanyPrice
--##CORTEX-CUSTOMER If you prefer to use currency conversion, uncomment below and
--## uncomment currency_conversion in PricingConditions
-- MAX(TargetCurrency_TCURR) AS TargetCurrency_TCURR,
--  SUM(
--   IF(
--     PricingConditions.CalculationTypeForCondition_KRECH = 'C'
--       AND PricingConditions.ConditionClass_KOAID = 'B'
--       AND PricingConditions.ConditionIsInactive_KINAK IS NULL,
--     PricingConditions.ConditionValueInTargetCurrency_KWERT,
--   NULL)
-- ) AS ListPriceInTargetCurrency,
-- SUM(
--   IF(
--     PricingConditions.CalculationTypeForCondition_KRECH = 'C'
--       AND PricingConditions.ConditionClass_KOAID = 'B'
--       AND PricingConditions.ConditionType_KSCHL = 'PB00',
--     PricingConditions.ConditionValueInTargetCurrency_KWERT,
--   NULL)
-- ) AS AdjustedPriceInTargetCurrency,
-- SUM(
--   IF(
--     PricingConditions.ConditionClass_KOAID = 'A'
--       AND PricingConditions.ConditionIsInactive_KINAK IS NULL,
--     PricingConditions.ConditionValueInTargetCurrency_KWERT,
--   NULL)
-- ) AS DiscountInTargetCurrency,
-- SUM(
--   IF(
--     PricingConditions.ConditionForInterCompanyBilling_KFKIV = 'X'
--       AND PricingConditions.ConditionClass_KOAID = 'B'
--       AND PricingConditions.ConditionIsInactive_KINAK IS NULL,
--     PricingConditions.ConditionValueInTargetCurrency_KWERT,
--   NULL)
-- ) AS InterCompanyPriceInTargetCurrency
FROM
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.PricingConditions` AS PricingConditions
GROUP BY
  PricingConditions.Client_MANDT,
  PricingConditions.NumberOfTheDocumentCondition_KNUMV,
  PricingConditions.ConditionItemNumber_KPOSN
