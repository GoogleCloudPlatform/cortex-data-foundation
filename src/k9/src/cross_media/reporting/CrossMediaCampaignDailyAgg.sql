CREATE TABLE IF NOT EXISTS `{{ project_id_tgt }}.{{ k9_datasets_reporting }}.CrossMediaCampaignDailyAgg`
(
  ReportDate DATE,
  SourceSystem STRING,
  CampaignId STRING,
  CountryCode STRING,
  TargetCurrency STRING,
  CountryName STRING,
  CampaignName STRING,
  ProductHierarchyType STRING,
  ProductHierarchyId STRING,
  ProductHierarchyTexts ARRAY<STRING>,
  SourceCurrency STRING,
  TotalImpressions INT64,
  TotalClicks INT64,
  TotalCostInSourceCurrency FLOAT64,
  TotalCostInTargetCurrency FLOAT64,
  LastUpdateTS TIMESTAMP
);
BEGIN TRANSACTION;