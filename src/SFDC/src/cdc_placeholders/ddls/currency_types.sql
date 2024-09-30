CREATE TABLE IF NOT EXISTS `{{ project_id_src }}.{{ sfdc_datasets_cdc }}.currency_types`
(
  CurrencyTypeId STRING,
  IsoCode STRING,
  ConversionRate FLOAT64,
  DecimalPlaces INT64,
  IsActive BOOL,
  IsCorporate BOOL,
  CreatedDatestamp TIMESTAMP,
  CreatedById STRING,
  LastModifiedDatestamp TIMESTAMP,
  LastModifiedById STRING,
  SystemModstamp TIMESTAMP
);
