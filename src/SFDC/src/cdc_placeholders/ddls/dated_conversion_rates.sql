CREATE TABLE IF NOT EXISTS `{{ project_id_src }}.{{ sfdc_datasets_cdc }}.dated_conversion_rates`
(
  DatedConversionRateId STRING,
  IsoCode STRING,
  StartDate DATE,
  NextStartDate DATE,
  ConversionRate FLOAT64,
  CreatedDatestamp TIMESTAMP,
  CreatedById STRING,
  LastModifiedDatestamp TIMESTAMP,
  LastModifiedById STRING,
  SystemModstamp TIMESTAMP
);
