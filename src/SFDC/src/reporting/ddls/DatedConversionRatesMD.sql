#-- Copyright 2023 Google LLC
#--
#-- Licensed under the Apache License, Version 2.0 (the "License");
#-- you may not use this file except in compliance with the License.
#-- You may obtain a copy of the License at
#--
#--     https://www.apache.org/licenses/LICENSE-2.0
#--
#-- Unless required by applicable law or agreed to in writing, software
#-- distributed under the License is distributed on an "AS IS" BASIS,
#-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#-- See the License for the specific language governing permissions and
#-- limitations under the License.


SELECT
  DatedConversionRate.DatedConversionRateId,
  DatedConversionRate.IsoCode,
  DatedConversionRate.StartDate,
  DatedConversionRate.NextStartDate,
  DatedConversionRate.ConversionRate,
  DatedConversionRate.CreatedDatestamp,
  DatedConversionRate.CreatedById,
  DatedConversionRate.LastModifiedDatestamp,
  DatedConversionRate.LastModifiedById,
  DATE_SUB(DatedConversionRate.NextStartDate, INTERVAL 1 DAY) AS EndDate
FROM
  `{{ project_id_src }}.{{ sfdc_datasets_cdc }}.dated_conversion_rates` AS DatedConversionRate
