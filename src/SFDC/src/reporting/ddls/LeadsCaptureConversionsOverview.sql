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

/*
* This view is intended to showcase how KPIs from LeadCaptureConversions view are meant to be
* calculated without the accompanying Looker visualizations.
*
* Please note that this is view is INFORMATIONAL ONLY and may be subject to change without
* notice in upcoming Cortex Data Foundation releases.
*/


SELECT
  LeadCountry,
  LeadIndustry,
  LeadOwnerName,
  LeadSource,
  LeadStatus,
  LeadCreatedDatestamp,
  TotalSaleAmountInTargetCurrency,
  TargetCurrency,
  SourceCurrency,
  CurrencyExchangeRate,
  CurrencyConversionDate,
  --## CORTEX-CUSTOMER Consider adding other dimensions from the CalendarDateDimension table as per your requirement
  LeadCreatedDate,
  LeadCreatedWeek,
  LeadCreatedMonth,
  LeadCreatedQuarter,
  LeadCreatedYear,
  COUNT(LeadId) AS NumOfLeads,
  COUNT(DISTINCT IF(IsLeadConverted IS TRUE, LeadId, NULL)) AS NumOfConvertedLeads,
  SUM(DATETIME_DIFF(LeadFirstResponseDatestamp, LeadCreatedDatestamp, HOUR)) AS TotalLeadResponseTimeHours,
  COUNTIF(LeadFirstResponseDatestamp IS NOT NULL) AS NumOfLeadsWithResponse
FROM
  `{{ project_id_tgt }}.{{ sfdc_datasets_reporting }}.LeadsCaptureConversions`
GROUP BY
  LeadCountry,
  LeadIndustry,
  LeadOwnerName,
  LeadSource,
  LeadStatus,
  --## CORTEX-CUSTOMER Consider adding other dimensions from the CalendarDateDimension table as per your requirement
  LeadCreatedDate,
  LeadCreatedWeek,
  LeadCreatedMonth,
  LeadCreatedQuarter,
  LeadCreatedYear,
  LeadCreatedDatestamp,
  TotalSaleAmountInTargetCurrency,
  TargetCurrency,
  SourceCurrency,
  CurrencyExchangeRate,
  CurrencyConversionDate
