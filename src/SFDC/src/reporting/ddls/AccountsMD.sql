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
  Accounts.AccountId,
  Accounts.AccountSource,
  Accounts.AnnualRevenue,
  Accounts.BillingCity,
  Accounts.BillingCountry,
  Accounts.BillingLatitude,
  Accounts.BillingLongitude,
  Accounts.BillingPostalcode,
  Accounts.BillingState,
  Accounts.BillingStreet,
  Accounts.CreatedById,
  Accounts.CreatedDatestamp,
  Accounts.Description,
  Accounts.Fax,
  Accounts.Industry,
  Accounts.LastActivityDate,
  Accounts.LastModifiedById,
  Accounts.LastModifiedDatestamp,
  Accounts.LastReferencedDatestamp,
  Accounts.LastViewedDatestamp,
  Accounts.MasterRecordId,
  Accounts.Name,
  Accounts.NumberOfEmployees,
  Accounts.OwnerId,
  Accounts.ParentId,
  Accounts.Phone,
  Accounts.RecordTypeId,
  Accounts.ShippingCity,
  Accounts.ShippingCountry,
  Accounts.ShippingGeocodeAccuracy,
  Accounts.ShippingLatitude,
  Accounts.ShippingLongitude,
  Accounts.ShippingPostalcode,
  Accounts.ShippingState,
  Accounts.ShippingStreet,
  Accounts.Type,
  Accounts.Website
FROM `{{ project_id_src }}.{{ sfdc_datasets_cdc }}.accounts` AS Accounts
