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
  Contacts.ContactId,
  Contacts.AccountId,
  Contacts.AssistantName,
  Contacts.AssistantPhone,
  Contacts.Birthdate,
  Contacts.CreatedById,
  Contacts.CreatedDatestamp,
  Contacts.Department,
  Contacts.Description,
  Contacts.Email,
  Contacts.Fax,
  Contacts.FirstName,
  Contacts.LastActivityDate,
  Contacts.LastModifiedById,
  Contacts.LastModifiedDatestamp,
  Contacts.LastName,
  Contacts.LastViewedDatestamp,
  Contacts.LeadSource,
  Contacts.MailingCity,
  Contacts.MailingCountry,
  Contacts.MailingGeocodeAccuracy,
  Contacts.MailingLatitude,
  Contacts.MailingLongitude,
  Contacts.MailingPostalCode,
  Contacts.MailingState,
  Contacts.MailingStreet,
  Contacts.MasterRecordId,
  Contacts.MobilePhone,
  Contacts.Name,
  Contacts.Title,
  Contacts.OtherCity,
  Contacts.OtherCountry,
  Contacts.OtherGeocodeAccuracy,
  Contacts.OtherLatitude,
  Contacts.OtherLongitude,
  Contacts.OtherPhone,
  Contacts.OtherPostalCode,
  Contacts.OtherState,
  Contacts.OtherStreet,
  Contacts.OwnerId,
  Contacts.Phone,
  Contacts.RecordTypeId,
  Contacts.ReportsToId,
  Contacts.Salutation,
  Contacts.PhotoUrl,
  Contacts.LastReferencedDatestamp,
  Contacts.LastCURequestDatestamp,
  Contacts.LastCuupdateDatestamp,
  Contacts.IsEmailBounced,
  Contacts.HomePhone,
  Contacts.IndividualId,
  Contacts.EmailBouncedDatestamp,
  Contacts.EmailBouncedReason
FROM
  `{{ project_id_src }}.{{ sfdc_datasets_cdc }}.contacts` AS Contacts
