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
  Users.UserId,
  Users.AboutMe,
  Users.AccountId,
  Users.Alias,
  Users.BadgeText,
  Users.BannerPhotoUrl,
  Users.CallCenterId,
  Users.City,
  Users.CommunityNickName,
  Users.CompanyName,
  Users.ContactId,
  Users.Country,
  Users.CreatedById,
  Users.CreatedDatestamp,
  Users.DefaultGroupNotificationFrequency,
  Users.DelegatedApproverId,
  Users.Department,
  Users.DigestFrequency,
  Users.Division,
  Users.Email,
  Users.EmailEncodingKey,
  Users.EmailPreferencesAutoBcc,
  Users.EmployeeNumber,
  Users.Extension,
  Users.Fax,
  Users.FederationIdentifier,
  Users.FirstName,
  Users.ForecastEnabled,
  Users.FullPhotoUrl,
  Users.GeocodeAccuracy,
  Users.IndividualId,
  Users.IsActive,
  Users.IsProfilePhotoActive,
  Users.LanguageLocaleKey,
  Users.LastLoginDatestamp,
  Users.LastModifiedById,
  Users.LastModifiedDatestamp,
  Users.LastName,
  Users.LastReferencedDatestamp,
  Users.LastViewedDatestamp,
  Users.Latitude,
  Users.LocaleSidKey,
  Users.Longitude,
  Users.ManagerId,
  Users.MediumBannerPhotoUrl,
  Users.MediumPhotoUrl,
  Users.MobilePhone,
  Users.Name,
  Users.Phone,
  Users.PostalCode,
  Users.ProfileId,
  Users.SenderEmail,
  Users.SenderName,
  Users.Signature,
  Users.SmallBannerPhotoUrl,
  Users.SmallPhotoUrl,
  Users.State,
  Users.Street,
  Users.TimeZoneSidKey,
  Users.Title,
  Users.UserRoleId,
  Users.UserType,
  Users.UserName
FROM
  `{{ project_id_src }}.{{ sfdc_datasets_cdc }}.users` AS Users
