-- Copyright 2024 Google LLC
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--      https://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- CustomerSiteUseMD dimension.

SELECT
  SiteUses.SITE_USE_ID,
  SiteUses.SITE_USE_CODE,
  Sites.CUST_ACCT_SITE_ID AS SITE_ID,
  Sites.CUST_ACCOUNT_ID AS ACCOUNT_ID,
  Accounts.CUSTOMER_CLASS_CODE AS ACCOUNT_CLASS_CODE,
  Accounts.CUSTOMER_TYPE AS ACCOUNT_TYPE,
  Accounts.ACCOUNT_NUMBER,
  Accounts.ACCOUNT_NAME,
  PartySites.PARTY_ID,
  Parties.PARTY_NUMBER,
  Parties.PARTY_NAME,
  PartySites.PARTY_SITE_ID,
  PartySites.PARTY_SITE_NAME,
  PartySites.STATUS AS PARTY_SITE_STATUS,
  Locations.LOCATION_ID,
  ARRAY_TO_STRING(
    [
      Locations.ADDRESS1, Locations.ADDRESS2, Locations.ADDRESS3, Locations.ADDRESS4,
      Locations.CITY, Locations.STATE, Locations.POSTAL_CODE, Locations.COUNTRY], -- noqa: LT02
    ', ') AS ADDRESS, -- noqa: LT02
  Locations.ADDRESS1,
  Locations.ADDRESS2,
  Locations.ADDRESS3,
  Locations.ADDRESS4,
  Locations.CITY,
  Locations.STATE,
  Locations.PROVINCE,
  Locations.POSTAL_CODE,
  Territories.COUNTRY_CODE,
  Territories.COUNTRY_NAME,
  COALESCE(CountryDim.RegionCode, 'Other') AS REGION
FROM
  `{{ project_id_src }}.{{ oracle_ebs_datasets_cdc }}.HZ_CUST_SITE_USES_ALL` AS SiteUses
LEFT JOIN
  `{{ project_id_src }}.{{ oracle_ebs_datasets_cdc }}.HZ_CUST_ACCT_SITES_ALL` AS Sites
  ON
    SiteUses.CUST_ACCT_SITE_ID = Sites.CUST_ACCT_SITE_ID
LEFT JOIN
  `{{ project_id_src }}.{{ oracle_ebs_datasets_cdc }}.HZ_CUST_ACCOUNTS` AS Accounts
  ON
    Sites.CUST_ACCOUNT_ID = Accounts.CUST_ACCOUNT_ID
LEFT JOIN
  `{{ project_id_src }}.{{ oracle_ebs_datasets_cdc }}.HZ_PARTY_SITES` AS PartySites
  ON
    Accounts.PARTY_ID = PartySites.PARTY_ID
    AND Sites.PARTY_SITE_ID = PartySites.PARTY_SITE_ID
LEFT JOIN
  `{{ project_id_src }}.{{ oracle_ebs_datasets_cdc }}.HZ_LOCATIONS` AS Locations
  ON
    PartySites.LOCATION_ID = Locations.LOCATION_ID
LEFT JOIN
  `{{ project_id_src }}.{{ oracle_ebs_datasets_cdc }}.FND_TERRITORIES` AS Territories
  ON
    Locations.COUNTRY = Territories.COUNTRY_CODE
LEFT JOIN
  `{{ project_id_src }}.{{ oracle_ebs_datasets_cdc }}.HZ_PARTIES` AS Parties
  ON
    Accounts.PARTY_ID = Parties.PARTY_ID
LEFT JOIN
  `{{ project_id_tgt }}.{{ k9_datasets_reporting }}.CountryDim` AS CountryDim
  ON
    Territories.COUNTRY_CODE = CountryDim.CountryCode
WHERE
  SiteUses.SITE_USE_CODE IN ('SHIP_TO', 'BILL_TO', 'SOLD_TO')
