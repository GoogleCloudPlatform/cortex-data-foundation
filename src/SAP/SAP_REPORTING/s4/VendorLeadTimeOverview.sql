--'VendorLeadTimeOverview' - A view built as reference for details of calculations implemented in dashboards.
--It is not designed for extensive reporting or analytical use.
SELECT DISTINCT
  VendorAccountNumber_LIFNR,
  NAME1 AS VendorName,
  CountryKey_LAND1 AS VendorCountry,
  Company_BUKRS,
  CompanyText_BUTXT AS Company,
  PurchasingGroup_EKGRP,
  PurchasingGroupText_EKNAM AS PurchasingGroup,
  PurchasingOrganization_EKORG,
  PurchasingOrganizationText_EKOTX AS PurchasingOrganization,
  PurchasingDocumentDate_BEDAT,
  YearOfPurchasingDocumentDate_BEDAT,
  MonthOfPurchasingDocumentDate_BEDAT,
  WeekOfPurchasingDocumentDate_BEDAT,
  TargetCurrency_TCURR,
  LanguageKey_SPRAS,
  --##CORTEX-CUSTOMER Consider adding other parameters in the PARTITION BY
  --clause to view aggregation at different level as per requirement.
  ROUND(
    AVG(VendorCycleTimeInDays) OVER (PARTITION BY VendorAccountNumber_LIFNR),
    1) AS AverageLeadTimeInDays
FROM
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.VendorPerformance`
WHERE
  Client_MANDT = '{{ mandt }}'
