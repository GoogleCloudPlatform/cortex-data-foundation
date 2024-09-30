SELECT
  setheadert.MANDT AS Client_MANDT,
  setheadert.SETCLASS AS SetClass_SETCLASS,
  setheadert.SUBCLASS AS OrganizationalUnit_SUBCLASS,
  setheadert.SETNAME AS SetName_SETNAME,
  setheadert.LANGU AS LanguageKey_LANGU,
  setheadert.DESCRIPT AS ShortDescriptionOfSet_DESCRIPT
FROM
  `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.setheadert` AS setheadert
WHERE
  setclass = '0101'
