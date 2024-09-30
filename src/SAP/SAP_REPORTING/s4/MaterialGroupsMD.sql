SELECT
  t023.MANDT AS Client_MANDT,
  t023.MATKL AS MaterialGroup_MATKL,
  t023.SPART AS Division_SPART,
  t023.WWGDA AS ReferenceGroupRefMaterial_WWGDA,
  t023.WWGPA AS GroupMaterial_WWGPA,
  t023.ABTNR AS DepartmentNumber_ABTNR,
  t023.BEGRU AS AuthorizationGroup_BEGRU,
  t023.GEWEI AS DefaultUnitofWeight_GEWEI,
  t023.BKLAS AS ValuationClass_BKLAS,
  t023.EKWSL AS PurchasingValueKey_EKWSL,
  t023.ANLKL AS AssetClass_ANLKL,
  t023.PRICE_GROUP AS PriceLevelGroup_PRICE_GROUP,
  t023t.wgbez AS MaterialGroupName_WGBEZ,
  t023t.spras AS Language_SPRAS
FROM `{{ project_id_src }}.{{ dataset_cdc_processed_s4 }}.t023` AS t023
INNER JOIN `{{ project_id_src }}.{{ dataset_cdc_processed_s4 }}.t023t` AS t023t
  ON t023.MANDT = t023t.MANDT
    AND t023.MATKL = t023t.MATKL
