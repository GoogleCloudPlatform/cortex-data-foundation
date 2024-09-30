SELECT
  sethanahier0101.MANDT AS Client_MANDT,
  sethanahier0101.SETCLASS AS SetClass_SETCLASS,
  sethanahier0101.SUBCLASS AS OrganizationalUnit_SUBCLASS,
  sethanahier0101.HIERBASE AS SetName_HIERBASE,
  sethanahier0101.SUCC AS NodeNumber_SUCC,
  sethanahier0101.PRED AS NodeNumber_PRED,
  sethanahier0101.HLEVEL AS Level_HLEVEL,
  sethanahier0101.SETID AS Identification_SETID,
  sethanahier0101.SETNAME AS SetName_SETNAME,
  sethanahier0101.VCOUNT AS Counter_VCOUNT,
  sethanahier0101.SEARCHFLD AS SearchField_SEARCHFLD,
  sethanahier0101.OLD_LINE AS LineCounter_OLD_LINE,
  sethanahier0101.VALUE_FROM AS FromValue_VALUE_FROM,
  sethanahier0101.VALUE_TO AS ToValue_VALUE_TO,
  sethanahier0101.OBJNR AS ObjectNumber_OBJNR
FROM
  `{{ project_id_src }}.{{ dataset_cdc_processed_s4 }}.sethanahier0101` AS sethanahier0101
