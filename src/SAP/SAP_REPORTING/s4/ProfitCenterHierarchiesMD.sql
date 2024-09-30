SELECT
  sethanahier0106.MANDT AS Client_MANDT,
  sethanahier0106.SETCLASS AS SetClass_SETCLASS,
  sethanahier0106.SUBCLASS AS OrganizationalUnit_SUBCLASS,
  sethanahier0106.HIERBASE AS SetName_HIERBASE,
  sethanahier0106.SUCC AS NodeNumber_SUCC,
  sethanahier0106.PRED AS NodeNumber_PRED,
  sethanahier0106.HLEVEL AS Level_HLEVEL,
  sethanahier0106.SETID AS Identification_SETID,
  sethanahier0106.SETNAME AS SetName_SETNAME,
  sethanahier0106.VCOUNT AS Counter_VCOUNT,
  sethanahier0106.SEARCHFLD AS SearchField_SEARCHFLD,
  sethanahier0106.OLD_LINE AS LineCounter_OLD_LINE,
  sethanahier0106.VALUE_FROM AS FromValue_VALUE_FROM,
  sethanahier0106.VALUE_TO AS ToValue_VALUE_TO
FROM
  `{{ project_id_src }}.{{ dataset_cdc_processed_s4 }}.sethanahier0106` AS sethanahier0106
