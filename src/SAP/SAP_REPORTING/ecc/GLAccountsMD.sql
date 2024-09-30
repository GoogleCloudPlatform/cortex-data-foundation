SELECT
  SKA1.MANDT AS Client_MANDT,
  SKA1.KTOPL AS ChartOfAccounts_KTOPL,
  SKA1.SAKNR AS GlAccountNumber_SAKNR,
  SKA1.XBILK AS Indicator_AccountIsABalanceSheetAccount_XBILK,
  SKA1.SAKAN AS GlAccountNumber_SignificantLength_SAKAN,
  SKA1.BILKT AS GroupAccountNumber_BILKT,
  SKA1.ERDAT AS DateOnWhichTheRecordWasCreated_ERDAT,
  SKA1.ERNAM AS NameOfPersonWhoCreatedTheObject_ERNAM,
  SKA1.GVTYP AS PlStatementAccountType_GVTYP,
  SKA1.KTOKS AS GlAccountGroup_KTOKS,
  SKA1.MUSTR AS NumberOfTheSampleAccount_MUSTR,
  SKA1.VBUND AS CompanyIdOfTradingPartner_VBUND,
  SKA1.XLOEV AS Indicator_AccountMarkedForDeletion_XLOEV,
  SKA1.XSPEA AS Indicator_AccountIsBlockedForCreation_XSPEA,
  SKA1.XSPEB AS Indicator_IsAccountBlockedForPosting_XSPEB,
  SKA1.XSPEP AS Indicator_AccountBlockedForPlanning_XSPEP,
  SKA1.MCOD1 AS SearchTermForUsingMatchcode_MCOD1,
  SKA1.FUNC_AREA AS FunctionalArea_FUNC_AREA,
  SKAT.SPRAS AS Language_SPRAS,
  SKAT.TXT20 AS GlAccountShortText_TXT20,
  SKAT.TXT50 AS GlAccountLongText_TXT50,
  SKAT.MCOD1 AS SearchTermForMatchcodeSearch_MCOD1
FROM `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.ska1` AS SKA1
INNER JOIN `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.skat` AS SKAT
  ON SKA1.MANDT = SKAT.MANDT AND SKA1.KTOPL = SKAT.KTOPL AND SKA1.SAKNR = SKAT.SAKNR
