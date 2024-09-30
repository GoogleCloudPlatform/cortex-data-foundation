SELECT
  CEPC.MANDT AS Client_MANDT,
  CEPC.PRCTR AS ProfitCenter_PRCTR,
  CEPC.DATBI AS ValidToDate_DATBI,
  CEPC.KOKRS AS ControllingArea_KOKRS,
  CEPC.DATAB AS ValidFromDate_DATAB,
  CEPC.ERSDA AS CreatedOn_ERSDA,
  CEPC.USNAM AS EnteredBy_USNAM,
  CEPC.MERKMAL AS FieldNameOfCoPaCharacteristic_MERKMAL,
  CEPC.ABTEI AS Department_ABTEI,
  CEPC.VERAK AS PersonResponsibleForProfitCenter_VERAK,
  CEPC.VERAK_USER AS UserResponsibleForTheProfitCenter_VERAK_USER,
  CEPC.WAERS AS CurrencyKey_WAERS,
  CEPC.NPRCTR AS SuccessorProfitCenter_NPRCTR, CEPC.LAND1 AS CountryKey_LAND1,
  CEPC.ANRED AS Title_ANRED, CEPC.NAME1 AS NAME1, CEPC.NAME2 AS NAME2,
  CEPC.NAME3 AS NAME3, CEPC.NAME4 AS NAME4, CEPC.ORT01 AS City_ORT01,
  CEPC.ORT02 AS District_ORT02,
  CEPC.STRAS AS StreetAndHouseNumber_STRAS, CEPC.PFACH AS PoBox_PFACH,
  CEPC.PSTLZ AS PostalCode_PSTLZ, CEPC.PSTL2 AS POBoxPostalCode_PSTL2,
  CEPC.SPRAS AS CtrLanguage_SPRAS,
  CEPC.TELBX AS TeleboxNumber_TELBX, CEPC.TELF1 AS FirstTelephoneNumber_TELF1,
  CEPC.TELF2 AS SecondTelephoneNumber_TELF2, CEPC.TELFX AS FaxNumber_TELFX,
  CEPC.TELTX AS TeletexNumber_TELTX, CEPC.TELX1 AS TelexNumber_TELX1,
  CEPC.DATLT AS DataCommunicationLineNo_DATLT,
  CEPC.DRNAM AS PrinterNameForProfitCenter_DRNAM,
  CEPC.KHINR AS ProfitCenterArea_KHINR,
  CEPC.BUKRS AS CompanyCode_BUKRS,
  CEPC.VNAME AS JointVenture_VNAME,
  CEPC.RECID AS RecoveryIndicator_RECID, CEPC.ETYPE AS EquityType_ETYPE,
  CEPC.TXJCD AS TaxJurisdiction_TXJCD, CEPC.REGIO AS Region_state_REGIO,
  CEPC.KVEWE AS UsageOfTheConditionTable_KVEWE,
  CEPC.KAPPL AS Application_KAPPL, CEPC.KALSM AS Procedure__pricing__KALSM,
  CEPC.LOCK_IND AS LockIndicator_LOCK_IND,
  CEPC.PCA_TEMPLATE AS TemplateForFormulaPlanningInProfitCenters_PCA_TEMPLATE,
  CEPC.SEGMENT AS SegmentForSegmentalReporting_SEGMENT,
  CEPCT.SPRAS AS Language_SPRAS, CEPCT.DATBI AS TxtValidToDate_DATBI,
  CEPCT.KTEXT AS GeneralName_KTEXT,
  CEPCT.LTEXT AS LongText_LTEXT,
  CEPCT.MCTXT AS SearchTermForMatchcodeSearch_MCTXT
FROM `{{ project_id_src }}.{{ dataset_cdc_processed_s4 }}.cepc` AS CEPC
INNER JOIN `{{ project_id_src }}.{{ dataset_cdc_processed_s4 }}.cepct` AS CEPCT
           ON CEPC.MANDT = CEPCT.MANDT
    AND CEPC.PRCTR = CEPCT.PRCTR
    AND CEPC.DATBI = CEPCT.DATBI AND CEPC.KOKRS = CEPCT.KOKRS -- noqa: disable=L012
WHERE cast(CEPC.DATAB AS STRING) <= concat( cast(extract( YEAR FROM current_date())AS STRING), cast(extract( MONTH FROM current_date())AS STRING), cast(extract( DAY FROM current_date())AS STRING) )
  AND cast(CEPC.DATBI AS STRING) >= concat( cast(extract( YEAR FROM current_date())AS STRING), cast(extract( MONTH FROM current_date())AS STRING), cast(extract( DAY FROM current_date())AS STRING) )
