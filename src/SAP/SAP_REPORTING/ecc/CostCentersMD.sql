SELECT
  CSKS.MANDT AS Client_MANDT, CSKS.KOKRS AS ControllingArea_KOKRS,
  CSKS.KOSTL AS CostCenter_KOSTL, CSKS.DATBI AS ValidTo_DATBI, CSKS.DATAB AS ValidFromDate_DATAB,
  CSKS.BKZKP AS LockIndicatorForActualPrimaryPostings_BKZKP,
  CSKS.PKZKP AS LockIndicatorForPlanPrimaryCosts_PKZKP, CSKS.BUKRS AS CompanyCode_BUKRS,
  CSKS.GSBER AS BusinessArea_GSBER, CSKS.KOSAR AS CostCenterCategory_KOSAR,
  CSKS.VERAK AS PersonResponsible_VERAK,
  CSKS.VERAK_USER AS UserResponsible_VERAK_USER, CSKS.WAERS AS CurrencyKey_WAERS,
  CSKS.KALSM AS CostingSheet_KALSM, CSKS.TXJCD AS TaxJurisdiction_TXJCD,
  CSKS.PRCTR AS ProfitCenter_PRCTR, CSKS.WERKS AS Plant_WERKS,
  CSKS.LOGSYSTEM AS LogicalSystem_LOGSYSTEM, CSKS.ERSDA AS CreatedOn_ERSDA,
  CSKS.USNAM AS EnteredBy_USNAM, CSKS.BKZKS AS LockIndicatorForActualSecondaryCosts_BKZKS,
  CSKS.BKZER AS LockIndicatorForActualRevenuePostings_BKZER,
  CSKS.BKZOB AS LockIndicatorForCommitmentUpdate_BKZOB,
  CSKS.PKZKS AS LockIndicatorForPlanSecondaryCosts_PKZKS,
  CSKS.PKZER AS LockIndicatorForPlanningRevenues_PKZER, CSKS.VMETH AS IndicatorForAllowedAllocationMethods_VMETH,
  CSKS.MGEFL AS IndicatorForRecordingConsumptionQuantities_MGEFL,
  CSKS.ABTEI AS Department_ABTEI, CSKS.NKOST AS SubsequentCostCenter_NKOST,
  CSKS.KVEWE AS UsageOfTheConditionTable_KVEWE, CSKS.KAPPL AS Application_KAPPL,
  CSKS.KOSZSCHL AS CoCcaOverheadKey_KOSZSCHL, CSKS.LAND1 AS CountryKey_LAND1,
  CSKS.ANRED AS Title_ANRED, CSKS.NAME1 AS Name1_NAME1, CSKS.NAME2 AS Name2_NAME2,
  CSKS.NAME3 AS Name3_NAME3, CSKS.NAME4 AS Name4_NAME4,
  CSKS.ORT01 AS City_ORT01, CSKS.ORT02 AS District_ORT02,
  CSKS.STRAS AS StreetAndHouseNumber_STRAS, CSKS.PFACH AS PoBox_PFACH,
  CSKS.PSTLZ AS PostalCode_PSTLZ, CSKS.PSTL2 AS POBoxPostalCode_PSTL2,
  CSKS.REGIO AS Region_REGIO, CSKS.SPRAS AS LanguageKey_SPRAS,
  CSKS.TELBX AS TeleboxNumber_TELBX, CSKS.TELF1 AS FirstTelephoneNumber_TELF1,
  CSKS.TELF2 AS SecondTelephoneNumber_TELF2,
  CSKS.TELFX AS FaxNumber_TELFX, CSKS.TELTX AS TeletexNumber_TELTX,
  CSKS.TELX1 AS TelexNumber_TELX1, CSKS.DATLT AS DataCommunicationLineNo_DATLT,
  CSKS.DRNAM AS PrinterDestinationForCctrReport_DRNAM,
  CSKS.KHINR AS StandardHierarchyArea_KHINR, CSKS.CCKEY AS CostCollectorKey_CCKEY,
  CSKS.KOMPL AS CompletionFlagForTheCostCenterMasterRecord_KOMPL,
  CSKS.STAKZ AS IndicatorObjectIsStatistical_STAKZ, CSKS.OBJNR AS ObjectNumber_OBJNR,
  CSKS.FUNKT AS FunctionOfCostCenter_FUNKT, CSKS.AFUNK AS AlternativeFunctionOfCostCenter_AFUNK,
  CSKS.CPI_TEMPL AS TemplateForActivityIndependentFormulaPlanning_CPI_TEMPL,
  CSKS.CPD_TEMPL AS TemplateForActivityDependentFormulaPlanning_CPD_TEMPL,
  CSKS.FUNC_AREA AS FunctionalArea_FUNC_AREA, CSKS.SCI_TEMPL AS Template_ActivityIndependentAllocationToCostCenter_SCI_TEMPL,
  CSKS.SCD_TEMPL AS Template_ActivityDependentAllocationToCostCenter_SCD_TEMPL,
  CSKS.SKI_TEMPL AS Template_ActualStatisticalKeyFigureOnCostCenter_SKI_TEMPL,
  CSKS.SKD_TEMPL AS Template_ActStatKeyFigureCostCenteractivityType_SKD_TEMPL,
  CSKS.VNAME AS JointVenture_VNAME, CSKS.RECID AS RecoveryIndicator_RECID,
  CSKS.ETYPE AS EquityType_ETYPE,
  CSKS.JV_OTYPE AS JointVentureObjectType_JV_OTYPE, CSKS.JV_JIBCL AS JibjibeClass_JV_JIBCL,
  CSKS.JV_JIBSA AS JibjibeSubclassA_JV_JIBSA, CSKS.FERC_IND AS RegulatoryIndicator_FERC_IND,
  CSKT.SPRAS AS Language_SPRAS, CSKT.KTEXT AS GeneralName_KTEXT,
  CSKT.LTEXT AS Description_LTEXT, CSKT.MCTXT AS SearchTermForMatchcodeUse_MCTXT
FROM `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.csks` AS CSKS
INNER JOIN `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.cskt` AS CSKT
  ON
    CSKS.MANDT = CSKT.mandt
    AND CSKS.KOKRS = CSKT.KOKRS
    AND CSKS.KOSTL = CSKT.KOSTL
    AND CSKS.DATBI = CSKT.DATBI
--noqa: disable=all
WHERE cast(CSKS.DATAB AS STRING) <= concat( cast(extract( YEAR FROM current_date())AS STRING), cast(extract( MONTH FROM current_date())AS STRING), cast(extract( DAY FROM current_date())AS STRING) )
  AND cast(CSKS.DATBI AS STRING) >= concat( cast(extract( YEAR FROM current_date())AS STRING), cast(extract( MONTH FROM current_date())AS STRING), cast(extract( DAY FROM current_date())AS STRING) )
--noqa: enable=all
