SELECT
  ADRC.CLIENT AS Client, ADRC.ADDRNUMBER AS AddressNumber_ADDRNUMBER, ADRC.DATE_FROM AS ValidFromDATE_FROM, ADRC.NATION AS VersionIdForInternationalAddresses_NATION,
  ADRC.DATE_TO AS ValidToDate_DATE_TO, ADRC.TITLE AS FormOfAddressKey_TITLE, ADRC.NAME1 AS Name1_NAME1, ADRC.NAME2 AS Name2_NAME2, ADRC.NAME3 AS Name3_NAME3, ADRC.NAME4 AS Name4_NAME4,
  ADRC.NAME_TEXT AS ConvertedNameField__withFormOfAddress___NAME_TEXT, ADRC.NAME_CO AS CoName_NAME_CO, ADRC.CITY1 AS City_CITY1, ADRC.CITY2 AS District_CITY2, ADRC.CITY_CODE AS CityCodeForCitystreetFile_CITY_CODE,
  ADRC.CITYP_CODE AS DistrictCodeForCityAndStreetFile_CITYP_CODE, ADRC.HOME_CITY AS City__differentFromPostalCity___HOME_CITY, ADRC.CITYH_CODE AS DifferentCityForCitystreetFile_CITYH_CODE,
  ADRC.CHCKSTATUS AS CityFileTestStatus_CHCKSTATUS, ADRC.REGIOGROUP AS RegionalStructureGrouping_REGIOGROUP, ADRC.POST_CODE1 AS CityPostalCode_POST_CODE1, ADRC.POST_CODE2 AS PoBoxPostalCode_POST_CODE2,
  ADRC.POST_CODE3 AS CompanyPostalCode__forLargeCustomers___POST_CODE3, ADRC.PCODE1_EXT AS cityPostalCodeExtension_PCODE1_EXT, ADRC.PCODE2_EXT AS PoBoxPostalCodeExtension_PCODE2_EXT,
  ADRC.PO_BOX AS PoBox_PO_BOX, ADRC.DONT_USE_P AS PoBoxAddressUndeliverableFlag_DONT_USE_P, ADRC.PO_BOX_NUM AS Flag_PoBoxWithoutNumber_PO_BOX_NUM,
  ADRC.PO_BOX_LOC AS PoBoxCity_PO_BOX_LOC, ADRC.CITY_CODE2 AS CityPoBoxCode__cityFile___CITY_CODE2, ADRC.PO_BOX_REG AS RegionForPoBox_PO_BOX_REG, ADRC.PO_BOX_CTY AS PoBoxCountry_PO_BOX_CTY,
  ADRC.TRANSPZONE AS TransportationZoneToOrFromWhichTheGoodsAreDelivered_TRANSPZONE, ADRC.STREET AS Street_STREET,
  ADRC.DONT_USE_S AS StreetAddressUndeliverableFlag_DONT_USE_S, ADRC.STREETCODE AS StreetNumberForCitystreetFile_STREETCODE,
  ADRC.HOUSE_NUM1 AS HouseNumber_HOUSE_NUM1, ADRC.HOUSE_NUM2 AS HouseNumberSupplement_HOUSE_NUM2, ADRC.STR_SUPPL1 AS Street2_STR_SUPPL1,
  ADRC.STR_SUPPL2 AS Street3_STR_SUPPL2, ADRC.STR_SUPPL3 AS Street4_STR_SUPPL3, ADRC.LOCATION AS Street5_LOCATION, ADRC.BUILDING AS Building__numberOrCode___BUILDING, ADRC.FLOOR AS FloorInBuilding_FLOOR,
  ADRC.ROOMNUMBER AS RoomOrAppartmentNumber_ROOMNUMBER, ADRC.COUNTRY AS CountryKey_COUNTRY, ADRC.LANGU AS Language_LANGU, ADRC.REGION AS Region_REGION,
  ADRC.ADDR_GROUP AS AddressGroup__key____businessAddressServices___ADDR_GROUP, ADRC.FLAGGROUPS AS Flag_ThereAreMoreAddressGroupAssignments_FLAGGROUPS, ADRC.PERS_ADDR AS Flag_ThisIsAPersonalAddress_PERS_ADDR,
  ADRC.SORT1 AS SearchTerm1_SORT1, ADRC.SORT2 AS SearchTerm2_SORT2, ADRC.DEFLT_COMM AS CommunicationMethod__key____businessAddressServices___DEFLT_COMM,
  ADRC.TEL_NUMBER AS FirstTelephone_TEL_NUMBER, ADRC.TEL_EXTENS AS FirstExtension_TEL_EXTENS, ADRC.FAX_NUMBER AS FirstFaxNor_FAX_NUMBER,
  ADRC.FAX_EXTENS AS FirstFax_FAX_EXTENS, ADRC.FLAGCOMM2 AS Flag_TelephoneNumber__s__Defined_FLAGCOMM2, ADRC.FLAGCOMM3 AS Flag_FaxNumber__s__Defined_FLAGCOMM3, ADRC.FLAGCOMM4 AS Flag_TeletexNumber__s__Defined_FLAGCOMM4,
  ADRC.FLAGCOMM5 AS Flag_TelexNumber__s__Defined_FLAGCOMM5, ADRC.FLAGCOMM6 AS EMailAddressX_FLAGCOMM6, ADRC.FLAGCOMM7 AS Flag_Rml__remoteMail__Addresse__s__Defined_FLAGCOMM7,
  ADRC.FLAGCOMM8 AS Flag_X400FLAGCOMM8, ADRC.FLAGCOMM9 AS Flag_RfcDestination__s__Defined_FLAGCOMM9, ADRC.FLAGCOMM10 AS Flag_PrinterDefined_FLAGCOMM10, ADRC.FLAGCOMM11 AS Flag_SsfDefined_FLAGCOMM11,
  ADRC.FLAGCOMM12 AS Flag_UriftpAddressDefined_FLAGCOMM12, ADRC.FLAGCOMM13 AS Flag_PagerAddressDefined_FLAGCOMM13,
  ADRC.MC_NAME1 AS Name__fieldName1__InUppercaseForSearchHelp_MC_NAME1, ADRC.MC_CITY1 AS CityNameInUppercaseForSearchHelp_MC_CITY1, ADRC.MC_STREET AS StreetNameInUppercaseForSearchHelp_MC_STREET,
  ADRC.TIME_ZONE AS AddressTimeZone_TIME_ZONE,
  ADRC.TAXJURCODE AS TaxJurisdiction_TAXJURCODE, ADRC.LANGU_CREA AS AddressRecordCreationOriginalLanguage_LANGU_CREA, ADRC.ADRC_UUID AS UuidUsedInTheAddress_ADRC_UUID,
  ADRC.UUID_BELATED AS Indicator_UuidCreatedLater_UUID_BELATED, ADRC.ID_CATEGORY AS CategoryOfAnAddressId_ID_CATEGORY, ADRC.ADRC_ERR_STATUS AS ErrorStatusOfAddress_ADRC_ERR_STATUS, ADRC.PO_BOX_LOBBY AS PoBoxLobby_PO_BOX_LOBBY,
  ADRC.DELI_SERV_TYPE AS TypeOfDeliveryService_DELI_SERV_TYPE, ADRC.DELI_SERV_NUMBER AS NumberOfDeliveryService_DELI_SERV_NUMBER, ADRC.COUNTY_CODE AS CountyCodeForCounty_COUNTY_CODE, ADRC.COUNTY AS County_COUNTY,
  ADRC.TOWNSHIP_CODE AS TownshipCodeForTownship_TOWNSHIP_CODE, ADRC.TOWNSHIP AS Township_TOWNSHIP, ADRC.MC_COUNTY AS CountyNameInUpperCaseForSearchHelp_MC_COUNTY, ADRC.MC_TOWNSHIP AS TownshipNameInUpperCaseForSearchHelp_MC_TOWNSHIP,
  ADRC.XPCPT AS BusinessPurposeCompletedFlag_XPCPT,
  ADRCT.DATE_FROM AS ValidFrom_DATE_FROM, ADRCT.NATION AS VersionInternationalAddresses_NATION, ADRCT.REMARK AS AddressNotes_REMARK,
  #ADRT.PERSNUMBER AS PersonNumber_PERSNUMBER,  ADRT.COMM_TYPE AS CommunicationMethod__key____businessAddressServices___COMM_TYPE,
  #ADRT.DATE_FROM AS ValidFromDate_DATE_FROM,  ADRT.CONSNUMBER AS SequenceNumber_CONSNUMBER,    ADRT.REMARK AS CommunicationLinkNotes_REMARK,
  ADR6.PERSNUMBER AS PersonNumber_PERSNUMBER, ADR6.DATE_FROM AS ValidFromDate_DATE_FROM,
  ADR6.CONSNUMBER AS SequenceNumber_CONSNUMBER, ADR6.FLGDEFAULT AS Flag_ThisAddressIsTheDefaultAddress_FLGDEFAULT, ADR6.FLG_NOUSE AS Flag_ThisCommunicationNumberIsNotUsed_FLG_NOUSE,
  ADR6.HOME_FLAG AS RecipientAddressInThisCommunicationType__mailSys_HOME_FLAG, ADR6.SMTP_ADDR AS EMailAddress_SMTP_ADDR, ADR6.SMTP_SRCH AS EMailAddressSearchField_SMTP_SRCH,
  ADR6.DFT_RECEIV AS Flag_RecipientIsStandardRecipientForThisAddress_DFT_RECEIV, ADR6.R3_USER AS Flag_ConnectedToAnSapSystem_R3_USER, ADR6.ENCODE AS RequiredDataEncoding__eMail___ENCODE,
  ADR6.TNEF AS Flag_ReceiverCanReceiveTnefEncodingBySmtp_TNEF, ADR6.VALID_FROM AS CommunicationData_ValidFrom__yyyymmddhhmmss___VALID_FROM, ADR6.VALID_TO AS CommunicationData_ValidTo__yyyymmddhhmmss___VALID_TO
FROM `{{ project_id_src }}.{{ dataset_cdc_processed_s4 }}.adrc` AS ADRC
INNER JOIN `{{ project_id_src }}.{{ dataset_cdc_processed_s4 }}.adr6` AS ADR6 ON adrc.client = adr6.client AND adrc.addrnumber = adr6.addrnumber
  AND adrc.date_from = adr6.date_from
LEFT OUTER JOIN `{{ project_id_src }}.{{ dataset_cdc_processed_s4 }}.adrct` AS ADRCT ON adrc.client = adrct.client AND adrc.addrnumber = adrct.addrnumber AND adrc.langu = adrct.langu
  AND adrc.date_from = adrct.date_from
#LEFT OUTER JOIN `{{ project_id_src }}.{{ dataset_cdc_processed_s4 }}.adrt` as ADRT on adrc.client  = adrct.client and adrc.addrnumber = adrt.addrnumber and adrc.langu = adrt.langu
WHERE cast(adrc.date_to AS STRING ) = '9999-12-31'
