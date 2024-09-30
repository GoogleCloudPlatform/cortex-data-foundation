SELECT
  EKES.MANDT AS Client_MANDT, EKES.EBELN AS PurchasingDocumentNumber_EBELN,
  EKES.EBELP AS ItemNumberOfPurchasingDocument_EBELP,
  EKES.ETENS AS SequentialNumberOfVendorConfirmation_ETENS,
  EKES.EBTYP AS ConfirmationCategory_EBTYP,
  EKES.EINDT AS DeliveryDateOfVendorConfirmation_EINDT,
  EKES.LPEIN AS DateCategoryOfDeliveryDateInVendorConfirmation_LPEIN,
  EKES.UZEIT AS DeliveryDateTimeSpotInVendorConfirmation_UZEIT,
  EKES.ERDAT AS CreationDateOfConfirmation_ERDAT,
  EKES.EZEIT AS TimeAtWhichVendorConfirmationWasCreated_EZEIT,
  EKES.MENGE AS QuantityAsPerVendorConfirmation_MENGE,
  EKES.DABMG AS QuantityReduced__mrp___DABMG,
  EKES.ESTKZ AS CreationIndicator_VendorConfirmation_ESTKZ,
  EKES.LOEKZ AS VendorConfirmationDeletionIndicator_LOEKZ,
  EKES.KZDIS AS Indicator_ConfirmationIsRelevantToMaterialsPlanning_KZDIS,
  EKES.XBLNR AS ReferenceDocumentNumber__forDependenciesSeeLongText___XBLNR,
  EKES.VBELN AS Delivery_VBELN, EKES.VBELP AS DeliveryItem_VBELP,
  EKES.MPROF AS MfrPartProfile_MPROF,
  EKES.EMATN AS MaterialNumberCorrespondingToManufacturerPartNumber_EMATN,
  EKES.MAHNZ AS NumberOfRemindersexpediters_MAHNZ, EKES.CHARG AS BatchNumber_CHARG,
  EKES.UECHA AS HigherLevelItemOfBatchSplitItem_UECHA,
  EKES.REF_ETENS AS SequentialNumberOfVendorConfirmation_REF_ETENS,
  EKES.IMWRK AS DeliveryHasStatusinPlant_IMWRK, EKES.VBELN_ST AS Delivery_VBELN_ST,
  EKES.VBELP_ST AS DeliveryItem_VBELP_ST,
  EKES.HANDOVERDATE AS HandoverDateAtTheHandoverLocation_HANDOVERDATE,
  EKES.HANDOVERTIME AS HandoverTimeAtTheHandoverLocation_HANDOVERTIME,
  EKES.SGT_SCAT AS StockSegment_SGT_SCAT,
  EKES.FSH_SALLOC_QTY AS AllocatedStockQuantity_FSH_SALLOC_QTY,
  vendor.AccountNumberOfVendorOrCreditor_LIFNR,
  vendor.NAME1, vendor.NAME2, docs.TermsPaymentKey_ZTERM, docs.DiscountDays1_ZBD1T
FROM `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.ekes` AS EKES
INNER JOIN `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.PurchaseDocuments` AS docs
  ON EKES.MANDT = docs.Client_MANDT
    AND EKES.EBELN = docs.DocumentNumber_EBELN
    AND docs.Item_EBELP = EKES.EBELP
INNER JOIN `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.VendorsMD` AS vendor
  ON vendor.Client_MANDT = docs.Client_MANDT
    AND vendor.AccountNumberOfVendorOrCreditor_LIFNR = docs.VendorAccountNumber_LIFNR
