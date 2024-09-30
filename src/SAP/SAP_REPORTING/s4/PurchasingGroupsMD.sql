SELECT
  t024.MANDT AS Client_MANDT,
  t024.EKGRP AS PurchasingGroup_EKGRP,
  t024.EKNAM AS PurchasingGroupText_EKNAM
-- t024.EKTEL AS TelephoneNumberPurchasingGroup_EKTEL,
-- t024.LDEST AS Spool_LDEST,
-- t024.TELFX AS FaxNumberPurchasingGroup_TELFX,
-- t024.TEL_NUMBER AS TelephoneNo_TEL_NUMBER,
-- t024.TEL_EXTENS AS TelephoneNoExtension_TEL_EXTENS,
-- t024.SMTP_ADDR AS EMailAddress_SMTP_ADDR
FROM
  `{{ project_id_src }}.{{ dataset_cdc_processed_s4 }}.t024` AS t024
  