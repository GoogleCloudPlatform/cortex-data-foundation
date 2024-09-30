SELECT
  Salesfull.MaterialNumber_MATNR,
  Salesfull.MaterialText_MAKTX,
  Salesfull.DeliveredUoM_MEINS,
  Salesfull.DeliveredQty,
  conv.val_out AS conv_factor,
  if( Salesfull.DeliveredUoM_MEINS IN ('G', 'LB', 'KG'), 'LB', Salesfull.DeliveredUoM_MEINS ) AS to_conv,
  ( Salesfull.DeliveredQty * conv.val_out ) AS converted
FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.SalesFulfillment` AS Salesfull
INNER JOIN `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.UoMConversionUtil` AS conv
  ON Salesfull.Client_MANDT = conv.mandt AND Salesfull.DeliveredUoM_MEINS = conv.unit_from
    AND conv.unit_to = if( Salesfull.DeliveredUoM_MEINS IN ('G', 'LB', 'KG'), 'LB', Salesfull.DeliveredUoM_MEINS )
WHERE Salesfull.DeliveredUoM_MEINS IN ('G', 'KG')
