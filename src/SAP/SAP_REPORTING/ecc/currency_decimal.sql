((
  SELECT
    currdec
  FROM `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.tcurx`
  WHERE currkey = ip_curr
))
