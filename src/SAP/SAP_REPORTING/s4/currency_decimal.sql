((
  SELECT
    currdec
  FROM `{{ project_id_src }}.{{ dataset_cdc_processed_s4 }}.tcurx`
  WHERE currkey = ip_curr
))
