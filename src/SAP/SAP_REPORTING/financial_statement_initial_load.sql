--## CORTEX-CUSTOMER: Update the start and end date for initial load as per your requirement
CALL `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.FinancialStatement`(
  {% if sql_flavour == 'ecc' -%}
  (SELECT MIN(budat) FROM `{{ project_id_src }}.{{ dataset_cdc_processed }}.bkpf`),
  {% else %}
  (SELECT MIN(budat) FROM  `{{ project_id_src }}.{{ dataset_cdc_processed }}.acdoca`),
  {% endif %}
  CURRENT_DATE());
