##########################################################
# WARNING: THIS SCRIPT PERMANENTLY DROPS YOUR OLD TABLES #
#                                                        #
#               !!!THIS CANNOT BE UNDONE!!!              #
##########################################################

{% if 'holiday_calendar' in migrate_list -%}
###### Holiday Calendar Tables Removal ######

DROP TABLE `{{ project_id_src }}.{{ dataset_cdc_processed }}.holiday_calendar`;

{% endif -%}

{% if 'trends' in migrate_list -%}
###### Trends Tables Removal ######

DROP TABLE `{{ project_id_src }}.{{ dataset_cdc_processed }}.trends`;

{% endif -%}

{% if 'weather' in migrate_list -%}
###### Weather Tables Removal ######

DROP TABLE `{{ project_id_src }}.{{ dataset_cdc_processed }}.postcode`;
DROP TABLE `{{ project_id_src }}.{{ dataset_cdc_processed }}.weather_daily`;
DROP TABLE `{{ project_id_src }}.{{ dataset_cdc_processed }}.weather_weekly`;

{% endif -%}

{% if 'currency_conversion' in migrate_list -%}
###### Currency Conversion Tables Removal ######

DROP TABLE `{{ project_id_src }}.{{ dataset_cdc_processed }}.currency_conversion`;
DROP TABLE `{{ project_id_src }}.{{ dataset_cdc_processed }}.currency_decimal`;

{% endif -%}

{% if 'inventory_snapshots' in migrate_list -%}

###### Inventory Snapshots Tables Removal ######

DROP TABLE `{{ project_id_src }}.{{ dataset_cdc_processed }}.slow_moving_threshold`;

DROP TABLE `{{ project_id_src }}.{{ dataset_cdc_processed }}.stock_characteristics_config`;

DROP TABLE `{{ project_id_src }}.{{ dataset_cdc_processed }}.stock_monthly_snapshots`;
DROP TABLE `{{ project_id_src }}.{{ dataset_cdc_processed }}.monthly_inventory_aggregation`;

DROP TABLE `{{ project_id_src }}.{{ dataset_cdc_processed }}.stock_weekly_snapshots`;
DROP TABLE `{{ project_id_src }}.{{ dataset_cdc_processed }}.weekly_inventory_aggregation`;

{% endif -%}

{% if 'prod_hierarchy_texts' in migrate_list -%}
###### Product Hierarchy Texts Tables Removal ######

DROP TABLE `{{ project_id_src }}.{{ dataset_cdc_processed }}.prod_hierarchy_texts`;

{% endif -%}

{% if 'hier_reader' in migrate_list -%}
###### Hierarchy Reader Tables Removal ######

DROP TABLE `{{ project_id_src }}.{{ dataset_cdc_processed }}.csks_hier`;
DROP TABLE `{{ project_id_src }}.{{ dataset_cdc_processed }}.cepc_hier`;

{% endif -%}
