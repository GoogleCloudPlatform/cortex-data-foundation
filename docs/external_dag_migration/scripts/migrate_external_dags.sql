{% if 'holiday_calendar' in migrate_list -%}
###### Holiday Calendar Tables Migration ######

CREATE OR REPLACE TABLE `{{ project_id_src }}.{{ k9_datasets_processing }}.holiday_calendar`
CLONE `{{ project_id_src }}.{{ dataset_cdc_processed }}.holiday_calendar`;

{% endif -%}

{% if 'trends' in migrate_list -%}
###### Trends Tables Migration ######

CREATE OR REPLACE TABLE `{{ project_id_src }}.{{ k9_datasets_processing }}.trends`
CLONE `{{ project_id_src }}.{{ dataset_cdc_processed }}.trends`;

{% endif -%}

{% if 'weather' in migrate_list -%}
###### Weather Tables Migration ######

CREATE OR REPLACE TABLE `{{ project_id_src }}.{{ k9_datasets_processing }}.postcode`
CLONE `{{ project_id_src }}.{{ dataset_cdc_processed }}.postcode`;
CREATE OR REPLACE TABLE `{{ project_id_src }}.{{ k9_datasets_processing }}.weather_daily`
CLONE `{{ project_id_src }}.{{ dataset_cdc_processed }}.weather_daily`;
CREATE OR REPLACE TABLE `{{ project_id_src }}.{{ k9_datasets_processing }}.weather_weekly`
CLONE `{{ project_id_src }}.{{ dataset_cdc_processed }}.weather_weekly`;

{% endif -%}

{% if 'currency_conversion' in migrate_list -%}
###### Currency Conversion Tables Migration ######

CREATE OR REPLACE TABLE `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.currency_conversion`
CLONE `{{ project_id_src }}.{{ dataset_cdc_processed }}.currency_conversion`;
CREATE OR REPLACE TABLE `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.currency_decimal`
CLONE `{{ project_id_src }}.{{ dataset_cdc_processed }}.currency_decimal`;

{% endif -%}

{% if 'inventory_snapshots' in migrate_list -%}

###### Inventory Snapshots Tables Migration ######

CREATE OR REPLACE TABLE `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.SlowMovingThreshold`
CLONE `{{ project_id_src }}.{{ dataset_cdc_processed }}.slow_moving_threshold`;

CREATE OR REPLACE TABLE `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.StockCharacteristicsConfig`
CLONE `{{ project_id_src }}.{{ dataset_cdc_processed }}.stock_characteristics_config`;

CREATE OR REPLACE TABLE `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.stock_monthly_snapshots`
CLONE `{{ project_id_src }}.{{ dataset_cdc_processed }}.stock_monthly_snapshots`;
CREATE OR REPLACE TABLE `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.monthly_inventory_aggregation`
CLONE `{{ project_id_src }}.{{ dataset_cdc_processed }}.monthly_inventory_aggregation`;

CREATE OR REPLACE TABLE `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.stock_weekly_snapshots`
CLONE `{{ project_id_src }}.{{ dataset_cdc_processed }}.stock_weekly_snapshots`;
CREATE OR REPLACE TABLE `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.weekly_inventory_aggregation`
CLONE `{{ project_id_src }}.{{ dataset_cdc_processed }}.weekly_inventory_aggregation`;

{% endif -%}

{% if 'prod_hierarchy_texts' in migrate_list -%}
###### Product Hierarchy Texts Tables Migration ######

CREATE OR REPLACE TABLE `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.prod_hierarchy_texts`
CLONE `{{ project_id_src }}.{{ dataset_cdc_processed }}.prod_hierarchy_texts`;

{% endif -%}

{% if 'hier_reader' in migrate_list -%}
###### Hierarchy Reader Tables Migration ######

CREATE OR REPLACE TABLE `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.csks_hier`
CLONE `{{ project_id_src }}.{{ dataset_cdc_processed }}.csks_hier`;
CREATE OR REPLACE TABLE `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.cepc_hier`
CLONE `{{ project_id_src }}.{{ dataset_cdc_processed }}.cepc_hier`;

{% endif -%}
