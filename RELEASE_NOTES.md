## September 2023 - Release 5.2
*   **LiveRamp for Marketing - NEW!🎉:** New integration templates and data models to enable identity resolution by [integrating with LiveRamp](https://docs.liveramp.com/identity/en/identity-resolution.html).
*   **SAP Reusable DAGs are now deployed via the K9 deployment mechanism** so they now behave like other cross-workload DAGs. This applies to all DAGs in the [`external_dag`](https://github.com/GoogleCloudPlatform/cortex-reporting/tree/cca3c1c86047d5ea387c50dc5e4eccdb27141775/external_dag) directory. Note that these DAGs no longer search for SQL files from `bq_data_replication` directory. If you have modified the SQLs for them, Please follow these steps to migrate:
    1. Back up any SQL file(s) you have modified from `gs://<your_airflow_bucket>/data/bq_data_replication`.
    2. If any of the relevant DAG(s) are currently running, pause them in Airflow.
    3. Deploy Cortex Data Foundations v5.2.
    4. Copy your modified SQL file into the deployment directory, which by default will be `gs://<your_deployment_bucket>/dags/sap/reporting/<dag_name>`.
    5. Copy `/dags` directory from the deployment bucket into your Airflow bucket. Then, wait for Airflow to refresh, and unpause the DAGs in Airflow.
    6. Once you are good with the result, remove migrated file(s) from `gs://<your_airflow_bucket>/data/bq_data_replication`.
*   **Metadata for SAP and TikTok:** New experimental annotations feature for SAP artifacts. The deployer will now create views with descriptions for views and their fields.
*   **Support for new regions**: New supported regions incorporated into test harness.
*   Fix for set hierarchy reader in SAP, generating a node with no associated master data for the header. **Note** the default name of the generated DAG has changed. Please follow the procedure described above for SAP reusable DAGs for migration.

## August 2023 - Release 5.1
*   **TikTok for Marketing - NEW!🎉:** New integration templates and data models for TikTok to measure paid media campaign performance insights, through impressions and reach. See the [ERD](images/erd_tiktok.png) (or [PDF](docs/erd_tiktok.pdf))
*   Placeholders for [K9 views](https://github.com/GoogleCloudPlatform/cortex-data-foundation#configure-k9-deployments) are now created for each workload: Reusable models like time dimension are required for the deployment of individual workloads, like SAP and Salesforce.com. If the submodules are executed independently (e.g., only SAP reporting), these dependencies will not be deployed. The placeholders create these dependencies so the submodules remain autonomous.
*   **Ask for support**: New experimental feature to help gather necessary data to request support after a failed deployment. This feature will gather the logs and the configuration so it can be sent to cortex-support@google.com more easily.
*   Added option to calculate doubtful receivables and days in arrear as positive amount in AccountingDocuments. This is commented out and will be the default behavior in the next release.
*   Pull [request #9](https://github.com/GoogleCloudPlatform/cortex-reporting/pull/9) from SAP Reporting, adding safe divide for Fill rate Percent.

## August 2023 - Release 5.0.1 (bug fix)
*   Enhanced validation of permissions and assets existence as first step in deployment. The former `Mando Checker` is now executed at the beginning of the validation process. The deployer will attempt to create, write and delete temporary files and datasets in the targets to validate permissions.
*   README: minor tweaks and some image refreshes.
*   K9: Move pre-processing or post-processing configuration to individual manifests to simplify execution.
*   Test harness: Restore behavior to ignore a table in Raw if it exists, and it has no records, and `testData` is set to True.
*   Materializer: Restore behavior to attempt to build all views sequentially when turbo mode is set to False.

## July 2023 - Release 5.0
*   **New Marketing models:** A new repository, [Cortex for Marketing](https://github.com/GoogleCloudPlatform/cortex-marketing), has been added to the Data Foundation. This repository starts with data ingestion and data processing DAGs for Cloud Composer and Dataflow and predefined data models for Google Ads and Campaign Manager 360. This accelerates Ads reporting scenarios like keyword performance insights across campaigns and audience insights across display campaigns directly in BigQuery. Please check the [ERDs in the docs](https://github.com/GoogleCloudPlatform/cortex-data-foundation/tree/main/docs) folder.
*   **Quick demo deployment**: For those looking for a frictionless demo deployment experience, we have created a button that will guide them through an automated process to create sample datasets with test data and enable APIs and permissions. This is available in the [README](https://github.com/GoogleCloudPlatform/cortex-data-foundation/tree/main#quick-demo-setup).
*   **Cross-workload and source reusable models** (a.k.a, **K9**, where the DAGs 🐶 live): Reusable models, such as time dimensions or external sources like Weather, are now available through a deployment mechanism that is shared across all datasets. This allows for cross workload reporting, like joining SAP and Google Ads data too. Some DAGs that used to be deployed as workload-specific, like currency conversion to SAP, is now migrated to the reporting dataset. Please check the migration guide in [the docs folder](https://github.com/GoogleCloudPlatform/cortex-data-foundation/tree/main/docs/external_dag_migration). Weather and Trends extraction DAGs are now disabled by default. More information about the K9 module in the [README](https://github.com/GoogleCloudPlatform/cortex-data-foundation/tree/main#configure-k9-deployments).
*   [Optional materialization and performance optimization](https://github.com/GoogleCloudPlatform/cortex-data-foundation/tree/main#performance-optimization-for-reporting-views) functionality is enabled for all modules and views. This impacts the SQL templates previously containing a `CREATE` statement. 🚨🚨 We strongly recommend checking the new default configurations as this will attempt to replace some views with tables. 🚨🚨 You will need to delete the existing views in Reporting before you can deploy a table with the same name. See the documentation for more details.
*   [CATGAP](https://github.com/GoogleCloudPlatform/cortex-data-foundation/tree/main/src/k9/src/catgap) has moved to K9. By default, the deployment is disabled.
*   The test harness data has now moved from buckets into BigQuery datasets available in all relevant regions. Test harness data is still not provided with any warranty of quality, but this change simplifies and speeds up the deployment for those exploring the framework.
*   Substitutions are removed, except for Logs GCS bucket. Configuration files are now normalized into `config.json`. Legacy Dotenv files have been removed. Check [the updated documentation](https://github.com/GoogleCloudPlatform/cortex-data-foundation/tree/main#configure-deployment) to understand parameters.
    *   deployCDC is now specific to each workload
    *   generateExtraData is now removed, the control of this deployment is owned by K9.
### **SAP**
- SAP-specific DAGs, like currency conversion, inventory management or hierarchy reader, were moved to the reporting module and are executed by default. This prevents downstream errors that used to happen when _GEN_EXT was flagged false.
- Please check the [reporting configuration for materialization](https://github.com/GoogleCloudPlatform/cortex-reporting/blob/main/reporting_settings_ecc.yaml), these have been modified to better match customer expectations but may not apply to your business at all.
*   UNION deployment logic has been removed, but supported views will continue to have the UNION template until further notice.
- The reporting module relies on the time dimension deployment in K9, it can be executed manually if deploying from the [cortex-reporting submodule](https://github.com/GoogleCloudPlatform/cortex-reporting/).

### **Salesforce**
- New currency conversion for Salesforce. Since these are optional components for Salesforce deployments, they can be created through the placeholder settings. If you don't have the currency types and conversion in Salesforce, comment out or remove the calls in `SFDC/config/ingestion_settings.yaml`. The configuration in `cdc_placeholder_settings.yaml` will take care of the dependencies in the reporting views.
- The reporting module relies on the time dimension deployment in K9, it can be executed manually if deploying from the [Salesforce submodule](https://github.com/GoogleCloudPlatform/cortex-salesforce).

### Known issues and limitations
-   SAP Reporting and Salesforce Reporting requires execution of K9 pre-processing if used as a separate submodule.
-   ML Models repository still allows for substitutions.
-   CM360 and Ads integration scripts are in `Experimental` mode while we gather early feedback from customers on usage. We would like to hear what you think through GitHub issues.
-   Google Ads DAG is supported on Airflow v2 only.

## March 2023 - Release 4.2
*   **New Supply Chain and Finance views**: These views support KPIs for Inventory management, Vendor Performance, Accounts Payable and Spend Analysis. Please check the updated [ECC ERD](https://github.com/GoogleCloudPlatform/cortex-data-foundation/blob/main/docs/erd_ecc.pdf) and the [S/4 ERD](https://github.com/GoogleCloudPlatform/cortex-data-foundation/blob/main/docs/erd_s4.pdf) The new objects are:
    -  **Reusable DAG scripts for inventory snapshots**: ([SAP_CDC/external_dag/inventory_snapshots](https://github.com/GoogleCloudPlatform/cortex-dag-generator/tree/main/src/external_dag/inventory_snapshots)): Initializer and periodic update for weekly and monthly inventory snapshots and aggregations, slow moving threshold, and stock characteristic configuration. Please check for CORTEX-CUSTOMER comments for potential updates needed for custom thresholds and material movements. These scripts are in PREVIEW and will be moved into another structure in deployment in the next major release. ⚠️⚠️ These scripts fill the base tables and need to be scheduled for the reporting views to function properly.⚠️⚠️
    -   **New views for reporting**: AccountsPayableOverview, DaysPayableOutstanding, InventoryByPlant, InventoryKeyMetrics, MaterialLedger, MaterialMovementTypesMD
    MaterialsBatchMD, POScheduleLine, PurchaseDocumentsHistory, SalesOrderStatus, SlowMovingThreshold, StockCharacteristicsConfig, StockInHand, StockMonthlySnapshots, StockWeeklySnapshots.
    -   **Overview views**: VendorLeadTimeOverview, VendorPerformanceOverview. These views show the reporting logic used in Looker in case you want to replicate them in another tool or a microservice. These views are not deployed by default.
*   **Materializer PREVIEW** 🫶: By default, the new views that will require a lot of computation are now deployed as materializing DAGs. This helps improve performance and reduce costs and is configurable. This configuration is optional and the generated SQL can be ported into a scheduler of choice if you are not using Cloud Composer or Airflow. The next major release will follow this deployment approach for all views. See the [documentation](https://github.com/GoogleCloudPlatform/cortex-data-foundation/blob/main/README.md#optional-sap-only_performance-optimization-for-reporting-views) for more details.
*   **Cortex Analytics Templates - Google Ads Pipelines (CATGAP)** 🐈: This new experimental feature uses Natural Language Processing machine learning models to intelligently map product categories from Google Ads to SAP's product hierarchy. We'd love to know what you think. CATGAP is not deployed by default. Please check [the documentation](https://github.com/GoogleCloudPlatform/cortex-data-foundation/tree/main/src/k9/src/catgap) for details and further setup.
*   ⚠️⚠️NOTE⚠️⚠️ Reporting views that expect parameters from currencies in config/config.json will produce the same result as many times as currencies are set as targets. Currency conversion in newer views is no longer commented out for convenience. However, the target currency needs to be passed as a filter from the reporting view. 🙏🙏 Please check for `CORTEX-CUSTOMER` comments for specific guidance if you deploy the data foundation with more than one currency.🙏🙏
*   Parameters that control runtime (e.g, "DEPLOY_SAP", "DEPLOY_SFDC", "DEPLOY_CDC" and "TEST_DATA") are now also read from the config file. These are still defaulted in cloudbuild.yaml substitutions. If you want to use the values in the file, the [substitutions section in cloudbuild.yaml](https://github.com/GoogleCloudPlatform/cortex-data-foundation/blob/main/cloudbuild.yaml#L107) needs to be commented out. Substitutions from the command line will be phased out.
*   Compatibility for Airflow v1 and v2 updated for currency_conversion DAG.
*   Addressing [SFDC issue #2](https://github.com/GoogleCloudPlatform/cortex-salesforce/issues/2)

### Known Issues and Limitations
*   If using the SAP Reporting submodule as standalone for deployment, sap_config.json needs to be manually generated from config.env for the materializer to read as input. Dotenv files will be removed in the next major release, and this will be addressed for SAP submodules.
*   New views are not released for `UNION` mode. If you would like to see them work in `UNION` mode, please reach out through an issue or to cortex-framework@google.com. 🙏We would like to better understand the use case and if this experimental feature is useful so we maintain compatibility in new views.🙏

## February 2023 - Release 4.1
*   More options for Salesforce integration with provided scripts or external tools: End-to-end integration from Salesforce APIs (provided out-of-the-box), structure mapping with no change data capture processing and optional CDC and mapping generation scripts. See the [README](https://github.com/GoogleCloudPlatform/cortex-data-foundation#loading-salesforce-data-into-bigquery) for more instructions
*   Option for `sequential` deployment (TURBO=false) for SFDC incorporated.
*   Salesforce integration now updates the RAW tables with changes, merging changes on landing. This removes the need for additional CDC processing. Deltas are captured using SystemModstamp provided by Salesforce APIs. See details in README.
*   `IsArchived` flag is removed from CDC processing for Salesforce.
*   Errors originating from gsutil steps in cloudbuild.sfdc.yaml not finding files to copy are now caught and surfaced gracefully.
*   Removing some `substitution` defaults (e.g., LOCATION) from cloudbuild.yaml file so all configurations are either passed from the command line or read from `config/config.json`. 🚨🔪🚨[TL;DR submit sample call](https://github.com/GoogleCloudPlatform/cortex-data-foundation#tldr-for-setup) was updated to default these flags. These parameters will be removed from subsitution defaults in future releases. 🚨🔪🚨
*   Detecting version for Airflow in DAG templates to use updated libraries for Airflow v2 in SAP and Salesforce. This remvoes some deprecation warnings but may need additional libraries installed in your Airflow instance.
*   Fix for test harness data not loading in an intended location when the location is not passed as a substitution.
*   Checking existence of DAG-generated files before attempting to copy with `gsutil` to avoid errors.
*   **NOTE**: 🚨🚨Structure of RAW landed tables has changed🚨🚨 to not require additional DAG processing. Please check the documentation on mapping and use of the new extraction process before upgrading to avoid disruption. We recommend pausing the replication, making abackup copy of any loaded tables, modifying the schemata of existing loaded tables and testing the new DAGs work with the new columns. The DAG will start fetching records using the last SystemModstamp present in RAW.
## December 2022 - Release 4.0
*   **🎆Welcome Salesforce.com to Cortex Data Foundation🎆🐈🦄**: New [module for Salesforce](https://github.com/GoogleCloudPlatform/cortex-salesforce), to be implemented alongside the SAP models or on its own. The module includes optional integration and CDC scripts and reporting views for Leads Capture & Conversion, Opportunity Trends & Pipeline, Sales Activity and Engagement, Case Overview and Trends, Case Management & Resolution, Accounts with Cases. See the [entity-relationship diagram](images/erd_sfdc.png) for a list of tables and views. Check the [Looker repository](https://github.com/looker-open-source/block-cortex-salesforce) for sample dashboards.
*   New configuration file (`config/config.json`) for deployment parameters. We maintain backward compatibility with the existing `gcloud builds submit` command parameters. Enhanced parameters like SAP UNION datasets and those for Salesforce, can be configured in config.json. See the [instructions for configuration in the README](https://github.com/GoogleCloudPlatform/cortex-data-foundation#configure-the-deployment-file) for more details. This file format will replace the `.env` format for SAP_REPORTING in the next releases.
*   New preview models for SAP Finance: AccountingDocumentsReceivable, AccountsPayable, AccountsPayableTurnover, CashDiscountUtilization, CurrencyConversion (exposing table processed by DAG), VendorPerformance, VendorConfig (dependant on TVARVC values). While PREVIEW views allow us to gain early feedback, they are also subject to change.
*   New split views for SAP Order-To-Cash: SalesOrderPricing, PricingConditions. See the [updated Looker reports](https://github.com/looker-open-source/block-cortex-sap) to make use of the split models.

### Known issues and limitations
*   Salesforce integration DAGs have been tested in Airflow 1.0. Airflow 2.0 may require library updates or use of backwards compatible libraries to be tested and confirmed in the next release.
*   Finance views for SAP are good candidates for partial or total materialization. Check BigQuery's execution details to identify opportunities to create materialization processes and further optimizations that fit your data best.
## November 2022 - Release 3.1
*   **New partitioning and clustering configuration for CDC deployment:** Configurable partitioning and clustering on deployment of CDC landing tables and scripts. See example in [cdc_settings.yaml](https://github.com/GoogleCloudPlatform/cortex-dag-generator/blob/main/cdc_settings.yaml) and [the README instructions](https://github.com/GoogleCloudPlatform/cortex-data-foundation#performance-optimization-for-cdc-tables).
*   **New date dimension 📅** generated from external DAGs through `_GEN_EXT=true` to allow for more flexibility in reporting. This table has been incorporated into views POSchedule, MaterialsValuation, PurchaseDocuments, Deliveries, Billing, SalesOrderScheduleLine, AccountingDocuments, InvoiceDocuments_Flow, POOrderHistory and SalesOrders_V2. The straucture of the table will be generated without data if `_GEN_EXT=false`.
  **Note:** If you do not want to execute the generation of other DAGs for external sources, remove them from the list in the [generation script](https://github.com/GoogleCloudPlatform/cortex-dag-generator/blob/main/generate_external_dags.sh#L4).
*   Performance improvements to AccountingDocuments, InvoiceDocuments_Flow,Billing, Deliveries, MaterialsValuation, PurchaseDocuments and POOrderHistory, SalesOrders_V2 when using currency conversion generated from DAG materializing results (see CORTEX-CUSTOMER tags and differences when merging).
*   New preview views modularizing functionality from SalesOrders_V2: OneTouchOrder, SalesOrderHeaderStatus, SalesOrderScheduleLine, SalesOrderPartnerFunction, SalesOrderDetails_SAMPLE
*   Replacing ECC or S4 specific functions (e.g., fiscal_Case_ecc, fiscal_case_s4) with non-specific (e.g., fiscal_case). SQL flavor-specific functions will be commented out from generation in the next release.

## August 2022 - Release 3.0
*   **💣!!New submodule structure!!💣** We are making room for more exciting features, so all SAP-related submodules ([SAP_CDC ](https://github.com/GoogleCloudPlatform/cortex-dag-generator), [SAP_REPORTING ](https://github.com/GoogleCloudPlatform/cortex-reporting/), [SAP_ML_MODELS ](https://github.com/GoogleCloudPlatform/cortex-ml-models)) have been moved under the directory `src/SAP/`. To upgrade, we recommend copying your customized views into the /ecc or /s4 directories respectively in your repository so you can see the changes after pulling this remote and using the difftool of your choice. Merge your changes with the new releases in the submodules first, and commit them into your forks or merging repositories. If getting a conflict, clone the Data Foundation, adjust the `.gitmodules` file manually, remove the directories for the old submodules (`src/SAP_CDC`, `src/views/SAP_REPORTING` and `src/views/SAP_ML_MODELS`) and execute `git modules update --remote`. See the `Recommendations for upgrades` under the `/docs` directory.
*   Submodules now execute completely independently. SAP's ML Models and SAP Reporting submodules now have their own `yaml` file for independent execution.
*   New experimental `UNION` logic! Available if your SAP ECC and S/4 systems do not share the same MANDT (a fix for this is coming soon). All views are now moved to `/ecc` or `/s4` respectively. This alters the templates of all of the views and requires two sets of datasets (e.g.: one `raw landing` for ECC, one for S/4). These datasets can be passed to the deployment in the file `sap_config.env`. See the documentation for more information about the `UNION` feature.
*   SAP Reporting can now be deployed using a configuration file (`sap_config.env`) instead of parameters from the `gcloud builds submit --substitutions` command. Parameters received from the substitutions override configurations in the file to maintain backwards compatibility. This affects all the cloudbuild*.yaml files.
*   New simplified view for Sales Orders (_SalesOrders_V2_) with less underlying joins for use cases that do not require them.
*   New experimental views for a collaboration with [C3.ai](https://c3.ai/). These views are considered `experimental` as we increase the functionality around and on top of them in collaboration with C3.ai. The output of experimental views may be subject to change until they are considered stable. Experimental views are flagged with an `## EXPERIMENTAL` comment.
*   **Turbo mode for SAP Reporting**: The file `sap_config.env` contains a variable called `TURBO`. If set to `true`, the cloudbuild*.yaml file will be generated dynamically from the dependencies file. This reduces the deployment time from ~1h to 7 minutes. We recommend using TURBO = `false` for first deployment to spot differnces in the source tables all in the same run.
*   Candidates for enhancement points within the code, that is portiions where customers should consider incorporating changes or applying extensions, are flagged with a comment `## CORTEX-CUSTOMER`.
*   Templatized currency and language for views that require the values to be hardcoded for better performance. See the documentation on how to configure these values.
*   New non-changing views created as tables by default. Sources that are unlikely to ever change (e.g.: material document types, valuation areas) are now created as tables -instead of views- by default. See the updated Entity-Relationship Diagram to spot them. This behavior will be back-ported to other pre-existing views with a future feature for default materialization.
*   New views: WeatherDaily, SalesOrders_V2, DeliveryBlockingReasonsMD, BillingBlockingReasonsMD, StorageLocationsMD, PurchaseDocumentTypesMD, MovementTypesMD, ReasonForMovementTypesMD, SpecialStocksMD, PurchasingOrganizationsMD, BatchesMD,  MaterialPlantsMD, ProductionOrders, PurchasingGroupsMD, MaterialsValuation, MaterialsMovement, MaterialTypesMD, BillOfMaterialsMD, DivisionsMD, ValuationAreasMD.
*   New DAG for currency conversion and currency decimal shifting. This will be improved to run a smaller range of conversion dates in a future release.
*   When test data is set to true, the dynamically generated MERGE operation will now copy the test data from the raw landing dataset into the CDC dataset. Test data in CDC dataset is no longer copied as it is from the raw landing. Test data is still meant to provide a base for specific demo scenarios and is still not guaranteed to be a reliable source with any quality or referential integrity.
*   We have started marking releases with tags. This release is now [v3.0](https://github.com/GoogleCloudPlatform/cortex-data-foundation/releases/tag/v3.0)
*   Views for ECC are now matched in S4. See S/4HANA ERD for more information.
*   New guide with [recommendations for upgrades](https://github.com/GoogleCloudPlatform/cortex-data-foundation/blob/main/docs/upgrade_recommendations/upgrade_recommendations.md) in the `docs` directory.

### Upgrade Recommendations
Since most views have at least a minor change in their dataset jinja templates (i.e., going from `{{ dataset_cdc_processed }}` to `{{ dataset_cdc_processed_ecc }}` ), we recommend the following:
1. If you haven't already, try to identify customizations in your SQLs with a comment so they are visible in the diff.
2. Create a branch for merging as outlined in the recommendations for upgrade for each submodule. Start with the submodules and leave the Data Foundation supermodule for last.
3. In the reporting submodule, copy all the SQLs with `CREATE OR REPLACE` followed by actual SELECT statements for views (e.g., `CustomersMD`) to the folder of your system version (`ecc` or `s4`), **except those that are not already there** (e.g., `ecc/CompaniesMD`). For example, if you have deployed and customized views for ECC, copy all the SQL files into `cortex-reporting/ecc` except for views that already have a .sql file in ecc/. You can use a command like ```cp -n *.sql ./ecc``` for this
4. Merge the changes from the remote branch with the upgrade into the temp branch. Once conflicts are solved, merge the merged branch into the main branch for your submodules.
5. Once you have pushed the merged changes into your repository, the merge process for the submodules in cortex-data-foundation is expected to produce a message like: ```src/views/SAP_REPORTING deleted in github-release3 and modified in HEAD.  Version HEAD of src/views/SAP_REPORTING left in tree.``` . Fix conflicts in the `.gitmodules` file. Valid paths for submodules now start with `src/SAP/`.
6. Add and commit the changes in the `.gitmodules` file.
7. Remove the remaining directories for the previous structure (`src/views/SAP_REPORTING`, `src/views/SAP_ML_MODELS`, `src/SAP_CDC`)




### Known issues
*   Currency conversion DAG processes all available dates each time. This will be addressed in a future release.
*   If the MANDT for the ECC and S/4HANA systems is the same, the UNION mode is not supported. This will be addressed in a future release.
*   Different source Reporting projects for ECC and S/4 in UNION mode are not supported.
*   ML deployment fails wtih a templating error if executed after UNION option from the Data Foundation. This error is avoided if UNION is executed from SAP REPORTING submodule. The error has no effect in the deployment of the reporting views.


## June 2022 - Release 2.2
*   New external datasets available: Holidays, Weather, Trends and product hierarchy flattener to feed the Trends search terms.
*   Region-specific deployments with test data are now possible. Supported locations are: US and EU (multilocations). Supported regions: us-central1, us-west4, us-west2, northamerica-northeast1, northamerica-northeast2, us-east4, us-west1, us-west3, southamerica-east1, southamerica-west1, us-east1, asia-south2, asia-east2, asia-southeast2, australia-southeast2, asia-south1, asia-northeast2, asia-northeast3, asia-southeast1, australia-southeast1, asia-east1, asia-northeast1, europe-west1, europe-north1, europe-west3, europe-west2, europe-west4, europe-central2, europe-west6. More information on supported regions and limitations of regional datasets in BigQuery [here](https://cloud.google.com/bigquery/docs/locations).
*   Addressing records with the same key and operation flag sent more than once in different chunks into raw landing
*   Escaping of table and column names in DAG and real-time view generation to avoid issues with reserved words ([issue #7](https://github.com/GoogleCloudPlatform/cortex-data-foundation/issues/7))
*   Minor tweak to CustomersMD: Coalesce address between customer master data and latest record in ADRC.
*   Minor correction to OrderToCash: Use CDC dataset as source for VBRP

### Known issues
*   Analytics hub is [currently only supported in the EU and US regions](https://cloud.google.com/bigquery/docs/analytics-hub-introduction). If deploying the Weather DAG in a specific location where the source linked datasets are not available, you may need to resort to another source for weather data or create a scheduled query to feed a copy in the same location, and use a transfer service to copy the records into a table in the desired location.


## May 2022 - Release 2.1.1 (minor update)

*   Fixed AddressesMD date format to '9999-12-31'
*   Merged original 800 and 200 demo client tables into test harness for ECC (now it has both 800 and 100). This client was mostly used for demos.
*   Commenting out deployment of Product Recommender ML model as it requires specific reservations and a sample dataset that is only offered in the US region.
*   Addressing [issue #4](https://github.com/GoogleCloudPlatform/cortex-data-foundation/issues/4) (Thanks, Ludovic Dessemon!)
*   Ignoring .INCLUDE when generating DAGs and Runtime views
*   Fixed RUNTIME view logic generation affecting keys with deletion
*   Thanks to Andres Mora Achurra from Camanchaca for the feedback :)

New Looker blocks available here replacing the older blocks: [https://marketplace.looker.com/marketplace/detail/cortex-finance](https://marketplace.looker.com/marketplace/detail/cortex-finance)


## March 2022 - Release 2.1

### Data Foundation

This release brings the following changes Cortex Data Foundation.

**Features & Improvements:**

* Templatized deployment and SQL Flavour for views, `setting.yaml `file and test harness to deploy either ECC or S4:
    1. New split source of data with `MANDT = 050` for ECC and `MANDT = 100` for S/4. These will populate automatically based on the `_SQL_FLAVOUR` parameter in the build (default to 'ECC')
    2. Original dataset from ECC and S/4 mixed still present in the source bucket (`MANDT = 800`)
    3. Views `FixedAssets`, `SDStatus_Items`, `GLDocumentsHdr`, `BSID` and `BSAD` implementations, `InvoiceDocuments_flow`,`DeliveriesStatus_PerSalesOrg`, `SalesFulfillment_perOrder`, `SalesFulfillment`, `UoMUsage_SAMPLE` will be adapted to S/4 with source tables in a next minor release.
* Additional views for ECC and S/4 with Accounts Receivables and On-Time/In-Full analysis:
    -  Billing
    - OrderToCash
    - AccountingDocumentsReceivable
* Additional helper functions for currency conversion
* Additional function for Due Date calculation for discounts
* New helper function for Fiscal period
* Renamed CDC parameters to match Reporting and ML Models.
* SAP Reporting now has a SQL Flavour switch to correctly deploy S4 or ECC based on the `_SQL_FLAVOUR` parameter. Possible valus are `ECC` or `S4`
* Enhanced reporting and calculation logics in:
    - `Deliveries`
    - `PurchaseDocuments`
    - `SalesOrders`
* Reduced CDC deployment copy step by 80% implementing multithreaded copy.
* Optimized overall build time when Cloud Build has the container cached, improving overall deployment time by 50%


**Bug Fixes:**
* Fix for Runtime views not generated from `settings.yaml`
* Deployment dependencies causing errors on view creation


**Known issues:**
-   Hierarchy flattener not configured to find sets in new test data
-   `.INCLUDE` in some tables in some versions of S/4 is known to be marked as key, which results in invalid SQLs in DAG generation. Recommendation as of now is to clear the _keyflag_ field in the offending `.INCLUDE` entry in the replicated `DD03L` table in BigQuery when generating the DAG. A better solution will be provided in a future release.

### Also check out
- **New!** Cortex Application Layer: a [Google Marketplace](https://console.cloud.google.com/marketplace/product/cortex-public/cloud-cortex-application-layer) offering showcasing how to consume the Cortex Data Foundation in your own cloud-native applications!
    - Best practices
    - Access patterns
    - Full working example with sample code
- **New!** [Looker Blocks](https://github.com/looker-open-source/block-cortex-sap) cool dashboards such as:
    - Orders Fulfillment
    - Order Snapshot (efficiency of Orders vs Deliveries)
    - Order Details
    - Sales Performance - Review the sales performance of Products, Division, Sales organization and Distribution channel.
    - Billing and Pricing
