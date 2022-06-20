
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
    - AccountingDocumentsReceivables
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
* Fix for Runtime views not generated from `setting.yaml`
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
