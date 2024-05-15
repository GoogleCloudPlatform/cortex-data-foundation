# Sustainability & ESG insights with Cortex Framework

With increasing demand from customers, corporate investors, and regulators for transparent and sustainable operations, timely insights into environmental, social and governance (ESG) risks and opportunities is important.

Cortex Framework enables **vendor ESG performance** insights using Dun & Bradstreet ESG ranking data connected with SAP ERP supplier performance data to help answer questions like:
* What is my raw material suppliers' ESG performance against industry peers?
* What is their ability to measure and manage GHG emissions?
* What is their adherence and commitment to environmental compliance and corporate governance?

## Data model overview

Two BigQuery views are provided for this module, in the `K9 Reporting` dataset:

* `SustainableSourcing` provides ESG Insights for each vendor.
* `SustainableVendorSourcing` provides vendor performance insights alongside ESG Insights.

You can adjust the [reporting settings file](src/k9/src/sustainability/reporting/sustainability_reporting.yaml) as described in our [README](https://github.com/GoogleCloudPlatform/cortex-data-foundation/blob/main/README.md#customizing-reporting_settings-file-configuration).

![ERD of Sustainability & ESG Insights](images/erd_sustainability.png)



## Prerequisites

The following is required for this module to function:

*   Dun & Brandstreet source table - loaded as `dun_bradstreet_esg` table in `K9 Processing` dataset. See section below for details.
*   A full Cortex SAP ECC or S/4 deployment, containing CDC and Reporting datasets. Refer to the [main README](README.md) for details.

## Acquiring Dun & Bradstreet data

Navigate to the [BigQuery Analytics Hub](https://cloud.google.com/analytics-hub) and search for "esg ranking". There is an option to choose between four datasets: `US Full File`, `UK Full File`, `Global Full File` & `Global Financial Services`. Click on one of them, then select `Request Access` to get in touch with D&B for further instructions. Alternatively navigate to the [Google Cloud Marketplace](https://console.cloud.google.com/marketplace/product/prod-dnb-mp-data-public/dun-bradstreet-esg-rankings) and select Dun & Bradstreet ESG Intelligence offering.

![Analytics Hub](images/analytics_hub.png)

## Loading Dun & Bradstreet data

Table `dun_bradstreet_esg` in `K9 Processing` dataset hosts [Dun & Bradstreet ESG data](https://www.dnb.co.uk/content/dam/english/dnb-solutions/ESG%20Methodology%20Whitepaper.pdf), and it should be loaded through a CSV file provided by DnB. Please refer to the [data dictionary](/src/k9/src/sustainability/data/dnb_esg_data_dict.csv) and [sample data](/src/k9/src/sustainability/data/dnb_esg_sample_record.csv).

## Looker Studio Dashboard

Refer to this [sample Looker Studio dashboard](https://lookerstudio.google.com/c/u/0/reporting/cab436f2-ff83-4cfa-b7e4-e99b9fd9a8d0/page/RJ2qD) on how the data could be used to showcase insights.
