# Cortex Framework data foundation

The Cortex Framework data foundation is the core architectural component of [Google Cloud Cortex Framework](https://cloud.google.com/solutions/cortex). Cortex Framework provides reference architectures and deployable solution content to kickstart your Agentic Data Cloud journey with Google Cloud. Cortex Framework incorporates your source data into tools and services that help ingest, transform, and load it to get insights faster from pre-defined data models that can be automatically deployed for use with [Google Cloud BigQuery](https://cloud.google.com/bigquery).

[**Cortex Framework version 7**](https://docs.cloud.google.com/cortex/docs/release-notes#April_30_2026) is now available in a [separate GitHub repository](https://github.com/GoogleCloudPlatform/cortex-framework). We recommend using v7 for all new deployments, as it introduces a highly modular architecture, simplifies data orchestration with [Dataform](https://docs.cloud.google.com/dataform/docs), and provides enhanced support for next-generation AI-ready data products.
 
**Note**: Important upgrade considerations for Version 7:  Because v7 is a new major version it implies breaking changes with no automatic migration path. For v6 customers looking to adopt v7, we provide [v6 compatibility content for SAP reporting.](https://docs.cloud.google.com/cortex/docs/v6-compatibility).

For any questions or issues related to the Google Cloud Cortex Framework itself,  see the [Cortex Framework Support](https://docs.cloud.google.com/cortex/docs/v6/support) page.

# Data sources and workloads

Cortex Framework focuses on solving specific problems and offers pre built solutions
for business areas like Marketing, Sales, Supply Chain, Manufacturing, Finance, and Sustainability.
Cortex Framework is flexible and it can include data from sources beyond what is prebuilt.
The following are the data sources available. For more information about each one, click any
of them.

**Marketing**:

*   [Salesforce Marketing Cloud](https://docs.cloud.google.com/cortex/docs/v6/marketing-salesforce)
*   [Google Ads](https://docs.cloud.google.com/cortex/docs/v6/marketing-googleads)
*   [Campaign Manager 360 (CM360)](https://docs.cloud.google.com/cortex/docs/v6/marketing-cm360)
*   [TikTok](https://docs.cloud.google.com/cortex/docs/v6/marketing-tiktok)
*   [Meta](https://docs.cloud.google.com/cortex/docs/v6/marketing-meta)
*   [LiveRamp](https://docs.cloud.google.com/cortex/docs/v6/marketing-liveramp)
*   [YouTube (with DV360)](https://docs.cloud.google.com/cortex/docs/v6/marketing-dv360)
*   [Google Analytics 4](https://docs.cloud.google.com/cortex/docs/v6/marketing-google-analytics)
*   [Cross Media & Product Connected Insights](https://docs.cloud.google.com/cortex/docs/v6/marketing-cross-media)
*   [Cortex for Meridian](https://docs.cloud.google.com/cortex/docs/v6/meridian)

**Operational**:

*   [SAP (ECC and S/4)](https://docs.cloud.google.com/cortex/docs/v6/operational-sap)
*   [Salesforce Sales Cloud](https://docs.cloud.google.com/cortex/docs/v6/operational-salesforce)
*   [Oracle EBS](https://docs.cloud.google.com/cortex/docs/v6/operational-oracle-ebs)

**Sustainability**:

*   [Dun & Bradstreet with SAP](https://docs.cloud.google.com/cortex/docs/v6/dun-and-bradstreet)

**Note**: If you want to know more about which entities are covered in each data source, see the
Entity-Relationship Diagrams (ERD) in the [docs](https://github.com/GoogleCloudPlatform/cortex-data-foundation/tree/main/docs) folder.

# Deployment

For Cortex Framework deployment instructions, see the following:

*   **Quickstart Demo**: a [quickstart demo](https://docs.cloud.google.com/cortex/docs/v6/quickstart-demo) to
test the Cortex Framework set up process with sample data within just a few clicks. *This demo deployment
is not suitable for production environments*.
*   **Deployment steps**: after reading the [prerequisites](https://docs.cloud.google.com/cortex/docs/v6/deployment-prerequisites) for Cortex Data Foundation deployment, follow the steps for deployment in production environments:
    1. [Establish workloads](https://docs.cloud.google.com/cortex/docs/v6/deployment-step-one)
    2. [Clone repository](https://docs.cloud.google.com/cortex/docs/v6/deployment-step-two)
    3. [Determine integration mechanism](https://docs.cloud.google.com/cortex/docs/v6/deployment-step-three)
    4. [Set up components](https://docs.cloud.google.com/cortex/docs/v6/deployment-step-four)
    5. [Configure deployment](https://docs.cloud.google.com/cortex/docs/v6/deployment-step-five)
    6. [Execute deployment](https://docs.cloud.google.com/cortex/docs/v6/deployment-step-six)

## Optional steps

You can customize your Cortex Framework deployment with the following optional steps:

*   [Use different projects to segregate access](https://docs.cloud.google.com/cortex/docs/v6/optional-step-segregate-access)
*   [Use Cloud Build features](https://docs.cloud.google.com/cortex/docs/v6/optional-step-cloud-build-features)
*   [Configure external datasets for K9](https://docs.cloud.google.com/cortex/docs/v6/optional-step-external-datasets)
*   [Enable Turbo Mode](https://docs.cloud.google.com/cortex/docs/v6/optional-step-turbo-mode)
*   [Telemetry](https://docs.cloud.google.com/cortex/docs/v6/optional-step-telemetry)
*   [Configure Common Dimensions](https://docs.cloud.google.com/cortex/docs/v6/optional-step-common-dimensions)
*   [Task dependent DAGs](https://docs.cloud.google.com/cortex/docs/v6/optional-step-task-dependent-dags)

## Looker Blocks and Dashboards

After Cortex Framework deployment, you can take advantage of prebuilt Looker Blocks and Dashboards for some of the Cortex Framework data sources.
For more information, see [Looker Blocks and Dashboards overview](https://docs.cloud.google.com/cortex/docs/v6/looker-block-overview).

The following are the Looker Blocks and Dashboards available in Cortex Framework:

* Looker Blocks
    *   **Operational**
        *   [Looker Block for SAP](https://docs.cloud.google.com/cortex/docs/v6/looker-block-sap)
        *   [Looker Block for Salesforce](https://docs.cloud.google.com/cortex/docs/v6/looker-block-salesforce)
        *   [Looker Block for Oracle EBS](https://docs.cloud.google.com/cortex/docs/v6/looker-block-oracle-ebs)
    *   **Marketing**
        *   [Looker Block for Salesforce Marketing Cloud](https://docs.cloud.google.com/cortex/docs/v6/looker-block-salesforce-marketing)
        *   [Looker Block for Meta](https://docs.cloud.google.com/cortex/docs/v6/looker-block-meta)
        *   [Looker Block for YouTube (with DV360)](https://docs.cloud.google.com/cortex/docs/v6/looker-block-youtube)
        *   [Looker Block for Cross Media & Product Connected Insights](https://docs.cloud.google.com/cortex/docs/v6/looker-block-cross-media)
* Looker Studio Dashboards
    *   **Sustainability**
        *   [Looker Studio Dashboard for Dun & Bradstreet](https://docs.cloud.google.com/cortex/docs/v6/looker-dashboard-dun-and-bradstreet)

Note: If you are looking for the README files before Release 6.0, see the
[deprecated docs folder](https://github.com/GoogleCloudPlatform/cortex-data-foundation/tree/main/docs/deprecated).
