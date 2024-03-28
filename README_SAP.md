# Integration options for SAP ECC or SAP S/4HANA

## Deployment Configuration for SAP

| Parameter                | Meaning                      | Default Value  | Description                                                              |
| ------------------------ | -----------------------      | -------------- | ------------------------------------------------------------------------ |
| `SAP.deployCDC`          | Deploy CDC                   | `true`         | Generate CDC processing scripts to run as DAGs in Cloud Composer.        |
| `SAP.datasets.raw`       | Raw landing dataset          | -              | Used by the CDC process, this is where the replication tool lands the data from SAP. If using test data, create an empty dataset. |
| `SAP.datasets.cdc`       | CDC Processed Dataset        | -              | Dataset that works as a source for the reporting views, and target for the records processed DAGs. If using test data, create an empty dataset. |
| `SAP.datasets.reporting` | Reporting Dataset SAP        | `"REPORTING"`  | Name of the dataset that is accessible to end users for reporting, where views and user-facing tables are deployed. |
| `SAP.datasets.ml`        | ML dataset                   | `"ML_MODELS"`  | Name of the dataset that stages results of Machine Learning algorithms or BQML models. |
| `SAP.SQLFlavor`          | SQL flavor for source system | `"ecc"`        | `s4` or `ecc`. For test data, keep the default value (`ecc`). For Demand Sensing, only `ecc` test data is provided at this time. |
| `SAP.mandt`              | Mandant or Client            | `"100"`        | Default mandant or client for SAP. For test data, keep the default value (`100`). For Demand Sensing, use `900`. |

Note: While there is not a minimum version of SAP that is required, the ECC models have been developed on the current earliest supported version of SAP ECC. Differences in fields between our system and other systems are expected, regardless of the version.

## Loading SAP data into BigQuery

### **Prerequisites for SAP replication**

- Cortex Data Foundation expects SAP tables to be replicated with the same field names and types as they are created in SAP.
- As long as the tables are replicated with the same format, names of fields and granularity as in the source, there is no requirement to use a specific replication tool.
- Table names need to be created in BigQuery in lowercase.
- The list of tables used by SAP models are available and configurable in the CDC [setting.yaml](https://github.com/GoogleCloudPlatform/cortex-dag-generator/blob/main/setting.yaml). If a table is not present during deployment, the models depending on it will fail. Other models will deploy successfully.
- If in doubt about a conversion option, we recommend following the [default table mapping](https://cloud.google.com/solutions/sap/docs/bq-connector/latest/planning#default_data_type_mapping).
- **`DD03L` for SAP metadata**: If you are not planning on deploying test data, and if you are planning on generating CDC DAG scripts during deployment, make sure table `DD03L` is replicated from SAP in the source project.
  This table contains metadata about tables, like the list of keys, and is needed for the CDC generator and dependency resolver to work.
  This table will also allow you to add tables not currently covered by the model to generate CDC scripts, like custom or Z tables.

> **Note**: **What happens if I have minor differences in a table name?** Because SAP systems may have minor variations due to versions or add-on and append structures into tables, or because some replication tools may have slightly different handling of special characters, some views may fail not finding a field. We recommend executing the deployment with `turboMode : false` to spot most failures in one go. Examples of this are:
>  - Fields starting with `_` (e.g., `_DATAAGING`) have their `_` removed
>  - Fields cannot start with `/` in BigQuery
>
>  In this case, you can adapt the failing view to select the field as it is landed by your replication tool of choice.

## **Change Data Capture (CDC) processing**

There are two main ways for replication tools to load records from SAP:
- Append-always: Insert every change in a record with a timestamp and an operation flag (Insert, Update, Delete), so the last version can be identified.
- Update when landing (merge or upsert): This creates an updated version of a record on landing in the `change data capture processed`. It performs the CDC operation in BigQuery.

![CDC options for SAP](images/cdc_options.png)

Cortex Data Foundation supports both modes (append-always or update when landing). For append-always, we provide CDC processing templates.

> **Note** Some functionality will need to be commented out for Update on landing. For example, [OneTouchOrder.sql](https://github.com/GoogleCloudPlatform/cortex-reporting/blob/main/OneTouchOrder.sql) and all its dependent queries. The functionality can be replaced with tables like CDPOS.

### Configure CDC templates for tools replicating in append-always mode

#### **Configure CDC for SAP**

> **Note**: **We strongly recommend configuring this file according to your needs.** Some default frequencies may result in unnecessary cost if the business does not require such level of data freshness.

If using a tool that runs in append-always mode, Cortex Data Foundation provides CDC templates to automate the updates and create a _latest version of the truth_ or digital twin in the CDC processed dataset.

You can use the configuration in the file [`setting.yaml`](https://github.com/GoogleCloudPlatform/cortex-dag-generator/blob/main/setting.yaml) if you need to generate change-data capture processing scripts. See the [Appendix - Setting up CDC Processing](./README.md#setting-up-cdc-processing) for options. For test data, you can leave this file as a default.

Make any changes to the [DAG templates](https://github.com/GoogleCloudPlatform/cortex-dag-generator/blob/main/src/template_dag/dag_sql.py) as required by your instance of Airflow or Cloud Composer. You will find more information in the [Appendix - Gathering Cloud Composer settings](./README.md#gathering-cloud-composer-settings).

This module is optional. If you want to add/process tables individually after deployment, you can modify the `setting.yaml` file to process only the tables you need and re-execute the specific module calling `src/SAP_CDC/cloudbuild.cdc.yaml` directly.

#### Performance optimization for CDC Tables
For certain CDC datasets, you may want to take advantages of BigQuery [table partitioning](https://cloud.google.com/bigquery/docs/partitioned-tables), [table clustering](https://cloud.google.com/bigquery/docs/clustered-tables) or both. This choice depends on many factors - the size and data of the table, columns available in the table, and your need for real time data with views vs data materialized as tables. By default, CDC settings do not apply table partitioning or table clustering - the choice is yours to configure it based on what works best for you.

To create tables with partitions and/or clusters, update the CDC `setting.yaml` file with relevant configurations. See Appendix section [Table Partition and Cluster Settings](./README.md#table-partition-and-cluster-settings) for details on how to configure this.

> **NOTE**:
> 1. This feature only applies when a dataset in `setting.yaml` is configured for replication as a table (e.g. `load_frequency = "@daily"`) and not defined as a view (`load_frequency = "RUNTIME"`).
> 2. A table can be both - a partitioned table as well as a clustered table.


> **Important ⚠️**: If you are using a replication tool that allows partitions in the raw dataset, like the BigQuery Connector for SAP, we recommend [setting time-based partitions](https://cloud.google.com/solutions/sap/docs/bq-connector/latest/planning#table_partitioning) in the raw tables. The type of partition will work better if it matches the frequency for CDC DAGs in the `setting.yaml` configuration.

You can read more about partitioning and clustering for SAP [here](https://cloud.google.com/blog/products/sap-google-cloud/design-considerations-for-sap-data-modeling-in-bigquery).
