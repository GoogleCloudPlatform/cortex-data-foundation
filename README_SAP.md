# Integration options for SAP ECC or SAP S/4HANA

## Deployment configuration for SAP

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

### Replicating raw data from SAP

The goal of the Data Foundation is to expose data and analytics models for reporting and applications. The models consume the data replicated from an SAP ECC or SAP S/4HANA system using a preferred replication tool, like those listed in the [Data Integration Guides for SAP](https://cloud.google.com/solutions/sap/docs/sap-data-integration-guides).

Data from SAP ECC or S/4HANA is expected to be replicated in raw form, that is, with the same structure as the tables in SAP and without transformations. The names of the tables in BigQuery should be lower case for cortex data model compatibility reasons.

For example, fields in table T001 are replicated using their equivalent data type in BigQuery, without transformations:

![alt_text](images/15.png "image_tooltip")

### Prerequisites for SAP replication

- Cortex Data Foundation expects SAP tables to be replicated with the same field names and types as they are created in SAP.
- As long as the tables are replicated with the same format, names of fields and granularity as in the source, there is no requirement to use a specific replication tool.
- Table names need to be created in BigQuery in lowercase.
- The list of tables used by SAP models are available and configurable in the CDC [setting.yaml](https://github.com/GoogleCloudPlatform/cortex-dag-generator/blob/main/setting.yaml). If a table is not present during deployment, the models depending on it will fail. Other models will deploy successfully.
- Things to consider if you are using [Google BigQuery Connector for SAP](https://cloud.google.com/solutions/sap/docs/bq-connector/latest/planning):
  * If in doubt about a conversion option, we recommend following the [default table mapping](https://cloud.google.com/solutions/sap/docs/bq-connector/latest/planning#default_data_type_mapping).
  * We recommend disabling [record compression](http://cloud/solutions/sap/docs/bq-connector/latest/planning#record_compression), because compression may change original SAP data in a way that impacts Cortex CDC layer as well as Cortex reporting dataset.

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

> **Note**: **We strongly recommend configuring this file according to your needs.** Some default frequencies may result in unnecessary cost if the business does not require such level of data freshness.

If using a tool that runs in append-always mode, Cortex Data Foundation provides CDC templates to automate the updates and create a _latest version of the truth_ or digital twin in the CDC processed dataset.

You can use the configuration in the file [`setting.yaml`](https://github.com/GoogleCloudPlatform/cortex-dag-generator/blob/main/setting.yaml) if you need to generate change-data capture processing scripts. See the [Appendix - Setting up CDC Processing](#setting-up-cdc-processing) for options. For test data, you can leave this file as a default.

Make any changes to the [DAG templates](https://github.com/GoogleCloudPlatform/cortex-dag-generator/blob/main/src/template_dag/dag_sql.py) as required by your instance of Airflow or Cloud Composer. You will find more information in the [Appendix - Gathering Cloud Composer settings](./README.md#gathering-cloud-composer-settings).

This module is optional. If you want to add/process tables individually after deployment, you can modify the `setting.yaml` file to process only the tables you need and re-execute the specific module calling `src/SAP_CDC/cloudbuild.cdc.yaml` directly.

### Setting up CDC processing

During deployment, you can choose to merge changes in real time using a view in BigQuery or scheduling a merge operation in Cloud Composer (or any other instance of Apache Airflow).

Cloud Composer can schedule the scripts to process the merge operations periodically. Data is updated to its latest version every time the merge operations execute, however, more frequent merge operations  translate into higher costs.

![alt_text](images/19.png "image_tooltip")


The scheduled frequency can be customized to fit the business needs.

You will notice the file uses [scheduling supported by Apache Airflow](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dag-run.html).

The following example shows an extract from the configuration file:


```yaml
data_to_replicate:
  - base_table: adrc
    load_frequency: "@hourly"
  - base_table: adr6
    target_table: adr6_cdc
    load_frequency: "@daily"
```


This configuration will:

1. Create a copy from **`source\_project\_id.REPLICATED\_DATASET.adrc`** into **`target\_project\_id.DATASET\_WITH\_LATEST\_RECORDS.adrc`** if the latter does not exist
2. Create a CDC script in the specified bucket
3. Create a copy from `source\_project\_id.REPLICATED\_DATASET.adr6` into `target\_project\_id.DATASET\_WITH\_LATEST\_RECORDS.adr6\_cdc` if the latter does not exist
4. Create a CDC script in the specified bucket

If you want to create DAGs or runtime views to process changes for tables that exist in SAP and are not listed in the file, add them to this file before deployment. For example, the following configuration creates a CDC script for custom table “_zztable\_customer”_ and a runtime view to scan changes in real time for another custom table called “_zzspecial\_table”_:


```yaml
  - base_table: zztable_customer
    load_frequency: "@daily"
  - base_table: zzspecial_table
    load_frequency: "RUNTIME"
```

This will work as long as the table `DD03L` is replicated in the source dataset and the schema of the custom table is present in that table.

### Sample generated template
The following template generates the processing of changes. Modifications, such as the name of the timestamp field, or additional operations, can be done at this point:


```sql
MERGE `${target_table}` T
USING (SELECT * FROM `${base_table}` WHERE recordstamp > (SELECT IF(MAX(recordstamp) IS NOT NULL, MAX(recordstamp),TIMESTAMP("1940-12-25 05:30:00+00")) FROM `${target_table}`)) S
ON ${p_key}
WHEN MATCHED AND S.operation_flag='D' AND S.is_deleted = true THEN
  DELETE
WHEN NOT MATCHED AND S.operation_flag='I' THEN
  INSERT (${fields})
  VALUES
  (${fields})
WHEN MATCHED AND S.operation_flag='U' THEN
UPDATE SET
    ${update_fields}
```


Alternatively, if your business requires near-real time insights and the replication tool supports it, the deployment tool accepts the option RUNTIME. This means a CDC script will not be generated. Instead, a  view will scan and fetch the latest available record at runtime for [immediate consistency](https://cloud.google.com/architecture/database-replication-to-bigquery-using-change-data-capture#immediate_consistency_approach).

### CDC fields required for MERGE operations
The following parameters will be required for the automated generation of change-data-capture batch processes:

*   Source project + dataset: Dataset where the SAP data is streamed or replicated. For the CDC scripts to work by default, the tables need to have a timestamp field (called recordstamp) and an operation field with the following values, all set during replication:
    *   I: for insert
    *   U: for update
    *   D: for deletion
*   Target project + dataset for the CDC processing: The script generated by default will generate the tables from a copy of the source dataset if they do not exist.
*   Replicated tables: Tables for which the scripts need to be generated
*   Processing frequency: Following the Cron notation, how frequently the DAGs are expected to run
*   Target GCS bucket where the CDC output files will be copied
*   The name of the connection used by Cloud Composer
*   Optional: If the result of the CDC processing will remain in the same dataset as the target, you can specify the name of the target table.

### Performance optimization for CDC tables
For certain CDC datasets, you may want to take advantages of BigQuery [table partitioning](https://cloud.google.com/bigquery/docs/partitioned-tables), [table clustering](https://cloud.google.com/bigquery/docs/clustered-tables) or both. This choice depends on many factors - the size and data of the table, columns available in the table, and your need for real time data with views vs data materialized as tables. By default, CDC settings do not apply table partitioning or table clustering - the choice is yours to configure it based on what works best for you.

To create tables with partitions and/or clusters, update the CDC `setting.yaml` file with relevant configurations. See Appendix section [Table Partition and Cluster Settings](./README.md#table-partition-and-cluster-settings) for details on how to configure this.

> **NOTE**:
> 1. This feature only applies when a dataset in `setting.yaml` is configured for replication as a table (e.g. `load_frequency = "@daily"`) and not defined as a view (`load_frequency = "RUNTIME"`).
> 2. A table can be both - a partitioned table as well as a clustered table.


> **Important ⚠️**: If you are using a replication tool that allows partitions in the raw dataset, like the BigQuery Connector for SAP, we recommend [setting time-based partitions](https://cloud.google.com/solutions/sap/docs/bq-connector/latest/planning#table_partitioning) in the raw tables. The type of partition will work better if it matches the frequency for CDC DAGs in the `setting.yaml` configuration.

You can read more about partitioning and clustering for SAP [here](https://cloud.google.com/blog/products/sap-google-cloud/design-considerations-for-sap-data-modeling-in-bigquery).


## \[Optional\] Configuring the flattener for SAP hierarchies

The deployment process can optionally flatten hierarchies represented as sets (e.g. transaction GS03) in SAP. The process can also generate the DAGs for these hierarchies to be refreshed periodically and automatically. This process requires configuration prior to the deployment and should be known by a Financial or Controlling consultant or power user.

If you need to generate scripts to flatten hierarchies, you can use the configuration in the file [`sets.yaml`](https://github.com/GoogleCloudPlatform/cortex-reporting/blob/main/local_k9/hier_reader/sets.yaml). This step is only executed if the CDC generation flag is set to `true`.

The deployment file takes the following parameters:

*   Name of the set
*   Class of the set (as listed by SAP in standard table SETCLS)
*   Organizational Unit: Controlling Area or additional key for the set
*   Client or Mandant
*   Reference table for the referenced master data
*   Reference key field for master data
*   Additional filter conditions (where clause)

The following are examples of configurations for Cost Centers and Profit Centers including the technical information. If unsure about these parameters, consult with a Finance or Controlling SAP consultant.


```
sets_data:
#Cost Centers:
# table: csks, select_fields (cost center): 'kostl', where clause: Controlling Area (kokrs), Valid to (datbi)
- setname: 'H1'
  setclass: '0101'
  orgunit: '1000'
  mandt:  '800'
  table: 'csks'
  key_field: 'kostl'
  where_clause: [ kokrs = '1000', datbi >= cast('9999-12-31' as date)]
  load_frequency: "@daily"
#Profit Centers:
# setclass: 0106, table: cepc, select_fields (profit center): 'cepc', where clause: Controlling Area (kokrs), Valid to (datbi)
- setname: 'HE'
  setclass: '0106'
  orgunit: '1000'
  mandt:  '800'
  table: 'cepc'
  key_field: 'prctr'
  where_clause: [ kokrs = '1000', datbi >= cast('9999-12-31' as date) ]
  load_frequency: "@monthly"
#G/L Accounts:
# table: ska1, select_fields (GL Account): 'saknr', where clause: Chart of Accounts (KTOPL), set will be manual. May also need to poll Financial Statement versions.

```


This configuration will generate two separate DAGs. For example, if there were two configurations for Cost Center hierarchies, one for Controlling Area 1000 and one for 2000, the DAGs would be 2 different files and separate processes but the target, flattened table would be the same.

**Important:** If re-running the process and re-initializing the load, make sure the tables are truncated. The CDC and initial load processes do not clear the contents of the tables which means the flattened data will be inserted again.
