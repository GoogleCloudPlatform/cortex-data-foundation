# **External DAGs / Data Sources Migration guide for v4.2 to v5.0**

**TL;DR:** This is a guide for migrating the output tables of External DAGs (e.g., Weather, Trends) from Cortex Data Foundation v4.2 to their new locations in v5.0. 

If you have not used External DAGs before, or if you have not deployed SAP, this guide will not apply and you can stop reading now.

# Background Context

Before Cortex Data Foundation version 4.2, `_GEN_EXT` flag controlled external data sources deployment, and some sources are workload-dependent (e.g., currency conversion for SAP). As of version 5.0, `_GEN_EXT` flag is retired, and the execution of DAGs that can serve multiple workloads is now controlled by a new execution module. This guide provides high-level recommendations for migrating your existing DAG output tables so your workflow will continue to function.

##  Cross-workload reusable DAGs (a.k.a, K9)

The K9 is introduced in v5.0 for the ingestion, processing and modelling of components that are reusable across different data sources.

The following external data sources are now deployed as a part of K9, into the K9_PROCESSING dataset:
- `date_dimension`
- `holiday_calendar`
- `trends`
- `weather`

Reporting views will refer to the K9_PROCESING dataset for these reusable components.

## SAP-dependent DAGs

- The following SAP-dependent DAGs are still executed by `generate_external_dags.sh`, but now execute during the reporting build step, and now write into the SAP reporting dataset instead of the cdc dataset.
    - `currency_conversion`
    - `inventory_snapshots`
    - `prod_hierarchy_texts`
    - `hier_reader`

# Migration Guide

## Deploying Cortex Data Foundations 5.0

First, deploy the newest version (v5.0) of Cortex Data Foundations to your desired projects, with the following guidelines:

- You should use your existing RAW and CDC datasets from prior development or staging deployments as your RAW and CDC datasets of this deployment, as no modification will be made to them during deployment.
- In `config/config.json`, set both `testData` and `SAP.deployCDC` to `False`.
- We recommend to first start testing with a new SAP Reporting project, adjacent to the current development or staging dataset, that is different from what you are currently using for v4.2, and follow the process below. This way, you may safely validate the deployment and migration process.

## (Optional) Stop current Composer AirFlow DAGs

If you are currently running the DAGs, you need to first pause them in your Composer environment.
- [Open Airflow UI from Composer](https://cloud.google.com/composer/docs/how-to/accessing/airflow-web-interface)
- [Pause the DAG](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html#dag-pausing-deactivation-and-deletion)

## Migrating existing tables

To migrate your existing tables to their new location, you can use `jinja-cli` to format the provided migration script template to complete the migration.

1. Install jinja-cli:
```shell
pip install jinja-cli
```

2. Identify the following parameters from your existing v4.2 and new v5.0 deployment:

| Name                  | Description |
|-----------------------|-------------|
| `project_id_src`      | Source Google Cloud Project: <br /> Project where your existing SAP CDC dataset from v4.2 deployment is located. <br /> K9_PROCESSING dataset will also be created in this project. |
| `project_id_tgt`        | Target Google Cloud where your newly deployed SAP Reporting dataset from the new v5.0 deployment is located. <br /> This may or may not be different from the source project. |
| `dataset_cdc_processed` | CDC BigQuery Dataset:<br /> BigQuery dataset where the CDC processed data lands the latest available records. <br /> This may or may not be the same as the source dataset.  |
| `dataset_reporting_tgt` | Target BigQuery reporting dataset:<br /> BigQuery dataset where the Data Foundation for SAP predefined data models will be deployed. |
| `k9_datasets_processing` | K9 BigQuery dataset:<br /> BigQuery dataset where the K9 (augmented data sources) is deployed. |

Then, create a json file with the required input data. Make sure to remove any DAGs you do not want to migrate from the `migrate_list` section:

```shell
cat  <<EOF > data.json
{
  "project_id_src": "your-source-project",
  "project_id_tgt": "your-target-project",
  "dataset_cdc_processed": "your-cdc-processed-dataset",
  "dataset_reporting_tgt": "your-reporting-target-dataset-OR-SAP_REPORTING",
  "k9_datasets_processing": "your-k9-processing-dataset-OR-K9_REPORTING",
  "migrate_list":
    [
        "holiday_calendar",
        "trends",
        "weather",
        "currency_conversion",
        "inventory_snapshots",
        "prod_hierarchy_texts",
        "hier_reader"
    ]
}
EOF
```

Here is what an example looks like, with `weather` and `trends` removed
```json
{
  "project_id_src": "kittycorn-demo",
  "project_id_tgt": "kittycorn-demo",
  "dataset_cdc_processed": "CDC_PROCESSED",
  "dataset_reporting_tgt": "SAP_REPORTING",
  "k9_datasets_processing": "K9_PROCESSING",
  "migrate_list":
    [
        "holiday_calendar",
        "currency_conversion",
        "inventory_snapshots",
        "prod_hierarchy_texts",
        "hier_reader"
    ]
}
```

3. Create an output folder
```shell
mkdir output
```

4. Now generate the parsed migration script:

Assume you are at the root of the repository:

```shell
jinja -d data.json -o output/migrate_external_dags.sql docs/external_dag_migration/scripts/migrate_external_dags.sql
```

5. Examine the output SQL file and execute in BigQuery to migrate your tables to the new location.

## Update and unpausing Airflow DAGs

Back up the current DAG Files in your Airflow bucket. Then, replace them with the newly generated files from your Cortex Data Foundations v5.0 deployment.

- [Open Airflow UI from Composer](https://cloud.google.com/composer/docs/how-to/accessing/airflow-web-interface)
- [Unpause the DAG](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html#dag-pausing-deactivation-and-deletion)

## Validation and Cleanup

The migration is now complete. You can now validate that all reporting views in the new v5.0 reporting deployment is working correctly.

If everything works properly, go through the above process again, this time targeting the v5.0 deployment to your production Reporting set.

Afterwards, feel free to remove all tables using the following script:

**!!WARNING: THIS STEP PERMANENTLY REMOVES YOUR OLD DAG TABLES AND CANNOT BE UNDONE!!** 

Please only execute this step after all validation is complete. Consider taking backups of these tables.

```shell
jinja -d data.json -o output/delete_old_dag_tables.sql docs/external_dag_migration/scripts/delete_old_dag_tables.sql
```
