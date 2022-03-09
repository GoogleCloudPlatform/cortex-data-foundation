# Google Cloud Cortex Framework - Data Foundation

This repository contains the reporting models, change-data-capture generation scripts and deployment scripts for the Data Foundation for SAP in the Google Cloud Cortex Framework.

For more informaiton about the Google Cloud Cortex Framework, see the [documentation](https://cloud.google.com/solutions/cortex).

This template uses the Apache license, as is Google's default.  See the
documentation for instructions on using alternate license.

## Prerequisites

### Establish project and dataset structure
You will require at least one project to host the BigQuery datasets and execute the deployment process.

This is where the deployment process will trigger Cloud Build runs. In the project structure, this is the Source Project.

If you currently have a replication tool from SAP ECC or S/4 HANA, you can use the same project (Source Project) or a different one for reporting.

You will need to identify:
1.  Source Google Cloud Project: Project where the source data is located which the data models will consume.
2.  Target Google Cloud Project: Project where Data Foundation for SAP predefined data models will be deployed and accessed by end-users. This may or may not be different from the source project.
3.  Source BigQuery Dataset: BigQuery dataset where the source SAP data is replicated to or where the test data will be created.
CDC BigQuery Dataset: BigQuery dataset where the CDC processed data lands the latest available records. This may or may not be the same as the source dataset.
4.  Target BigQuery reporting dataset: BigQuery dataset where the Data Foundation for SAP predefined data models will be deployed. This will be named as SAP_REPORTING and the name cannot be changed.

Alternatively, if you do not have a replication tool set up or do not wish to use the replicated data, the deployment process can generate **test data** for you in the test harness. You will still need to create and identify the datasets ahead of time.

### Enable Required Components
The following Google Cloud components are required:
-  Google Cloud Project
-  BigQuery instance and datasets
-  Service Account with Impersonation rights
-  Cloud Storage Buckets
-  Cloud Build API
-  Cloud Resource Manager API
-  Optional:
   [Cloud Composer](https://console.cloud.google.com/marketplace/product/google/composer.googleapis.com) for change data capture (CDC) processing and hierarchy flattening through [Directed Acyclic Graphs (DAG)](https://airflow.apache.org/docs/apache-airflow/stable/concepts/dags.html). You can find how to set up an instance of Cloud Composer in the documentation.
-  Looker (optional, connects to reporting templates. Requires manual setup)

From the [Cloud Shell](https://shell.cloud.google.com/?hl=en_US&fromcloudshell=true&show=ide%2Cterminal), you can enable Google Cloud Services using the `gcloud` command line interface in your Google Cloud project.

Replace the <<SOURCE_PROJECT>> placeholder with your source project. Copy and paste the following command into the cloud shell:

```bash
gcloud config set project <<SOURCE_PROJECT>>
gcloud services enable bigquery.googleapis.com \
                       cloudbuild.googleapis.com \
                       composer.googleapis.com \
                       storage-component.googleapis.com \
                       cloudresourcemanager.googleapis.com
```

## Deployment Instructions

1.  Navigate to the `src/SAP_CDC` directory
2.  Replace the parameters for your project and execute the following command. See the **Appendix** section below for help on these parameters:
  ```bash
  gcloud builds submit \
        --gcs-log-dir=gs://${gcs_log_bucket} \
        --config=cloudbuild.cdc.yaml \
        --substitutions _PROJECT_ID_SRC=${project_id_src},_DATASET_SRC=${dataset_repl},_PROJECT_ID_TGT=${project_id_tgt},_DATASET_TGT=${dataset_tgt},_GCS_BUCKET=${tgt_bucket},_TEST_DATA=${gen_test}
  ```
3.  Once the build has finished successfully, navigate to `src/views/SAP_REPORTING`. Execute the build from this directory using the following command:
  ```bash
  gcloud builds submit \
        --gcs-log-dir=gs://${gcs_log_bucket} \
        --config=cloudbuild.reporting.yaml \
        --substitutions _PJID_SRC=${project_id_src},_PJID_TGT=${project_id_tgt},_DS_CDC=${CDC_PROCESSED_DATASET},_DS_RAW=${RAW_LANDING_DATASET},_DS_REPORTING=${REPORTING_DATASET},_DS_MODELS=${ML_MODELS_DATASET},_LOCATION=${LOCATION},_MANDT=${MANDT},_SQL_FLAVOUR=${SQL_FLAVOUR},_VIEWS_DIR=${views_dir}
  ```

## Appendix

Here are the possible parameters for the Cloud Build commands:

| Name                  | Description | Mandatory | Default Value |
|-----------------------|-------------|-----------|---------------|
| `project_id_src`        | Source Google Cloud Project:<br /> Project where the source data is located which the data models will consume. | Y | N/A
| `project_id_tgt`        | Target Google Cloud Project:<br /> Project where Data Foundation for SAP predefined data models will be deployed and accessed by end-users. <br /> This may or may not be different from the source project. | Y | N/A
| `dataset_raw_landing`   | Source BigQuery Dataset:<br /> BigQuery dataset where the source SAP data is replicated to or where the test data will be created.  | Y | N/A
| `dataset_cdc_processed` | CDC BigQuery Dataset:<br /> BigQuery dataset where the CDC processed data lands the latest available records. <br /> This may or may not be the same as the source dataset.  | Y | N/A
| `dataset_reporting_tgt` | Target BigQuery reporting dataset:<br /> BigQuery dataset where the Data Foundation for SAP predefined data models will be deployed. | N | SAP_REPORTING
| `dataset_models_tgt`    | Target BigQuery reporting dataset:<br /> BigQuery dataset where the Data Foundation for SAP predefined data models will be deployed. | N | SAP_ML_MODELS
| `mandt`                 | SAP Mandant. Must be 3 character.  | Y | 800
| `sql_flavour`           | Which database target type. <br />Valid values are `ECC` or `S4` | N | `ECC`


