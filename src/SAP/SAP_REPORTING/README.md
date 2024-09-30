# SAP Reporting

Templates for SAP reporting (and more!) for Google Cloud Cortex Data Foundation.

## Deployment

We recommend looking at the instructions in the parent module, the [Cortex Data Foundation](https://github.com/GoogleCloudPlatform/cortex-data-foundation). You will find instructions and details on the parameters for the deployment there.

> Configuration file for this repository is **`config/config.json`**.

> To fulfill this some of the dependencies of the Reporting views, please make sure you run K9-pre DAGs in the [Cortex Data Foundation](https://github.com/GoogleCloudPlatform/cortex-data-foundation).
To configure running K9-pre DAGs, edit [k9_settings.yaml](https://github.com/GoogleCloudPlatform/cortex-data-foundation/tree/main/src/k9/config/k9_settings.yaml).

Individual views can be deployed with **`cloudbuild.reporting.yaml`** using the same parameters as described in the parent module
for SAP deployment (that README refers to `config/config.json`).

# Cloudbuild Parameters:
Running Cloud Build with `cloudbuild.reporting.yaml` requires the following
parameters:
- `_GCS_BUCKET`: GCS bucket created for logs that this generator writes to (logs bucket).

# Results
- Views will be created in the target project (`projectIdTarget` value in `config/config.json`).
- The generated python scripts will be copied to `gs://<targetBucket>/dags`
- The generated SQL scripts will be copied to `gs://<targetBucket>/data/bq_data_replication`

`targetBucket` - is a GCS bucket created for holding the DAG python scripts and SQL scripts
(`targetBucket` in `config/config.json`).
