#!/bin/bash

# Copyright 2025 Google LLC

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Automated Meridian sample deployment with pre defined configuration

# Exit on error.
set -e

# Determine script's own directory
SCRIPT_OWN_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)

PROJECT_ID=$(gcloud config get-value project)
GEO_LOCATION="us"
REGION_LOCATION="us-central1"
TEST_DATA_PROJECT="kittycorn-public"

# Function to add === at the beginning and end of an echo
fancy_echo_start() {
    local message="$1"
    echo -e "\n"
    echo -e "❇️  ${message} ❇️ \n"
}

fancy_echo_important() {
    local message="$1"
    echo -e "❗️IMPORTANT❗️ ${message} \n"
}

fancy_echo_done() {
    local message="$1"
    echo -e "\n✅ ${message} \n"
}

fancy_error_echo() {
    local message="$1"
    echo -e "   ❗ ${message}"
}

fancy_sub_echo() {
    local message="$1"
    echo -e "   ➡️  ${message}"
}

fancy_robot_header() {
    local message="$1"
    echo -e "🤖 -= ${message} =- 🤖 \n"
}

clear
fancy_robot_header "Cortex for Meridian - Automated Sample Deployment"


# Enable APIs
fancy_echo_start "Enabling APIs"

API_SERVICES=("compute.googleapis.com" "bigquery.googleapis.com"
    "cloudbuild.googleapis.com" "storage-component.googleapis.com"
    "cloudresourcemanager.googleapis.com"
    "workflows.googleapis.com" "aiplatform.googleapis.com")

# Loop through the list of API service names
for api_service in "${API_SERVICES[@]}"; do
    # Construct the full API service name
    full_api_name="${api_service}"

    # Execute the gcloud command to enable the API
    fancy_sub_echo "Enabling API: ${full_api_name} ⚙️"
    gcloud services enable "${full_api_name}" --project="${PROJECT_ID}"

done

fancy_echo_done "Done enabling APIs"

# Create data sets
fancy_echo_start "Creating empty data sets"

DATA_SET_NAMES=('K9_PROCESSING' 'K9_REPORTING' 'CORTEX_ORACLE_EBS_REPORTING' 
    'CORTEX_ORACLE_EBS_REPORTING_DEMO' 'CORTEX_GADS_CDC' 'CORTEX_GADS_RAW' 'CORTEX_ORACLE_EBS_CDC'
    'CORTEX_GADS_REPORTING' 'CORTEX_TIKTOK_CDC' 'CORTEX_TIKTOK_RAW' 'CORTEX_TIKTOK_REPORTING'
    'CORTEX_META_CDC' 'CORTEX_META_RAW' 'CORTEX_META_REPORTING' 'CORTEX_DV360_RAW'
    'CORTEX_DV360_CDC' 'CORTEX_DV360_REPORTING')

for dataset_name in "${DATA_SET_NAMES[@]}"; do
    # Construct the full dataset ID
    full_dataset_id="${PROJECT_ID}:${dataset_name}"

    # Execute the bq command to create the dataset
    fancy_sub_echo "Creating dataset: ${full_dataset_id} 🗂️"
    bq \
    --location="${GEO_LOCATION}" \
    --project_id="${PROJECT_ID}" \
    mk -d -f \
    "${full_dataset_id}" >/dev/null
done

DATA_SET_NAMES_REGION_SPECIFIC=('CORTEX_VERTEX_AI_PROCESSING')

for dataset_name in "${DATA_SET_NAMES_REGION_SPECIFIC[@]}"; do
    # Construct the full dataset ID
    full_dataset_id="${PROJECT_ID}:${dataset_name}"

    # Execute the bq command to create the dataset
    fancy_sub_echo "Creating dataset: ${full_dataset_id} 🗂️"
    bq \
    --location="${REGION_LOCATION}" \
    --project_id="${PROJECT_ID}" \
    mk -d  -f \
    "${full_dataset_id}" >/dev/null
done

fancy_echo_done "Done creating data sets"

# Load sales data
fancy_echo_start "Loading sample Oracle EBS sales reporting data for MMM"

BQ_SOURCE_DATASET_SALES="${TEST_DATA_PROJECT}:oracleebs__reporting__6_3__us.SalesOrdersDailyAgg"
BQ_TARGET_DATASET_SALES="${PROJECT_ID}:CORTEX_ORACLE_EBS_REPORTING_DEMO.SalesOrdersDailyAgg"

fancy_sub_echo "Copying and overwriting data from table ${BQ_SOURCE_DATASET_SALES} to ${BQ_TARGET_DATASET_SALES}"

# Delete default columns partitioned table to replace with test data non partitioned table
bq rm --force=true "${BQ_TARGET_DATASET_SALES}"

# Execute the copy command
bq cp \
  --force=true \
  "${BQ_SOURCE_DATASET_SALES}" \
  "${BQ_TARGET_DATASET_SALES}"

fancy_echo_done "Done loading sample Oracle EBS sales reporting data for MMM"

# Create Meridian runner SA

fancy_echo_start "Creating Cortex for Meridian Colab Runner Service Account"

# Define the service account ID
SERVICE_ACCOUNT_ID="cortex-meridian-colab-runner"

# Define the full service account email
MERIDIAN_RUNNER_SERVICE_ACCOUNT_EMAIL="${SERVICE_ACCOUNT_ID}@${PROJECT_ID}.iam.gserviceaccount.com"

# Check if the service account exists
if gcloud iam service-accounts list \
    --project="$PROJECT_ID" \
    --filter="email:${MERIDIAN_RUNNER_SERVICE_ACCOUNT_EMAIL}" \
    --format="value(email)" |
grep -q "${MERIDIAN_RUNNER_SERVICE_ACCOUNT_EMAIL}"; then
    fancy_sub_echo "Service account '${MERIDIAN_RUNNER_SERVICE_ACCOUNT_EMAIL}' already exists skipping create 🔐"
else
    fancy_sub_echo "Creating service account '${SERVICE_ACCOUNT_ID}'..."
    gcloud iam service-accounts create "${SERVICE_ACCOUNT_ID}" --project="$PROJECT_ID" \
        --description="Cortex for Meridian Colab Runner Service Account" \
        --display-name="Cortex Meridian Runner"
    if [ $? -eq 0 ]; then
        fancy_sub_echo "Service account '${MERIDIAN_RUNNER_SERVICE_ACCOUNT_EMAIL}' created successfully 🔐"
    else
        fancy_error_echo "Error creating service account '${SERVICE_ACCOUNT_ID}' 🔐"
        exit 1
    fi
fi

fancy_echo_done "Done creating Service Account"

# IAM roles assignments

fancy_echo_start "Assigning IAM roles to Cloud Build Default Service Account & Meridian Colab Runner Service Account"

CLOUD_BUILD_ROLES=('roles/aiplatform.colabEnterpriseAdmin'
    'roles/storage.objectUser' 'roles/workflows.editor' 'roles/bigquery.jobUser'
    'roles/bigquery.dataEditor' 'roles/iam.serviceAccountUser')

# Get cloud build service account email
CLOUD_BUILD_SA=$(gcloud builds get-default-service-account --project="$PROJECT_ID" --format='value(serviceAccountEmail)' | sed 's|.*/||')

for role in "${CLOUD_BUILD_ROLES[@]}"; do
    fancy_sub_echo "Assigning role: $role to $CLOUD_BUILD_SA 🔑"
    gcloud projects add-iam-policy-binding "$PROJECT_ID" \
        --member="serviceAccount:$CLOUD_BUILD_SA" \
        --role="$role" \
        --condition=None \
        --no-user-output-enabled
    if [ $? -ne 0 ]; then
        fancy_error_echo "Error assigning role '$role' to service account '$CLOUD_BUILD_SA'"
        exit 1
    fi
done

fancy_sub_echo "Done assigning roles to service account '$CLOUD_BUILD_SA'"

MERIDIAN_RUNNER_ROLES=('roles/bigquery.dataViewer' 'roles/bigquery.jobUser'
    'roles/bigquery.readSessionUser' 'roles/cloudbuild.builds.editor'
    'roles/aiplatform.colabEnterpriseAdmin' 'roles/logging.logWriter'
    'roles/aiplatform.notebookRuntimeAdmin' 'roles/storage.admin'
    'roles/storage.objectUser' 'roles/aiplatform.colabServiceAgent')

for role in "${MERIDIAN_RUNNER_ROLES[@]}"; do
    fancy_sub_echo "Assigning role: $role to $MERIDIAN_RUNNER_SERVICE_ACCOUNT_EMAIL 🔑"
    gcloud projects add-iam-policy-binding "$PROJECT_ID" \
        --member="serviceAccount:$MERIDIAN_RUNNER_SERVICE_ACCOUNT_EMAIL" \
        --role="$role" \
        --condition=None \
        --no-user-output-enabled
    if [ $? -ne 0 ]; then
        fancy_error_echo "Error assigning role '$role' to service account '$MERIDIAN_RUNNER_SERVICE_ACCOUNT_EMAIL'"
        exit 1
    fi
done

fancy_sub_echo "Done assigning roles to service account '$MERIDIAN_RUNNER_SERVICE_ACCOUNT_EMAIL'"

fancy_echo_done "Done assigning IAM roles to service accounts"

# Create storage buckets
fancy_echo_start "Creating GCS storage buckets"

BUCKET_NAMES=("gs://${PROJECT_ID}-dags" "gs://${PROJECT_ID}-logs")

# Loop through the defined bucket names
for BUCKET_NAME in "${BUCKET_NAMES[@]}"; do
    fancy_sub_echo "Processing bucket: $BUCKET_NAME"

    # Check if the bucket already exists
    if gcloud storage buckets describe "${BUCKET_NAME}" --project="$PROJECT_ID" >/dev/null 2>&1; then
        fancy_sub_echo "Bucket '$BUCKET_NAME' already exists. Skipping creation"

    else
        fancy_sub_echo "Bucket '$BUCKET_NAME' does not exist. Creating ..."
        gcloud storage buckets create "$BUCKET_NAME" --project="${PROJECT_ID}" --location="${GEO_LOCATION}"
        if [ $? -eq 0 ]; then
            fancy_sub_echo "Bucket '$BUCKET_NAME' created successfully 💾"
        else
            fancy_error_echo "Error creating bucket '$BUCKET_NAME'"
            exit 1
        fi
    fi
done

fancy_echo_done "Done creating GCS storage buckets"

# Copy default config to config.json and replace placeholders.

fancy_echo_start "Creating Cortex Data Foundation config file"

CONFIG_FILE_TEMPLATE="config_meridian_1_click.json"
TARGET_CONFIG_FILE_PATH="${SCRIPT_OWN_DIR}/../../../config/config.json"
TARGET_LOGS_BUCKET="${PROJECT_ID}-logs"

fancy_sub_echo "Creating config file ${TARGET_CONFIG_FILE_PATH} ⚙️"

# Define source template path relative to script's directory
SOURCE_CONFIG_TEMPLATE_PATH="${SCRIPT_OWN_DIR}/${CONFIG_FILE_TEMPLATE}"

# Check if template file exists
if [ ! -f "${SOURCE_CONFIG_TEMPLATE_PATH}" ]; then
    fancy_error_echo "Configuration template file not found: ${SOURCE_CONFIG_TEMPLATE_PATH}"
    exit 1
fi

# Replace placeholders in the build template file
sed "s#{{PROJECT_ID_SOURCE}}#$PROJECT_ID#g; \
    s#{{PROJECT_ID_TARGET}}#$PROJECT_ID#g; \
    s#{{TARGET_BUCKET}}#$TARGET_LOGS_BUCKET#g" \
    "$SOURCE_CONFIG_TEMPLATE_PATH" >"$TARGET_CONFIG_FILE_PATH"

fancy_echo_done "Done creating Cortex Data Foundation config file"

# Inject Meridian demo model training parameters.

fancy_echo_start "Injecting demo Meridian model training params into config file."

fancy_echo_important "The default configuration parameters and sample data for Meridian are intended for demo purposes only and should not be deployed for production use. Meridian configuration parameters should be chosen with great care as they will influence the behaviour of the model and results. Please consult Meridian documentation for guidance on how to setup the model configuration for your unique business needs and goals. See Meridian modeling (https://developers.google.com/meridian/docs/basics/about-the-project). If needed consult with an official Google Meridian partner (https://developers.google.com/meridian/partners) and/or your Google Ads representative."

MERIDIAN_CONFIG_FILE_PATH_RELATIVE_TO_SCRIPT="../../k9/src/meridian/src/configuration/cortex_meridian_config.json"
MERIDIAN_CONFIG_FILE_FULL_PATH="${SCRIPT_OWN_DIR}/${MERIDIAN_CONFIG_FILE_PATH_RELATIVE_TO_SCRIPT}"

fancy_sub_echo "Injecting values into config file ${MERIDIAN_CONFIG_FILE_FULL_PATH} ⚙️"

# Create a temporary file
TEMP_CONFIG_FILE=$(mktemp)

# Replace placeholders in the build template file
sed "s#{{ROI_MU}}#0.2#g; \
    s#{{ROI_SIGMA}}#0.9#g; \
    s#{{PRIOR}}#50#g; \
    s#{{N_CHAINS}}#7#g; \
    s#{{N_ADAPT}}#500#g; \
    s#{{N_BURNIN}}#500#g; \
    s#{{N_KEEP}}#1000#g;" \
    "$MERIDIAN_CONFIG_FILE_FULL_PATH" >"$TEMP_CONFIG_FILE"

# Replace actual file with temp file
cp "$TEMP_CONFIG_FILE" "$MERIDIAN_CONFIG_FILE_FULL_PATH"

fancy_echo_done "Done injecting demo Meridian model training params into config file"

# Start deployment using Cloud Build

fancy_echo_start "Submitting Cloud Build to start deployment"

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
pushd "${SCRIPT_DIR}/../../../" 1>/dev/null # Data Foundation root

fancy_sub_echo "Deploying Cortex Data Foundation via Cloud Build👷"

source_project=$(cat "config/config.json" | python3 -c "import json,sys; print(str(json.load(sys.stdin)['projectIdSource']))" 2>/dev/null || echo "")
gcloud config set project "${source_project}"
./deploy.sh
popd 1>/dev/null

fancy_echo_done "Done Cortex Data Foundation was deployed via Cloud Build"

fancy_echo_start "Starting Colab Notebook execution"

WORKFLOW_NAME="cortex-meridian-execute-notebook"

gcloud workflows execute "${WORKFLOW_NAME}" --data="{}" --location="${REGION_LOCATION}" --project="${PROJECT_ID}" &>/dev/null
fancy_sub_echo "Notebook execution started in ${REGION_LOCATION} in project ${PROJECT_ID} ⚙️"

fancy_echo_start "Done starting Colab Notebook execution"

fancy_robot_header "Cortex for Meridian - Automated Sample Deployment Finished"