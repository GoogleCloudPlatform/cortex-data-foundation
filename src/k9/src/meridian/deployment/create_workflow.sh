#!/bin/bash

# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This script will create the a Cloud Build Trigger to start a Colab Enterprise notebook execution based on the runtime template

set -e

echo "Deploying Cloud Workflow..."

PROJECT_ID=$(jq -r .projectIdTarget "${_CONFIG_FILE}")
GCS_BUCKET_NAME_SUFFIX=$(jq -r .k9.Meridian.gcsBucketNameSuffix "${_CONFIG_FILE}")
GCS_BUCKET_NAME="${PROJECT_ID}-${GCS_BUCKET_NAME_SUFFIX}"

TEMPLATE_FILE=$(jq -r .k9.Meridian.workflow.template "${_CONFIG_FILE}")
FUll_TEMPLATE_FILE_LOCAL_PATH="/workspace/src/workflows/${TEMPLATE_FILE}"

WORKFLOW_NAME=$(jq -r .k9.Meridian.workflow.name "${_CONFIG_FILE}")
WORKFLOW_SA=$(jq -r .k9.Meridian.runnerServiceAccount "${_CONFIG_FILE}")
WORKFLOW_SA="${WORKFLOW_SA}@${PROJECT_ID}.iam.gserviceaccount.com"
WORKFLOW_REGION=$(jq -r .k9.Meridian.workflow.region "${_CONFIG_FILE}")

COLAB_REGION=$(jq -r .k9.Meridian.colabEnterprise.region "${_CONFIG_FILE}")
COLAB_RUNTIME_TEMPLATE_NAME=$(jq -r .k9.Meridian.colabEnterprise.runtimeTemplateName "${_CONFIG_FILE}")
COLAB_EXECUTION_JOB_NAME=$(jq -r .k9.Meridian.colabEnterprise.executionName "${_CONFIG_FILE}")
GCS_NOTEBOOK_FOLDER="notebooks"
NOTEBOOK_FILE=$(jq -r .k9.Meridian.defaultNotebookFile "${_CONFIG_FILE}")
FULL_NOTEBOOK_PATH="gs://${GCS_BUCKET_NAME}/${GCS_NOTEBOOK_FOLDER}/${NOTEBOOK_FILE}"
NOTEBOOK_RUN_LOGS_FOLDER=$(jq -r .k9.Meridian.colabEnterprise.notebookRunLogsFolder "${_CONFIG_FILE}")
FULL_OUTPUT_PATH="gs://${GCS_BUCKET_NAME}/${NOTEBOOK_RUN_LOGS_FOLDER}"
COLAB_EXECUTION_SA=$(jq -r .k9.Meridian.runnerServiceAccount "${_CONFIG_FILE}")
COLAB_EXECUTION_SA="${COLAB_EXECUTION_SA}@${PROJECT_ID}.iam.gserviceaccount.com"

# Create a temporary file
TEMP_WORKFLOW_DEFINITION_FILE=$(mktemp)

# Replace placeholders in the build template file
sed "s#{{REGION}}#$COLAB_REGION#g; \
     s#{{PROJECT_ID}}#$PROJECT_ID#g; \
     s#{{COLAB_RUNTIME_TEMPLATE_NAME}}#$COLAB_RUNTIME_TEMPLATE_NAME#g; \
     s#{{COLAB_EXECUTION_JOB_NAME}}#$COLAB_EXECUTION_JOB_NAME#g; \
     s#{{FULL_NOTEBOOK_PATH}}#$FULL_NOTEBOOK_PATH#g; \
     s#{{FULL_OUTPUT_PATH}}#$FULL_OUTPUT_PATH#g; \
     s#{{MERIDIAN_BUCKET_NAME}}#$GCS_BUCKET_NAME#g; \
     s#{{COLAB_EXECUTION_SA}}#$COLAB_EXECUTION_SA#g" \
    "$FUll_TEMPLATE_FILE_LOCAL_PATH" >"$TEMP_WORKFLOW_DEFINITION_FILE"

# Check if the workflow already exists
json_output=$(gcloud workflows list --location="${WORKFLOW_REGION}" --project="$PROJECT_ID" --filter="name:${WORKFLOW_NAME}" --format=json --quiet)
exists=false

# Check if the output is empty (meaning no matching templates were found)
if [[ -z "$json_output" ]]; then
    echo "Workflow '$WORKFLOW_NAME' does not exist in $WORKFLOW_REGION."
    echo "Exists = false"
    exists=false
else
    #Check if the json is an empty array.
    if echo "$json_output" | jq -e 'length == 0' >/dev/null; then
        echo "Workflow '$WORKFLOW_NAME' does not exist in $WORKFLOW_REGION."
        echo "Exists = false"
        exists=false
    else
        echo "✅ Workflow $WORKFLOW_NAME already exists."
        echo "========================================"
        exists=true
    fi
fi

if [[ "$exists" == "false" ]]; then
    echo "Creating workflow..."

    # Create workflow
    gcloud workflows deploy "${WORKFLOW_NAME}" \
        --location="${WORKFLOW_REGION}" \
        --source="${TEMP_WORKFLOW_DEFINITION_FILE}" \
        --project="$PROJECT_ID" \
        --service-account="${WORKFLOW_SA}"

    if [ $? -eq 0 ]; then
        echo "✅ Workflow '$WORKFLOW_NAME' created successfully."
        echo "========================================"
    else
        echo "❗ Error creating workflow '$WORKFLOW_NAME'."
        echo "========================================"
        exit 1
    fi
fi
