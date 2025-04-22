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

# This script will upload the notebook for running Meridian to GCS

set -e

PROJECT_ID=$(jq -r .projectIdTarget "${_CONFIG_FILE}")
GCS_BUCKET_NAME_SUFFIX=$(jq -r .k9.Meridian.gcsBucketNameSuffix "${_CONFIG_FILE}")
GCS_BUCKET_NAME="${PROJECT_ID}-${GCS_BUCKET_NAME_SUFFIX}"

GCS_NOTEBOOK_FOLDER="notebooks"
NOTEBOOK_FILE=$(jq -r .k9.Meridian.defaultNotebookFile "${_CONFIG_FILE}")
FULL_NOTEBOOK_FILE_LOCAL_PATH="/workspace/src/notebooks/${NOTEBOOK_FILE}"
FULL_NOTEBOOK_FILE_GCS_PATH="gs://${GCS_BUCKET_NAME}/${GCS_NOTEBOOK_FOLDER}/${NOTEBOOK_FILE}"

TESTED_CORTEX_VERSION=$(jq -r .cortexVersion "${_CONFIG_FILE}")
TESTED_MERIDIAN_VERSION="1.0.5"

# Create a temporary file
TEMP_NOTEBOOK_FILE=$(mktemp)

# Replace placeholders in the notebook file
sed "s#{{CORTEX_BQ_K9_PROJECT_ID}}#$CORTEX_BQ_K9_PROJECT_ID#g; \
     s#{{TESTED_CORTEX_VERSION}}#$TESTED_CORTEX_VERSION#g; \
     s#{{TESTED_MERIDIAN_VERSION}}#$TESTED_MERIDIAN_VERSION#g; \
     s#{{BUCKET_SUFFIX}}#$GCS_BUCKET_NAME_SUFFIX#g" \
    "$FULL_NOTEBOOK_FILE_LOCAL_PATH" >"$TEMP_NOTEBOOK_FILE"

if gcloud storage ls "${FULL_NOTEBOOK_FILE_GCS_PATH}" --project="$PROJECT_ID" >/dev/null 2>&1; then
    echo "✅ File $NOTEBOOK_FILE already exists in $FULL_NOTEBOOK_FILE_GCS_PATH. Skipping upload."
    echo "========================================"
else
    echo "Uploading $NOTEBOOK_FILE to $FULL_NOTEBOOK_FILE_GCS_PATH..."
    gcloud storage cp "${TEMP_NOTEBOOK_FILE}" "${FULL_NOTEBOOK_FILE_GCS_PATH}" --project="$PROJECT_ID"

    if [ $? -eq 0 ]; then
        echo "✅ File $NOTEBOOK_FILE uploaded successfully to $FULL_NOTEBOOK_FILE_GCS_PATH."
        echo "========================================"
    else
        echo "❗ Failed to upload file $NOTEBOOK_FILE."
        echo "========================================"
        exit 1
    fi
fi
