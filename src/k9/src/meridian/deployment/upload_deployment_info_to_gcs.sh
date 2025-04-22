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

# This script will upload the config file for running Meridian to GCS

set -e

PROJECT_ID=$(jq -r .projectIdTarget "${_CONFIG_FILE}")
GCS_BUCKET_NAME_SUFFIX=$(jq -r .k9.Meridian.gcsBucketNameSuffix "${_CONFIG_FILE}")
GCS_BUCKET_NAME="${PROJECT_ID}-${GCS_BUCKET_NAME_SUFFIX}"

GCS_CONFIG_FOLDER="configuration"
CORTEX_CONFIG_FILE_LOCAL_PATH="${_CONFIG_FILE}"
FULL_DEPLOYMENT_INFO_FILE_GCS_PATH="gs://${GCS_BUCKET_NAME}/${GCS_CONFIG_FOLDER}/${_DEPLOYMENT_INFO_FILE}"

if gcloud storage ls "${FULL_DEPLOYMENT_INFO_FILE_GCS_PATH}" --project="$PROJECT_ID" >/dev/null 2>&1; then
    echo "⚠️ Deployment info file already exists in $FULL_CONFIG_FILE_GCS_PATH. Will upload and replace file."
    echo "========================================"
else
    echo "Uploading deployment info file to $FULL_DEPLOYMENT_INFO_FILE_GCS_PATH..."
    gcloud storage cp "${CORTEX_CONFIG_FILE_LOCAL_PATH}" "${FULL_DEPLOYMENT_INFO_FILE_GCS_PATH}" --project="$PROJECT_ID"

    if [ $? -eq 0 ]; then
        echo "✅ Deployment info file uploaded successfully to $FULL_DEPLOYMENT_INFO_FILE_GCS_PATH."
        echo "========================================"
    else
        echo "❗ Failed to upload deployment info file."
        echo "========================================"
        exit 1
    fi
fi
