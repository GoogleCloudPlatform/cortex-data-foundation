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

# This script will create the GCS bucket for all Cortex for Meridian artifacts

set -e

PROJECT_ID=$(jq -r .projectIdTarget ${_CONFIG_FILE})
BUCKET_LOCATION=$(jq -r .location ${_CONFIG_FILE})
GCS_BUCKET_NAME_SUFFIX=$(jq -r .k9.Meridian.gcsBucketNameSuffix "${_CONFIG_FILE}")
GCS_BUCKET_NAME="gs://${PROJECT_ID}-${GCS_BUCKET_NAME_SUFFIX}"

echo "Deploying Cortex for Meridian GCS bucket..."
if gcloud storage buckets describe "${GCS_BUCKET_NAME}" --project="$PROJECT_ID" >/dev/null 2>&1; then
    echo "✅ Bucket already exists in ${PROJECT_ID}."
    echo "========================================"
else
    gcloud storage buckets create \
        "${GCS_BUCKET_NAME}" \
        --location="$BUCKET_LOCATION" \
        --uniform-bucket-level-access \
        --project="$PROJECT_ID"
    if [ $? -eq 0 ]; then
        echo "✅ Bucket created successfully in ${PROJECT_ID}."
        echo "========================================"
    else
        echo "❗ Failed to create bucket in ${PROJECT_ID}."
        echo "========================================"
        exit 1
    fi
fi
