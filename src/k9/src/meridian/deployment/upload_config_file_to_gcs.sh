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
MERIDIAN_CONFIG_FILE=$(jq -r .k9.Meridian.defaultConfigFile "${_CONFIG_FILE}")
FULL_CONFIG_FILE_LOCAL_PATH="/workspace/src/configuration/${MERIDIAN_CONFIG_FILE}"
FULL_CONFIG_FILE_GCS_PATH="gs://${GCS_BUCKET_NAME}/${GCS_CONFIG_FOLDER}/${MERIDIAN_CONFIG_FILE}"

CORTEX_BQ_K9_PROJECT_ID=$(jq -r .projectIdTarget "${_CONFIG_FILE}")
CORTEX_BQ_K9_DATASET_ID=$(jq -r .k9.datasets.reporting "${_CONFIG_FILE}")

DATASOURCE_TYPE=$(jq -r .k9.Meridian.salesDataSourceType "${_CONFIG_FILE}")
echo "Data source type: $DATASOURCE_TYPE"

CORTEX_MERIDIAN_VIEW_NAME="CrossMediaSalesInsightsWeeklyAgg"

if [[ "$DATASOURCE_TYPE" == "BYOD" ]]; then
    REVENUE_PER_KPI=""
    KPI="conversions"
    KPI_TYPE="non_revenue"

elif [[ "$DATASOURCE_TYPE" == "SAP" ]]; then
    REVENUE_PER_KPI="average_revenue_per_sales_order"
    KPI="number_of_sales_orders"
    KPI_TYPE="non_revenue"
    
elif [[ "$DATASOURCE_TYPE" == "OracleEBS" ]]; then
    REVENUE_PER_KPI="average_revenue_per_sales_order"
    KPI="number_of_sales_orders"
    KPI_TYPE="non_revenue"
else
    echo "❗ Unknown option for data source type: $DATASOURCE_TYPE."
    echo "========================================"
    exit 1
fi

# Create a temporary file
TEMP_CONFIG_FILE=$(mktemp)

# Replace placeholders in the build template file
sed "s#{{CORTEX_BQ_K9_PROJECT_ID}}#$CORTEX_BQ_K9_PROJECT_ID#g; \
     s#{{CORTEX_BQ_K9_DATASET_ID}}#$CORTEX_BQ_K9_DATASET_ID#g; \
     s#{{CORTEX_MERIDIAN_VIEW_NAME}}#$CORTEX_MERIDIAN_VIEW_NAME#g; \
     s#{{KPI}}#$KPI#g; \
     s#{{REVENUE_PER_KPI}}#$REVENUE_PER_KPI#g; \
     s#{{ROI_MU}}#0#g; \
     s#{{ROI_SIGMA}}#0#g; \
     s#{{PRIOR}}#0#g; \
     s#{{N_CHAINS}}#0#g; \
     s#{{N_ADAPT}}#0#g; \
     s#{{N_BURNIN}}#0#g; \
     s#{{N_KEEP}}#0#g; \
     s#{{KPI_TYPE}}#$KPI_TYPE#g" \
     "$FULL_CONFIG_FILE_LOCAL_PATH" >"$TEMP_CONFIG_FILE"

if gcloud storage ls "${FULL_CONFIG_FILE_GCS_PATH}" --project="$PROJECT_ID" >/dev/null 2>&1; then
    echo "✅ File $MERIDIAN_CONFIG_FILE already exists in $FULL_CONFIG_FILE_GCS_PATH. Skipping upload."
    echo "========================================"
else
    echo "Uploading $MERIDIAN_CONFIG_FILE to $FULL_CONFIG_FILE_GCS_PATH..."
    gcloud storage cp "${TEMP_CONFIG_FILE}" "${FULL_CONFIG_FILE_GCS_PATH}" --project="$PROJECT_ID"

    if [ $? -eq 0 ]; then
        echo "✅ File $MERIDIAN_CONFIG_FILE uploaded successfully to $FULL_CONFIG_FILE_GCS_PATH."
        echo "========================================"
    else
        echo "❗ Failed to upload file $MERIDIAN_CONFIG_FILE."
        echo "========================================"
        exit 1
    fi
fi
