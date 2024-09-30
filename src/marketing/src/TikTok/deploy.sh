#!/bin/bash

# Copyright 2024 Google LLC
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

# This script generates all necessary DAG files and BQ objects for TikTok.

set -e

export PYTHONPATH=$PYTHONPATH:src/TikTok/:src/

_DEPLOY_CDC=$(jq -r .marketing.TikTok.deployCDC ${_CONFIG_FILE})
_TGT_BUCKET=$(jq -r .targetBucket ${_CONFIG_FILE})

if [ ${_DEPLOY_CDC} = "true" ]
then
    echo "Deploying TikTok raw layer..."
    python src/TikTok/src/raw/deploy_raw_layer.py \
        --pipeline-temp-bucket gs://${_TGT_BUCKET}/_pipeline_temp/ \
        --pipeline-staging-bucket gs://${_TGT_BUCKET}/_pipeline_staging/
    echo "✅ TikTok raw layer deployed successfully."
    echo "========================================"

    echo "Deploying TikTok CDC layer..."
    python src/TikTok/src/cdc/deploy_cdc_layer.py
    echo "✅ TikTok CDC layer deployed successfully."
    echo "========================================"

    # Copy generated files to Target GCS bucket
    if [ ! -z "$(shopt -s nullglob dotglob; echo src/TikTok/_generated_dags/*)" ]
    then
        echo "Copying TikTok artifacts to gs://${_TGT_BUCKET}/dags/tiktok."
        gcloud storage cp -r src/TikTok/_generated_dags/* gs://${_TGT_BUCKET}/dags/tiktok/
        echo "✅ TikTok artifacts have been copied."
    else
        echo "❗ No file generated. Nothing to copy."
    fi
else
    echo "== Skipping RAW and CDC layers for TikTok =="
fi

# Deploy reporting layer
echo "Deploying TikTok Reporting layer..."
declare -a _WORKER_POOL_OPTIONS

if [[ -n "${_WORKER_POOL_NAME}" ]]; then
_WORKER_POOL_OPTIONS+=(--worker_pool_name "${_WORKER_POOL_NAME}")
fi

if [[ -n "${_CLOUD_BUILD_REGION}" ]]; then
_WORKER_POOL_OPTIONS+=(--region "${_CLOUD_BUILD_REGION}")
fi

src/common/materializer/deploy.sh \
    --gcs_logs_bucket ${_GCS_LOGS_BUCKET} \
    --gcs_tgt_bucket ${_TGT_BUCKET} \
    --module_name TikTok \
    --config_file ${_CONFIG_FILE} \
    --target_type "Reporting" \
    --materializer_settings_file src/TikTok/config/reporting_settings.yaml \
    "${_WORKER_POOL_OPTIONS[@]}"

echo "✅ TikTok Reporting layer deployed successfully."
echo "==================================================="
