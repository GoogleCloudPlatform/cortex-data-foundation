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

# This script generates all necessary DAG files and BQ objects for SFMC.

set -e

export PYTHONPATH=$PYTHONPATH:src/SFMC/:src/

_DEPLOY_CDC=$(jq -r .marketing.SFMC.deployCDC ${_CONFIG_FILE})
_TGT_BUCKET=$(jq -r .targetBucket ${_CONFIG_FILE})

if [ ${_DEPLOY_CDC} = "true" ]
then
    echo "Deploying SFMC raw layer..."
    python src/SFMC/src/raw/deploy_raw_layer.py \
        --pipeline-temp-bucket gs://${_TGT_BUCKET}/_pipeline_temp/ \
        --pipeline-staging-bucket gs://${_TGT_BUCKET}/_pipeline_staging/
    echo "✅ SFMC raw layer deployed successfully."
    echo "========================================"

    echo "Deploying SFMC CDC layer..."
    python src/SFMC/src/cdc/deploy_cdc_layer.py
    echo "✅ SFMC CDC layer deployed successfully."
    echo "========================================"

    # Copy generated files to Target GCS bucket
    if [ ! -z "$(shopt -s nullglob dotglob; echo src/SFMC/_generated_dags/*)" ]
    then
        echo "Copying SFMC artifacts to gs://${_TGT_BUCKET}/dags/sfmc."
        gcloud storage cp -r src/SFMC/_generated_dags/* gs://${_TGT_BUCKET}/dags/sfmc/
        echo "✅ SFMC artifacts have been copied."
    else
        echo "❗ No file generated. Nothing to copy."
    fi
else
    echo "== Skipping RAW and CDC layers for SFMC =="
fi

echo "Deploying SFMC Reporting layer..."
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
    --module_name SFMC \
    --config_file ${_CONFIG_FILE} \
    --target_type "Reporting" \
    --materializer_settings_file src/SFMC/config/reporting_settings.yaml \
    "${_WORKER_POOL_OPTIONS[@]}"

echo "✅ SFMC Reporting layer deployed successfully."
echo "==================================================="
