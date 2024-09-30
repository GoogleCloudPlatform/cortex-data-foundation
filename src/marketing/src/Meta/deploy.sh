#!/bin/bash

# Copyright 2023 Google LLC
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

# This script generates all necessary DAG files and BQ objects for Meta.

set -e

export PYTHONPATH=$PYTHONPATH:src/Meta/:src/

_DEPLOY_CDC=$(jq -r .marketing.Meta.deployCDC ${_CONFIG_FILE})
_TGT_BUCKET=$(jq -r .targetBucket ${_CONFIG_FILE})

if [ ${_DEPLOY_CDC} = "true" ]
then
    echo "Deploying Meta raw layer..."
    python src/Meta/src/raw/deploy_raw_layer.py \
        --pipeline-temp-bucket gs://${_TGT_BUCKET}/_pipeline_temp/ \
        --pipeline-staging-bucket gs://${_TGT_BUCKET}/_pipeline_staging/
    echo "✅ Meta raw layer deployed successfully."
    echo "========================================"

    echo "Deploying Meta CDC layer..."
    python src/Meta/src/cdc/deploy_cdc_layer.py
    echo "✅ Meta CDC layer deployed successfully."
    echo "========================================"

    # Copy generated files to Target GCS bucket
    if [ ! -z "$(shopt -s nullglob dotglob; echo src/Meta/_generated_dags/*)" ]
    then
        echo "Copying Meta artifacts to gs://${_TGT_BUCKET}/dags/meta."
        gsutil -m cp -r src/Meta/_generated_dags/* gs://${_TGT_BUCKET}/dags/meta/
        echo "✅ Meta artifacts have been copied."
    else
        echo "❗ No file generated. Nothing to copy."
    fi
else
    echo "== Skipping RAW and CDC layers for Meta =="
fi

# Deploy reporting layer
echo "Deploying Meta Reporting layer..."
src/common/materializer/deploy.sh \
    --gcs_logs_bucket ${_GCS_LOGS_BUCKET} \
    --gcs_tgt_bucket ${_TGT_BUCKET} \
    --module_name Meta \
    --config_file ${_CONFIG_FILE} \
    --target_type "Reporting" \
    --materializer_settings_file src/Meta/config/reporting_settings.yaml

echo "✅ Meta Reporting layer deployed successfully."
echo "==================================================="
