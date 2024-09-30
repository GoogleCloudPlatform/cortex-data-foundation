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

# This script generates all necessary DAG files and BQ objects for GA4.

set -e

export PYTHONPATH=$PYTHONPATH:src/GA4/:src/

_TGT_BUCKET=$(jq -r .targetBucket ${_CONFIG_FILE})

echo "Deploying Google Analytics 4 Reporting layer..."
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
    --module_name GA4 \
    --config_file ${_CONFIG_FILE} \
    --target_type "Reporting" \
    --materializer_settings_file src/GA4/config/reporting_settings.yaml \
    "${_WORKER_POOL_OPTIONS[@]}"

echo "✅ Google Analytics 4 Reporting layer deployed successfully."
echo "==================================================="
