#!/bin/bash

# Copyright 2022 Google LLC
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

set -e

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

if [[ "$1" == "" || "$2" == "" ]]
then
    echo "ERROR: Config file or Logs bucket parameter is missing."
    echo "USAGE: deploy_catgap.sh CONFIG_JSON_PATH LOGS_BUCKET_NAME"
    exit 1
fi

DF_CONFIG_FILE=$1
LOGS_BUCKET=$2

if [[ ! -f "${DF_CONFIG_FILE}" ]]
then
    echo "ERROR: ${DF_CONFIG_FILE} not found."
    exit 1
fi

export SOURCE_PROJECT=$(cat ${DF_CONFIG_FILE} | python3 -c "import json,sys; print(str(json.load(sys.stdin)['projectIdSource']))" 2>/dev/null || echo "")
export TARGET_BUCKET=$(cat ${DF_CONFIG_FILE} | python3 -c "import json,sys; print(str(json.load(sys.stdin)['targetBucket']))" 2>/dev/null || echo "")
if [[ "${SOURCE_PROJECT}" == "" ]]
then
    echo "ERROR: projectIdSource value in config.json is empty."
    exit 1
fi
if [[ "${TARGET_BUCKET}" == "" ]]
then
    echo "ERROR: targetBucket value in config.json is empty."
    exit 1
fi

echo "Deploying CATGAP"

cp -f "${DF_CONFIG_FILE}" "${SCRIPT_DIR}/config/config.json"
set +e
gcloud builds submit --project="${SOURCE_PROJECT}" \
    --config="${SCRIPT_DIR}/cloudbuild.catgap.yaml" \
    --substitutions \
    _TGT_BUCKET="${TARGET_BUCKET}",_GCS_BUCKET="${LOGS_BUCKET}" \
    "${SCRIPT_DIR}"
_err=$?
rm -f "${SCRIPT_DIR}/config/config.json"

if [ $_err -ne 0 ]
then
    echo "CATGAP deployment failed."
    exit 1
fi

echo "CATGAP has been deployed."
