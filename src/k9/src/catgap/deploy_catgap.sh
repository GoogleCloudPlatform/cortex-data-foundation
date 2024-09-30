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

set -e

#--------------------
# Help Message
#--------------------
usage() {
  cat <<HELP_USAGE

Deploy Catgap based on settings.

$0 [OPTIONS]

Options:

  -c | --config-file                 : Cortex config file
  -l | --gcs-logs-bucket             : GCS Bucket for Cloud Build logs
  -h | --help                        : Display this message
  -p | --worker-pool-name            : Set worker pool for cloud build
  -r | --region                      : Set region for worker pool. Required if worker-pool-name present
  -u | --build-account               : Set user specified cloud build service account.
HELP_USAGE

}

#--------------------
# Validate parameters and provide default value
#--------------------

validate_args() {

  # Check for required parameters

  error=0
  if [ -z "${GCS_LOGS_BUCKET-}" ]; then
    echo 'ERROR: "--gcs-logs-bucket" is required. See help for details.'
    error=1
  fi
  if [ -z "${CONFIG_FILE-}" ]; then
    echo 'ERROR: "--config-file" is required. See help for details.'
    error=1
  fi
  if [[ "$error" -ne 0 ]]; then
    usage
    exit 1
  fi
}

#--------------------
# Parameters parsing
#-  -------------------

set -o errexit -o noclobber -o nounset -o pipefail
params="$(getopt -o c:l:p:r:u:h --long config-file:,gcs-logs-bucket:,worker-pool-name:,region:,build-account:,help --name "$0" -- "$@")"
eval set -- "$params"

_WORKER_POOL_NAME=""
_CLOUD_BUILD_REGION=""
_BUILD_ACCOUNT=""

while true; do
  case "$1" in
  -c | --config-file)
    CONFIG_FILE=$2
    shift 2
    ;;
  -l | --gcs-logs-bucket)
    GCS_LOGS_BUCKET=$2
    shift 2
    ;;
  -p | --worker-pool-name)
    _WORKER_POOL_NAME=$2
    shift 2
    ;;
  -r | --region)
    _REGION=$2
    shift 2
    ;;
  -u | --build-account)
    _BUILD_ACCOUNT=$2
    shift 2
    ;;
  -h | --help)
    usage
    exit
    ;;
  --)
    break
    ;;
  *)
    echo "ERROR: Invalid arguments." >&2
    usage
    echo "Please rerun the command with valid arguments." >&2
    exit 1
    ;;
  esac
done

validate_args

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

export SOURCE_PROJECT=$(cat ${CONFIG_FILE} | python3 -c "import json,sys; print(str(json.load(sys.stdin)['projectIdSource']))" 2>/dev/null || echo "")
export TARGET_BUCKET=$(cat ${CONFIG_FILE} | python3 -c "import json,sys; print(str(json.load(sys.stdin)['targetBucket']))" 2>/dev/null || echo "")
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

    declare -a _WORKER_POOL_OPTIONS
    declare -a _REGION_PARAMETER

    if [[ -n "${_WORKER_POOL_NAME}" ]]; then
      _WORKER_POOL_OPTIONS+=(",_WORKER_POOL_NAME=\"${_WORKER_POOL_NAME}\"")
    fi

    if [[ -n "${_CLOUD_BUILD_REGION}" ]]; then
      _WORKER_POOL_OPTIONS+=(",_CLOUD_BUILD_REGION=\"${_CLOUD_BUILD_REGION}\"")
      _REGION_PARAMETER=(--region "${_CLOUD_BUILD_REGION}")
    fi

    if [[ -n "${_BUILD_ACCOUNT}" ]]; then
      _WORKER_POOL_OPTIONS+=(",_BUILD_ACCOUNT=\"${_BUILD_ACCOUNT}\"")
    fi


echo "Deploying CATGAP"

cp -f "${CONFIG_FILE}" "${SCRIPT_DIR}/config/config.json"
set +e
gcloud builds submit --project="${SOURCE_PROJECT}" \
    --config="${SCRIPT_DIR}/cloudbuild.catgap.yaml" \
    --substitutions \
    _TGT_BUCKET="${TARGET_BUCKET}",_GCS_BUCKET="${GCS_LOGS_BUCKET}" "${_WORKER_POOL_OPTIONS[@]}" \
    "${_REGION_PARAMETER[@]}" \
    "${SCRIPT_DIR}"
_err=$?
rm -f "${SCRIPT_DIR}/config/config.json"

if [ $_err -ne 0 ]
then
    echo "CATGAP deployment failed."
    exit 1
fi

echo "CATGAP has been deployed."
