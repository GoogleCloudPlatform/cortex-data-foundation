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

# This script deploys BigQuery objects based on configurable settings.

set -e

#--------------------
# Help Message
#--------------------
usage() {
  cat <<HELP_USAGE

Generates and deploys Cortex BQ objects based on settings for a given module.

$0 [OPTIONS]

Options:

  -l | --gcs_logs_bucket             : GCS Bucket for Cloud Build logs
  -t | --gcs_tgt_bucket              : GCS Bucket where DAG files are copied to
  -m | --module_name                 : Module name ("SAP", "SFDC" etc)
  -y | --target_type                 : Target dataset type to deploy to ("CDC"/"Reporting"), case insensitive
  -c | --config_file                 : Cortex config file
  -f | --materializer_settings_file  : Deployment settings file for Materializer
  -h | --help                        : Display this message
  -p | --worker_pool_name            : Set worker pool for cloud build
  -r | --region                      : Set region for worker pool. Required if worker_pool_name present
  -u | --build_account               : Set user specified cloud build service account.
  -g | --gcs_bucket                  : GCS bucket for the cloud build account. Required if build-account present

HELP_USAGE

}

#--------------------
# Validate parameters and provide default value
#--------------------

validate_args() {

  # Check for required parameters

  error=0
  if [ -z "${GCS_LOGS_BUCKET-}" ]; then
    echo 'ERROR: "--gcs_logs_bucket" is required. See help for details.'
    error=1
  fi
  if [ -z "${GCS_TGT_BUCKET-}" ]; then
    echo 'ERROR: "--gcs_tgt_bucket" is required. See help for details.'
    error=1
  fi
  if [ -z "${MODULE_NAME-}" ]; then
    echo 'ERROR: "--module_name" is required. See help for details.'
    error=1
  fi
  if [ -z "${CONFIG_FILE-}" ]; then
    echo 'ERROR: "--config_file" is required. See help for details.'
    error=1
  fi
  if [ -z "${MATERIALIZER_SETTINGS_FILE-}" ]; then
    echo 'ERROR: "--materializer_settings_file" is required. See help for details.'
    error=1
  fi
  if [[ "$error" -ne 0 ]]; then
    usage
    exit 1
  fi

  # Default values for optional parameters

  if [ -z "${TGT_DATASET_TYPE-}" ]; then
    echo 'NOTE: "--target_type is not set. Defaulting type to "Reporting".'
    TGT_DATASET_TYPE="Reporting"
  fi
}

#--------------------
# Parameters parsing
#-  -------------------

set -o errexit -o noclobber -o nounset -o pipefail
params="$(getopt -o l:t:y:m:c:f:k:h:p:r:v:g --long gcs_logs_bucket:,gcs_tgt_bucket:,target_type:,module_name:,config_file:,materializer_settings_file:,k9_manifest:,worker_pool_name:,region:,build_account:,gcs_bucket:,help --name "$0" -- "$@")"
eval set -- "$params"

while true; do
  case "$1" in
  -l | --gcs_logs_bucket)
    GCS_LOGS_BUCKET=$2
    shift 2
    ;;
  -t | --gcs_tgt_bucket)
    GCS_TGT_BUCKET=$2
    shift 2
    ;;
  -m | --module_name)
    MODULE_NAME=$2
    shift 2
    ;;
  -y | --target_type)
    TGT_DATASET_TYPE=$2
    shift 2
    ;;
  -c | --config_file)
    CONFIG_FILE=$2
    shift 2
    ;;
  -f | --materializer_settings_file)
    MATERIALIZER_SETTINGS_FILE=$2
    shift 2
    ;;
  -k | --k9_manifest)
    K9_MANIFEST_FILE=$2
    shift 2
    ;;
  -p | --worker_pool_name)
    _WORKER_POOL_NAME=$2
    shift 2
    ;;
  -r | --region)
    _REGION=$2
    shift 2
    ;;
  -u | --build_account)
    _BUILD_ACCOUNT=$2
    shift 2
    ;;
  -g | --gcs_bucket)
    _GCS_BUCKET=$2
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

set +o noclobber +o nounset +o pipefail

echo "Deploying ${TGT_DATASET_TYPE}...."
echo "Starting build file generation..."
echo "Arguments:"
echo "  GCS_LOGS_BUCKET = '${GCS_LOGS_BUCKET}'"
echo "  GCS_TGT_BUCKET = '${GCS_TGT_BUCKET}'"
echo "  MODULE_NAME = '${MODULE_NAME}'"
echo "  TGT_DATASET_TYPE = '${TGT_DATASET_TYPE}'"
echo "  CONFIG_FILE = '${CONFIG_FILE}'"
echo "  K9_MANIFEST_FILE = '${K9_MANIFEST_FILE}'"
echo "  MATERIALIZER_SETTINGS_FILE = '${MATERIALIZER_SETTINGS_FILE}'"

# Set various directories that help navigate to various things.
THIS_DIR=$(dirname "$0")
GENERATED_FILES_PARENT_DIR="generated_materializer_build_files"

export PYTHONPATH=$PYTHONPATH:.

# Generate Actual Build files that will create relevant BQ objects.
echo "Executing generate_build_files.py"
python3 "$THIS_DIR"/generate_build_files.py \
  --module_name "${MODULE_NAME}" \
  --config_file "${CONFIG_FILE}" \
  --target_dataset_type "${TGT_DATASET_TYPE}" \
  --materializer_settings_file "${MATERIALIZER_SETTINGS_FILE}" \
  --k9_manifest_file "${K9_MANIFEST_FILE}" \
  --private_worker_pool "${_WORKER_POOL_NAME}"

echo "Build files generated successfully."

# We may have one or more build files. Let's run all of them.
set +e
failure=0
echo "Executing generated gcloud build files...."
for build_file_name in "${GENERATED_FILES_PARENT_DIR}"/"${MODULE_NAME}"/cloudbuild.materializer.*.yaml; do
  [[ -e "$build_file_name" ]] || break
  echo -e "gcloud builds submit . --config=\"${build_file_name}\" --substitutions=_GCS_LOGS_BUCKET=\"${GCS_LOGS_BUCKET}\",_GCS_TGT_BUCKET=\"${GCS_TGT_BUCKET}\" "
  gcloud builds submit . --config="${build_file_name}" --substitutions=_GCS_LOGS_BUCKET="${GCS_LOGS_BUCKET}",_GCS_TGT_BUCKET="${GCS_TGT_BUCKET}",_WORKER_POOL_NAME="${_WORKER_POOL_NAME}",_CLOUD_BUILD_REGION="${_REGION}",_BUILD_ACCOUNT="${_BUILD_ACCOUNT}",_GCS_BUCKET="${_GCS_BUCKET}" --region="${_REGION}"
  # shellcheck disable=SC2181
  if [ $? -ne 0 ]; then
    failure=1
  fi
done
echo "Generated gcloud build files executed."

set -e

echo "Executing generate_dependent_dags.py"
python3 "$THIS_DIR"/generate_dependent_dags.py \
  --module_name "${MODULE_NAME}" \
  --target_dataset_type "${TGT_DATASET_TYPE}" \
  --config_file "${CONFIG_FILE}" \
  --materializer_settings_file "${MATERIALIZER_SETTINGS_FILE}"

echo "generate_dependent_dags.py completed successfully."

# Copy generated files to GCS target bucket if task dependent dags were generated.
if [[ $(find generated_materializer_dag_files/*/*/task_dep_dags -type f 2> /dev/null | wc -l) -gt 0 ]]
then
  echo "Copying DAG files to GCS bucket..."
  echo "gcloud storage cp --recursive 'generated_materializer_dag_files/*' gs://${GCS_TGT_BUCKET}/dags/"
  gcloud storage cp --recursive 'generated_materializer_dag_files/*' "gs://${GCS_TGT_BUCKET}/dags/"
else
  echo "No task dependent DAG files to copy to GCS bucket!"
fi

if [ "${failure}" -eq "0" ]; then
  echo "Deployment of ${TGT_DATASET_TYPE} for ${MODULE_NAME} completed successful."
else
  echo "Deployment of ${TGT_DATASET_TYPE} for ${MODULE_NAME} failed. Please check logs."
  exit 1
fi
