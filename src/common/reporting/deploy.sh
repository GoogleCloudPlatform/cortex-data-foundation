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

# This script deploys BigQuery Reporting dataset objects based on
# configurable settings.

# Arguments:
#  $1: GCS Bucket Name for Cloud Build Logs
#  $2: Module Name
#  $3: Directory Containing Reporting module
#  $3: Cortex Level Config file
#  $3: DataSource Level reporting setting file.

set -e

#--------------------
# Help Message
#--------------------
usage() {
    cat <<HELP_USAGE

Generates and deploys Cortex Reporting artficats for a given module.

$0 [OPTIONS]

Options:

  -l | --gcs_logs_bucket          : GCS Bucket for Cloud Build logs
  -t | --gcs_tgt_bucket           : GCS Bucket where DAG files are copied to
  -m | --module_name              : Module name ('SAP', 'SFDC' etc)
  -c | --config_file              : Cortex config file
  -r | --reporting_settings_file  : File with reporting settings
  -h | --help                     : Display this message

HELP_USAGE

}

#--------------------
# Parameters parsing
#--------------------

set -o errexit -o noclobber -o nounset -o pipefail
params="$(getopt -o l:t:m:c:r:h --long gcs_logs_bucket:,gcs_tgt_bucket:,module_name:,config_file:,reporting_settings_file:,help --name "$0" -- "$@")"
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
    -c | --config_file)
        CONFIG_FILE=$2
        shift 2
        ;;
    -r | --reporting_settings_file)
        REPORTING_SETTINGS_FILE=$2
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

set +o noclobber +o nounset +o pipefail

echo "Deploying Reporting...."
echo "Starting build file generation..."
echo "Arguments:"
echo "  GCS_LOGS_BUCKET = '${GCS_LOGS_BUCKET}'"
echo "  GCS_TGT_BUCKET = '${GCS_TGT_BUCKET}'"
echo "  MODULE_NAME = '${MODULE_NAME}'"
echo "  CONFIG_FILE = '${CONFIG_FILE}'"
echo "  REPORTING_SETTINGS_FILE = '${REPORTING_SETTINGS_FILE}'"

# Set various directories that help navigate to various things.
THIS_DIR=$(dirname "$0")
GENERATED_FILES_DIR="generated_reporting_build_files"

export PYTHONPATH=$PYTHONPATH:.

# Generate Actual Build files that will create reporting BQ objects.
echo "Executing generate_build_files.py"
python3 "$THIS_DIR"/generate_build_files.py \
    --module_name "${MODULE_NAME}" \
    --config_file "${CONFIG_FILE}" \
    --reporting_settings_file "${REPORTING_SETTINGS_FILE}"

echo "Build files generated successfully."

# We may have one or more build files. Let's run all of them.
set +e
failure=0
echo "Executing generated gcloud build files...."
for build_file_name in "${GENERATED_FILES_DIR}"/cloudbuild."${MODULE_NAME}".reporting.create_bq_objects.*.yaml; do
    [[ -e "$build_file_name" ]] || break
    echo -e "gcloud builds submit . --config=\"${build_file_name}\" --substitutions=_GCS_LOGS_BUCKET=\"${GCS_LOGS_BUCKET}\",_GCS_TGT_BUCKET=\"${GCS_TGT_BUCKET}\" "
    gcloud builds submit . --config="${build_file_name}" --substitutions=_GCS_LOGS_BUCKET="${GCS_LOGS_BUCKET}",_GCS_TGT_BUCKET="${GCS_TGT_BUCKET}"
    # shellcheck disable=SC2181
    if [ $? -ne 0 ]; then
        failure=1
    fi
done
echo "Generated gcloud build files executed."

set -e

if [ "${failure}" -eq "0" ]; then
    echo "Reporting for ${MODULE_NAME} deployed successfully."
else
    echo "Reporting deployment for ${MODULE_NAME} failed. Please check logs."
    exit 1
fi
