#!/bin/bash
# Copyright 2023 Google Inc
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Script to perform Data Foundation Deployment

# Exit on error.

set -e

echo -e "\n============ 🦄🦄🦄 Getting logs and configs! 🔪🔪🔪 ============\n"

project_id=$(jq -r ."projectIdSource" "config/config.json")
if [[ "${project_id}" == "" ]]
then
    echo "ERROR: Configuration in config/config.json is invalid. 'projectIdSource' is empty."
    exit 1
fi

echo "Retrieving the most recent failed Cortex deployment build in project ${project_id}."
failed_build=$(gcloud builds list --filter "status=FAILURE AND tags:cortex" --format "value(ID)" --sort-by=~CREATE_TIME --limit=1 --project "${project_id}")
if [[ "${failed_build}" == "" ]]
then
    echo "ERROR: Cannot find the latest failed Cortex deployment build."
    echo "This may be due to insufficient permissions. Please try the build again and share all the error messages with cortex-support@google.com"
    exit 1
fi

log_file="cortex_${failed_build}.log"
echo "The build id is '$failed_build'"
rm -rf $log_file
echo "Retrieving logs to ${log_file}"
gcloud builds log ${failed_build} --project "${project_id}" > "${log_file}"
echo "Your logs are in ${log_file}. Please share it with cortex-support@google.com."
echo ""

cp -f config/config.json to_be_uploaded.json
if [ $? -ne 0 ]
    then
        echo "Warning: config.json not found in your local."
        exit 1
    else
        echo "Your configs are in to_be_uploaded.json. Please share it with cortex-support@google.com."
fi
