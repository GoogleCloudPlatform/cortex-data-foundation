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

# This script will create the Colab Enterprise runtime template

set -e

PROJECT_ID=$(jq -r .projectIdTarget ${_CONFIG_FILE})
REGION=$(jq -r .k9.Meridian.colabEnterprise.region ${_CONFIG_FILE})
TEMPLATE_NAME=$(jq -r .k9.Meridian.colabEnterprise.runtimeTemplateName ${_CONFIG_FILE})
MACHINE_TYPE=$(jq -r .k9.Meridian.colabEnterprise.runtimeMachine_type ${_CONFIG_FILE})
ACCELERATOR_CORES=$(jq -r .k9.Meridian.colabEnterprise.runtimeAcceleratorCoreCount ${_CONFIG_FILE})
ACCELERATOR_TYPE=$(jq -r .k9.Meridian.colabEnterprise.runtimeAcceleratorType ${_CONFIG_FILE})

echo "Deploying Colab Enterprise runtime template..."

# Check if the runtime template already exists
json_output=$(gcloud colab runtime-templates list --region="$REGION" --project="$PROJECT_ID" --filter="displayName:$TEMPLATE_NAME" --format=json --quiet)
exists=false
# Check if the output is empty (meaning no matching templates were found)
if [[ -z "$json_output" ]]; then
    echo "Runtime template '$TEMPLATE_NAME' does not exist in $REGION."
    exists=false
else
    #Check if the json is an empty array.
    if echo "$json_output" | jq -e 'length == 0' >/dev/null; then
        echo "Runtime template '$TEMPLATE_NAME' does not exist in $REGION."
        exists=false
    else
        echo "✅ Runtime template '$TEMPLATE_NAME' already exists in $REGION. Skipping creation."
        echo "========================================"
        exists=true
    fi
fi

if [[ "$exists" == "false" ]]; then
    echo "Creating runtime template '$TEMPLATE_NAME' in $REGION." "$YELLOW"

    gcloud colab runtime-templates create \
        --display-name="$TEMPLATE_NAME" \
        --region="$REGION" \
        --machine-type="$MACHINE_TYPE" \
        --project="$PROJECT_ID" \
        --accelerator-type="$ACCELERATOR_TYPE" \
        --accelerator-count="$ACCELERATOR_CORES"

    # shellcheck disable=SC2181
    if [ $? -eq 0 ]; then
        echo "✅ Runtime template '$TEMPLATE_NAME' created successfully."
        echo "========================================"
    else
        echo "❗ Error creating runtime template."
        echo "========================================"
        exit 1
    fi
fi
