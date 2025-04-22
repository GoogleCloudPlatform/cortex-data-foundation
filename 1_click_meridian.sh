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
#
# 1-Click Cortex for Meridian Deployment Launcher

set -e

while [[ $# -gt 0 ]]; do
    case "$1" in
    --project)
        PROJECT_IN="$2"
        shift 2
        ;;
    *)
        # Handle other arguments or flags if needed
        echo "Unknown option: $1" >&2
        shift
        ;;
    esac
done

if [ -n "$PROJECT_IN" ]; then
    echo "Starting Cortex with Meridian deployment"

    echo "Setting active project to: ${PROJECT_IN}"

    gcloud config set project "${PROJECT_IN}"
    export PROJECT_ID=$(gcloud config get-value project)

    SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
    pushd "${SCRIPT_DIR}" 1>/dev/null
    src/utils/automatic/deploy_cortex_meridian_1_click.sh
    popd 1>/dev/null

else
    echo "Error: --project argument not provided or value is missing." >&2
    exit 1
fi
