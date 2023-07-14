#!/bin/bash

# Copyright 2023 Google LLC

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Exit on error.
set -e

log_bucket=$1
echo "Deploying Cortex Data Foundation."

cloud_build_project=$(cat "config/config.json" | python3 -c "import json,sys; print(str(json.load(sys.stdin)['projectIdSource']))" 2>/dev/null || echo "")
if [[ "${cloud_build_project}" == "" ]]
then
    echo "ERROR: Cortex Data Foundation is not configured."
    echo "Please read https://github.com/GoogleCloudPlatform/cortex-data-foundation/blob/main/README.md"
    exit 1
fi
echo "Using Cloud Build in project '${cloud_build_project}'"

if [[ "${log_bucket}" == "" ]]
then
    _GCS_BUCKET="${cloud_build_project}_cloudbuild"
else
    _GCS_BUCKET="${log_bucket}"
fi
echo "Using logs bucket ${_GCS_BUCKET}"

set +e
echo -e "\n\033[0;32m\033[1mPlease wait while Data Foundation is being deployed...\033[0m\n"
gcloud builds submit --config=cloudbuild.yaml --suppress-logs \
    --project "${cloud_build_project}" \
    --substitutions=_GCS_BUCKET="${_GCS_BUCKET}" . \
    && _SUCCESS="true"
if [[ "${_SUCCESS}" != "true" ]]; then
    echo -e "\n🛑 Data Foundation deployment has failed. 🛑"
    exit 1
else
    echo -e "\n✅ Data Foundation has been successfully deployed. 🦄"
fi
