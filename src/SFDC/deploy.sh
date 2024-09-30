#!/bin/bash

# Copyright 2022 Google LLC

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This script executes cloudbuild.sfdc.yaml with Cloud Build.
# It accepts Logs Bucket name as the only parameter.
# If no parameter is provided, the name is constructed from the active project,
# assuming the bucket exists.

GCS_LOGS_BUCKET="$1"

echo -e "🦄🦄🦄 Running Cortex Data Foundation modules for SalesForce.com 🔪🔪🔪\n"

if [[ "${GCS_LOGS_BUCKET}" == "" ]]
then
    echo "No Build Logs Bucket name provided."
    cloud_build_project=$(gcloud config list --format 'value(core.project)' 2>/dev/null)
    GCS_LOGS_BUCKET="${cloud_build_project}_cloudbuild"
    echo "Using ${GCS_LOGS_BUCKET}"
fi

gcloud builds submit . \
        --config=cloudbuild.sfdc.yaml \
        --substitutions=_GCS_LOGS_BUCKET="${GCS_LOGS_BUCKET}"
