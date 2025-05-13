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

log_bucket=$1
build_account="$2"

echo "Deploying CDC and extra data."

if [[ "${log_bucket}" == "" ]]
then
    echo "No Build Logs Bucket name provided."
    cloud_build_project=$(gcloud config list --format 'value(core.project)' 2>/dev/null)
    _GCS_BUCKET="${cloud_build_project}_cloudbuild"
    echo "Using ${_GCS_BUCKET}"
else
    _GCS_BUCKET="${log_bucket}"
fi

gcloud builds submit --config=cloudbuild.cdc.yaml --substitutions=_GCS_BUCKET="${_GCS_BUCKET}",_BUILD_ACCOUNT="${build_account}" .

