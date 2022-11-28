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

# Create test tables from GCS bucket
project="$1"
dataset="$2"
sql_fl=${3:ecc}
sql_flavor=$(echo "${sql_fl}" | tr '[:upper:]' '[:lower:]')
location=${4:-US}
location_low=$(echo "${location}" | tr '[:upper:]' '[:lower:]')

valid_locations="us-central1 us-west4 us-west2 northamerica-northeast1 northamerica-northeast2 us-east4 us-west1 us-west3 southamerica-east1 southamerica-west1 us-east1 asia-south2 asia-east2 asia-southeast2 australia-southeast2 asia-south1 asia-northeast2 asia-northeast3 asia-southeast1 australia-southeast1 asia-east1 asia-northeast1 europe-west1 europe-north1 europe-west3 europe-west2 europe-west4 europe-central2 europe-west6"

if [[ "${valid_locations}" =~ "${location_low}" ]]; then
  echo "Creating test harness in location ${location_low}"
else
  echo "ERROR: Location ${location_low} is not a valid location for the test harness."
  echo "Please set _TEST_DATA to false or use a supported location listed in README."
  echo "If you believe this location should be supported and is supported by BigQuery, please create an issue."
  exit 1
fi

if [[ "${location_low}" == 'australia-southeast1' ]]; then
    location_low=australia-southeast11
fi

# Temporary URL list file.
url_list_file=$(mktemp)

# Getting a list of URLs.
gsutil ls "gs://kittycorn-test-harness-${location_low}/${sql_flavor}/" >> "${url_list_file}"

# Disable existing on error because need to delete the temp file
set +e

# Loading to BigQuery.
python3 src/utils/bqload.py --source-list-file "${url_list_file}" --dataset "${project}.${dataset}" \
  --location "${location}" --parallel-jobs 10
ERR_CODE=$?

# Enable exiting on error back.
set -e

# Deleting temporary file.
rm -f "${url_list_file}"

exit $ERR_CODE
