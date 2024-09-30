#!/bin/bash

# Copyright 2023 Google LLC
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

# This script generates all necessary DAG files and BQ objects for LiveRamp.

set -e

export PYTHONPATH=$PYTHONPATH:src/LiveRamp/:src/

_TGT_BUCKET=$(jq -r .targetBucket ${_CONFIG_FILE})

echo "Deploying LivRamp CDC layer..."
python src/LiveRamp/src/deploy.py
echo "✅ LiveRamp CDC layer deployed successfully."
echo "========================================"

# Copy generated files to Target GCS bucket
if [ ! -z "$(shopt -s nullglob dotglob; echo src/LiveRamp/_generated_dags/*)" ]
then
    echo "Copying LiveRamp artifacts to gs://${_TGT_BUCKET}/dags/liveramp."
    gcloud storage cp -r src/LiveRamp/_generated_dags/* gs://${_TGT_BUCKET}/dags/liveramp/
    echo "✅ LiveRamp artifacts have been copied."
else
    echo "❗ No file generated. Nothing to copy."
fi

echo "✅ LiveRamp deployed successfully."
echo "==================================================="
