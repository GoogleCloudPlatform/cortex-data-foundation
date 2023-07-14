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

# Interactive Configurator Launcher

# Exit on error.
set -e

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

pushd "${SCRIPT_DIR}" 1> /dev/null
echo -n "Please wait while we are checking dependencies..."
python3 -m pip install --use-pep517 --disable-pip-version-check --require-hashes --no-deps -r "requirements.txt" 1>/dev/null && \
python3 -m pip install --use-pep517 --disable-pip-version-check --require-hashes --no-deps -r "../../../requirements.txt" 1>/dev/null
echo -e -n "\r                                                    \r"
popd 1> /dev/null

export PYTHONPATH=$$PYTHONPATH:${SCRIPT_DIR}:.
pushd "${SCRIPT_DIR}/../../.." 1> /dev/null # Data Foundation root
python3 "${SCRIPT_DIR}/main.py" "${1}" "${2}" "${3}"
source_project=$(cat "config/config.json" | python3 -c "import json,sys; print(str(json.load(sys.stdin)['projectIdSource']))" 2>/dev/null || echo "")
gcloud config set project "${source_project}"
./deploy.sh
popd 1> /dev/null
