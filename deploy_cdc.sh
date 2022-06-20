#!/bin/bash
<<<<<<< HEAD
=======

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

>>>>>>> rel22
project_id_src=$1
dataset_repl=$2
project_id_tgt=$3
dataset_tgt=$4
tgt_bucket=$5
log_bucket=$6
gen_test=$7
sql_flavour=$8
echo "Deploying CDC and unfolding hierarchies"
#"Source" in this context is where data is replicated and "Target" is where the CDC results are peristed
gcloud builds submit --config=./src/SAP_CDC/cloudbuild.cdc.yaml --substitutions=_PJID_SRC="$project_id_src",_DS_RAW="$dataset_repl",_PJID_TGT="$project_id_tgt",_DS_CDC="$dataset_tgt",_GCS_BUCKET="$tgt_bucket",_GCS_LOG_BUCKET="$log_bucket",_TEST_DATA="$gen_test",_SQL_FLAVOUR="$sql_flavour" ./src/SAP_CDC/

#pip install -r ./src/SAP_CDC/requirements.txt --user && cd ./src/SAP_CDC/src && python3 config_reader.py $project_id_src $dataset_repl $project_id_tgt $dataset_tgt $tgt_bucket $gen_test
