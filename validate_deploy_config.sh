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

# This script takes parameters from Cloud Build --substitutions and calls
# init_deployment_config.py to produce a validated config.json file
# for subsequent submodules (SAP, Salesforce.com) to use as
# parameters for deployment

# TODO: Make fully interactive and Incorporate Mando Checker for pre-flight here

#################################################################
##  Globals
#################################################################

readonly CONFIG_FILE="./config/config.json"

#--------------------
# Parse arguments
#--------------------
source_project=""
target_project=""
cdc_processed_dataset=""
raw_landing_dataset=""
reporting_dataset=""
models_dataset=""
location=""
mandt=""
sql_flavour=""
test_data=""
gen_ext=""
deploy_sap=""
deploy_sfdc=""

#################################################################
##  Functions
#################################################################

usage() {
  cat <<HELP_USAGE

"\n🦄🦄🦄 Validating parameters for Google \x1b[38;2;66;133;244mCloud \x1b[38;2;234;67;53mCortex
\x1b[38;2;251;188;5mData \x1b[38;2;52;168;83mFoundation\x1b[0m 🔪🔪🔪\n"
Helps validate deployment options and generate configuration file.

$0 [OPTIONS]

Options
-h | --help                     : Display this message
-s | source-project             : Source Dataset Project ID. Mandatory
-t | target-project             : Target Dataset Project ID. Mandatory
-c | cdc-processed-dataset      : Source Dataset Name. Mandatory
-r | raw-landing-dataset        : Raw Landing Dataset Name. (Default: cdc-processed-dataset)
-p | reporting-dataset          : Target Dataset Name for Reporting (Default: REPORTING)
-a | models-dataset             : Target Dataset Name for ML (Default: MODELS)
-l | location                   : Dataset Location (Default: US)
-m | mandt                      : SAP Mandante. (Default: 100)
-f | sql-flavour                : SQL Flavor Selection, ECC or S4. (Default: ECC)
-d | test-data                  : Generate test data (Default: false)
-e | gen-ext                    : Generate external data (Default: true )
-i | deploy-sap                 : Deploy SAP
-j | deploy-sfdc                : Deploy SalesForce.com


HELP_USAGE

}

# Validate variables from the --substitutions flag for Cloud Build
# A variable coming from the parameter overwrites a variable in the config file.
# Globals: All input parameters
# Outputs: Non-zero for error
validate_builds_parameters() {

  if [ -f "${CONFIG_FILE}" ]; then
    echo "+++ Config file found ${CONFIG_FILE} +++ "
  else
    echo "+++ WARNING: config file config/config.env not found +++ "
  fi

  cat <<EOF
  Input Params:
  -------------
  source_project = ${source_project}
  target_project = ${target_project}
  cdc_processed_dataset = ${cdc_processed_dataset}
  raw_landing_dataset = ${raw_landing_dataset}
  reporting_dataset = ${reporting_dataset}
  models_dataset = ${models_dataset}
  location = ${location}
  mandt = ${mandt}
  sql_flavour = ${sql_flavour}
  test_data = ${test_data}
  gen_ext = ${gen_ext}
  deploy_sap = ${deploy_sap}
  deploy_sfdc = ${deploy_sfdc}
EOF

  # Send --substitution parameters and config/config.json file for
  # to validate. This script will create a valid config.json file in
  # /workspace for subsequent Cloud Build steps to copy and use
  python3 init_deployment_config.py \
    --PJID_SRC="${source_project}" \
    --PJID_TGT="${target_project}" \
    --DS_CDC="${cdc_processed_dataset}" \
    --DS_RAW="${raw_landing_dataset}" \
    --DS_REPORTING="${reporting_dataset}" \
    --DS_MODELS="${models_dataset}" \
    --LOCATION="${location}" \
    --MANDT="${mandt}" \
    --SQL_FLAVOUR="${sql_flavour}" \
    --TEST_DATA="${test_data}" \
    --GEN_EXT="${gen_ext}" \
    --DEPLOY_SAP="${deploy_sap}" \
    --DEPLOY_SFDC="${deploy_sfdc}"
}

#--------------------
# Main logic
#--------------------

set -o errexit -o noclobber -o nounset -o pipefail
opts="$(getopt -o hs:t:c:r:p:a:l:m:f:d:e:g:b:i:j: -l help,source-project:,target-project:,cdc-processed-dataset:,raw-landing-dataset:,reporting-dataset:,models-dataset:,location:,mandt:,sql-flavour:,test-data:,gen-ext:,deploy-sap:,deploy-sfdc: --name "$0" -- "$@")"

eval set -- "$opts"

while true; do
  case "$1" in
    -h | --help)
      usage
      shift
      exit
      ;;
    -s | --source-project)
      source_project=$2
      shift 2
      ;;
    -t | --target-project)
      target_project=$2
      shift 2
      ;;
    -c | --cdc-processed-dataset)
      cdc_processed_dataset=$2
      shift 2
      ;;
    -r | --raw-landing-dataset)
      raw_landing_dataset=$2
      shift 2
      ;;
    -p | --reporting-dataset)
      reporting_dataset=$2
      shift 2
      ;;
    -a | --models-dataset)
      models_dataset=$2
      shift 2
      ;;
    -l | --location)
      location=$2
      shift 2
      ;;
    -m | --mandt)
      mandt=$2
      shift 2
      ;;
    -f | --sql-flavour)
      sql_flavour=$2
      shift 2
      ;;
    -d | --test-data)
      test_data=$2
      shift 2
      ;;
    -e | --gen-ext)
      gen_ext=$2
      shift 2
      ;;
    -i | --deploy-sap)
      deploy_sap=$2
      shift 2
      ;;
    -j | --deploy-sfdc)
      deploy_sfdc=$2
      shift 2
      ;;
    --)
      shift
      break
      ;;
    *)
      echo "Invalid argument $1"
      usage
      exit 1
      ;;
  esac
done

#TODO: interactive check on python env, generate venv

echo -e "\n🦄🦄🦄 Validating parameters for Google \x1b[38;2;66;133;244mCloud \x1b[38;2;234;67;53mCortex \x1b[38;2;251;188;5mData \x1b[38;2;52;168;83mFoundation\x1b[0m 🔪🔪🔪\n"

validate_builds_parameters
