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

# Processes sap_config.env file
# by reading values to the corresponding Cloud Build substitution variables:
# _DEPLOY_CDC, _GEN_EXT, _LOCATION,
# _PJID_SRC, _DS_RAW, _PJID_TGT, _DS_CDC, _TEST_DATA, _SQL_FLAVOUR
# _GCS_LOG_BUCKET (GCS_BUCKET from Data Foundation), _GCS_BUCKET (TGT_BUCKET from Data Foundation)

CONFIG_FILE="config/config.env"

apply_config(){
    config_file_path=$1
    eval $(cat ${config_file_path} | sed -e 's/^[ \t]*//;s/[ \t]*$//;/^#/d;/^\s*$/d;s#\([^\]\)"#\1#g;s/=\(.*\)/=\"\1\"/g;s/^/export /;s/$/;/')
}

make_defaults(){
    if [[ "${_RUN_EXT_SQL_}" == "" ]]
    then
        export _RUN_EXT_SQL_="true"
        export RUN_EXT_SQL="true"
    fi
    if [[ "${_SQL_FLAVOUR_}" == "" ]]
    then
        export _SQL_FLAVOUR_="ecc"
        export SQL_FLAVOUR="ecc"
    fi
    if [[ "${_GEN_EXT_}" == "" ]]
    then
        export _GEN_EXT_="true"
        export GEN_EXT="true"
    fi
    if [[ "${_TEST_DATA_}" == "" ]]
    then
        export _TEST_DATA_="false"
        export TEST_DATA="false"
    fi
}

# Converting Cloud Build substitutions to env variables.
export _PJID_SRC_="${1}"; export _PJID_TGT_="${2}"
export _DS_RAW_="${3}"; export _DS_CDC_="${4}"
export _MANDT_="${5}"; export _LOCATION_="${6}"
export _SQL_FLAVOUR_="${7}"; export _TEST_DATA_="${8}";
export _CURRENCY_="${9}"; export _LANGUAGE_="${10}";
export _GCS_LOG_BUCKET_="${11}"; export _GCS_BUCKET_="${12}";
export _DEPLOY_CDC_="${13}"; export _GEN_EXT_="${14}";
export _RUN_EXT_SQL_="${15}"
export _DS_REPORTING_="${16}"
export _DS_MODELS="${17}"

if [ -f "${CONFIG_FILE}" ]
then
    echo -e "\n======== 🔪🔪🔪 Found Configuration ${CONFIG_FILE} 🔪🔪🔪 ========"
    cat "${CONFIG_FILE}"
    echo "============================================"
    apply_config "${CONFIG_FILE}"
else
    # No config.env, nothing to process.
    make_defaults
    exit 0
fi

if [[ "${_GCS_BUCKET_}" == "" ]]
then
    if [[ "${GCS_BUCKET}" != "" ]]
    then
        export _GCS_BUCKET_="${GCS_BUCKET}"
    else
        echo "No Build Logs Bucket name provided."
        cloud_build_project=$(gcloud config list --format 'value(core.project)' 2>/dev/null)
        export _GCS_LOG_BUCKET_="${cloud_build_project}_cloudbuild"
        export GCS_BUCKET="${_GCS_BUCKET_}"
        echo "Using ${_GCS_BUCKET_}"
    fi
fi

if [[ "${_SQL_FLAVOUR_}" == "" ]]
then
    export _SQL_FLAVOUR_="${SQL_FLAVOUR}"
else
    # if _SQL_FLAVOUR is passed to the Cloud Build,
    # use its value for resolving flavor-specific values,
    # even when they are taken from sap_config.env
    export SQL_FLAVOUR="${_SQL_FLAVOUR_}"
fi

# Resolve flavor-specific variables
SQL_FLAVOUR_LOW=$(echo "${SQL_FLAVOUR}" | tr '[:upper:]' '[:lower:]')

if [[ "${SQL_FLAVOUR_LOW}" == "s4" ]]
then
    MANDT1="${MANDT_S4}"
    DS_CDC1="${DS_CDC_S4}"
    DS_RAW1="${DS_RAW_S4}"
    DS_REPORTING1="${DS_REPORTING_S4}"
    DS_MODELS1="${DS_MODELS_S4}"
else
    MANDT1="${MANDT_ECC}"
    DS_CDC1="${DS_CDC_ECC}"
    DS_RAW1="${DS_RAW_ECC}"
    DS_MODELS1="${DS_MODELS_ECC}"
    if [[ "${SQL_FLAVOUR_LOW}" == "union" ]]
    then
        DS_REPORTING1="${DS_REPORTING_UNION}"
    else
        DS_REPORTING1="${DS_REPORTING_ECC}"
    fi
fi

# If not in config, initialize from flavor-specific values (must be in config)
if [[ "${MANDT}" == "" ]]; then export MANDT="${MANDT1}"; fi
if [[ "${DS_CDC}" == "" ]]; then export DS_CDC="${DS_CDC1}"; fi
if [[ "${DS_RAW}" == "" ]]; then export DS_RAW="${DS_RAW1}"; fi
if [[ "${DS_REPORTING}" == "" ]]; then export DS_REPORTING="${DS_REPORTING1}"; fi
if [[ "${DS_MODELS}" == "" ]]; then export DS_MODELS="${DS_MODELS1}"; fi

# Provision substitution variables.
# Only change a variable if it's empty.
# It's usually empty if not passed to the Cloud Build.
# If passed to the cloud build, don't change it,
# copy the value to the config variable instead.

if [[ "${_PJID_SRC_}" == "" ]]
then
    export _PJID_SRC_="${PJID_SRC}"
else
    export PJID_SRC="${_PJID_SRC_}"
fi

if [[ "${_PJID_TGT_}" == "" ]]
then
    export _PJID_TGT_="${PJID_TGT}"
else
    export PJID_TGT="${_PJID_TGT_}"
fi

if [[ "${_DS_RAW_}" == "" ]]
then
    export _DS_RAW_="${DS_RAW}"
else
    export DS_RAW="${_DS_RAW_}"
fi

if [[ "${_DS_CDC_}" == "" ]]
then
    export _DS_CDC_="${DS_CDC}"
else
    export DS_CDC="${_DS_CDC_}"
fi

if [[ "${_DS_REPORTING_}" == "" ]]
then
    export _DS_REPORTING_="${DS_REPORTING}"
else
    export DS_REPORTING="${_DS_REPORTING_}"
fi

if [[ "${_DS_MODELS_}" == "" ]]
then
    export _DS_MODELS_="${DS_MODELS}"
else
    export DS_MODELS="${_DS_MODELS_}"
fi

if [[ "${_LOCATION_}" == "" ]]
then
    export _LOCATION_="${LOCATION}"
else
    export LOCATION="${_LOCATION_}"
fi

if [[ "${_MANDT_}" == "" ]]
then
    export _MANDT_="${MANDT}"
else
    export MANDT="${_MANDT_}"
fi

if [[ "${_CURRENCY_}" == "" ]]
then
    export _CURRENCY_="${CURRENCY}"
else
    export CURRENCY="${_CURRENCY_}"
fi

if [[ "${_LANGUAGE_}" == "" ]]
then
    export _LANGUAGE_="${LANGUAGE}"
else
    export LANGUAGE="${_LANGUAGE_}"
fi

# _GCS_BUCKET in CDC Cloud Build is TGT_BUCKET from Data Foundation
if [[ "${_GCS_BUCKET_}" == "" ]]
then
    export _GCS_BUCKET_="${TGT_BUCKET}"
else
    export TGT_BUCKET="${_GCS_BUCKET_}"
fi

if [[ "${_DEPLOY_CDC_}" == "" ]]
then
    export _DEPLOY_CDC_="${DEPLOY_CDC}"
else
    export DEPLOY_CDC="${_DEPLOY_CDC_}"
fi

if [[ "${_TEST_DATA_}" == "" ]]
then
    export _TEST_DATA_="${TEST_DATA}"
else
    export TEST_DATA="${_TEST_DATA_}"
fi

if [[ "${_GEN_EXT_}" == "" ]]
then
    export _GEN_EXT_="${GEN_EXT}"
else
    export GEN_EXT="${_GEN_EXT_}"
fi

if [[ "${_RUN_EXT_SQL_}" == "" ]]
then
    export _RUN_EXT_SQL_="${RUN_EXT_SQL}"
else
    export RUN_EXT_SQL="${_RUN_EXT_SQL_}"
fi

# SFDC values come from config.env
export _DS_RAW_SFDC_="${DS_RAW_SFDC}"
export _DS_CDC_SFDC_="${DS_CDC_SFDC}"
export _DS_REPORTING_SFDC_="${DS_REPORTING_SFDC}"
export _SFDC_CREATE_MAPPING_VIEWS_="${SFDC_CREATE_MAPPING_VIEWS}"

make_defaults