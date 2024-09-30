#!/bin/bash

#--------------------
# Help Message
#--------------------

usage() {
  cat <<HELP_USAGE

Will generate one view.

$0 [OPTIONS] VIEW_FILE

Options
-h | --help                     : Display this message
-a | source-project             : Source Dataset Project ID. Mandatory
-b | target-project             : Target Dataset Project ID. Mandatory
-x | cdc-processed-dataset      : Source Dataset Name. Mandatory
-y | raw-landing-dataset        : Raw Landing Dataset Name.
-r | target-reporting-dataset   : Target Dataset Name for Reporting (Default: SAP_REPORTING)
-s | target-models-dataset      : Target Dataset Name for ML (Default: ML_MODELS)
-l | location                   : Dataset Location (Default: US)
-m | mandt                      : SAP Mandante
-f | sql-flavour                : SQL Flavor Selection, ECC or S4. (Default: ECC)

HELP_USAGE

}

#--------------------
# Validate input
#--------------------
validate() {

  [ -f data.json ] && rm data.json

  if ! type "jinja" >/dev/null 2>&1; then
    echo "ERROR: jinja-cli not available, please check if installed and try again"
    exit 1
  fi

  if [ -z "${project_id_src-}" ]; then
    echo 'ERROR: "source-project" is required. See help for details.'
    exit 1
  fi

  if [ -z "${project_id_tgt-}" ]; then
    echo 'INFO: "target-project" missing, defaulting to source-target.'
    project_id_tgt="${project_id_src}"
  fi

  if [ -z "${dataset_cdc_processed-}" ]; then
    echo 'ERROR: "cdc-processed-dataset" is required. See help for details.'
    exit 1
  fi

  if [ -z "${dataset_raw_landing-}" ]; then
    echo 'INFO: "raw-landing-dataset" missing, defaulting to SAP_REPORTING.'
    dataset_raw_landing="${dataset_cdc_processed}"
  fi

  if [ -z "${dataset_reporting_tgt-}" ]; then
    echo 'INFO: "target-reporting-dataset" missing, defaulting to SAP_REPORTING.'
    dataset_reporting_tgt="SAP_REPORTING"
  fi

  if [ -z "${dataset_models_tgt-}" ]; then
    echo 'INFO: "target-models-dataset" missing, defaulting to ML_MODELS.'
    dataset_models_tgt="ML_MODELS"
  fi

  if [ -z "${location-}" ]; then
    echo 'INFO: "location" missing, defaulting to US.'
    location="US"
  fi

  if [ -z "${mandt-}" ]; then
    echo 'ERROR: "mandt" is required. See help for details.'
    exit 1
  fi

  if [[ -z "${sql_flavour-}" || -n "${sql_flavour-}" && $(echo "${sql_flavour}" | tr '[:upper:]' '[:lower:]') != "s4" ]]; then
    sql_flavour="ecc"
  else
    sql_flavour="s4"
  fi

  if [[ -z "${sql_file-}" || "${sql_file}" == "none" ]]; then
    echo 'ERROR: "VIEW FILE" is required. See help for details.'
    exit 1
  fi
}

#--------------------
# Parameters parsing
#--------------------

set -o errexit -o noclobber -o nounset -o pipefail
params="$(getopt -o ha:b:x:y:r:s:l:m:f: -l help,source-project:,target-project:,cdc-processed-dataset:,raw-landing-dataset:,target-reporting-dataset:,target-models-dataset:,location:,mandt:,sql-flavour: --name "$0" -- "$@")"
eval set -- "$params"

while true; do
  case "$1" in
    -h | --help)
      usage
      shift
      exit
      ;;
    -a | --source-project)
      project_id_src=$2
      shift 2
      ;;
    -b | --target-project)
      project_id_tgt=$2
      shift 2
      ;;
    -x | --cdc-processed-dataset)
      dataset_cdc_processed=$2
      shift 2
      ;;
    -y | --raw-landing-dataset)
      dataset_raw_landing=$2
      shift 2
      ;;
    -r | --target-reporting-dataset)
      dataset_reporting_tgt=$2
      shift 2
      ;;
    -s | --target-models-dataset)
      dataset_models_tgt=$2
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
    --)
      shift
      break
      ;;
    *)
      echo "Invalid option ($1). Run --help for usage" >&2
      exit 1
      ;;
  esac
done

sql_file=${@:$OPTIND:1}

#set +o errexit +o noclobber +o nounset +o pipefail

#--------------------
# Run
#--------------------

validate
# helpful for debugging
# echo "source-project: ${project_id_src}"
# echo "target-project: ${project_id_tgt}"
# echo "raw-landing-dataset: ${dataset_raw_landing}"
# echo "cdc-processed-dataset: ${dataset_cdc_processed}"
# echo "target-reporting-dataset: ${dataset_reporting_tgt}"
# echo "target-models-dataset: ${dataset_models_tgt}"
# echo "location: ${location}"
# echo "mandt: ${mandt}"
# echo "sql-flavour: ${sql_flavour}"
# echo "sql-file: ${sql_file}"

cat <<EOF >data.json
{
    "project_id_src": "${project_id_src}",
    "project_id_tgt": "${project_id_tgt}",
    "dataset_raw_landing": "${dataset_raw_landing}",
    "dataset_cdc_processed": "${dataset_cdc_processed}",
    "dataset_reporting_tgt": "${dataset_reporting_tgt}",
    "dataset_models_tgt": "${dataset_models_tgt}",
    "mandt": "${mandt}",
    "sql_flavour": "${sql_flavour}"
}
EOF

echo "---Creating View: --- "
query=$(jinja -d data.json "${sql_file}")
echo "${query}"
bq query --batch --location="${location}" --use_legacy_sql=false "${query}"
