#!/bin/bash

#--------------------
# Help Message
#--------------------
usage() {
  cat <<HELP_USAGE

Will generate one view.

$0 [OPTIONS] VIEW_DIR

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

  if [[ -z "${views_dir-}" || "${views_dir}" == "none" ]]; then
    echo 'ERROR: "VIEW_DIR" is required. See help for details.'
    exit 1
  fi
}

#--------------------
# Make Safe for bq mk
#--------------------

bq_safe_mk() {
  dataset=$1
  exists=$(bq query --location=$location --project_id=$project_id_tgt --use_legacy_sql=false "select distinct 'KITTYCORN' from ${dataset}.INFORMATION_SCHEMA.TABLES")
  if [[ "$exists" == *"KITTYCORN"* ]]; then
    echo "Not creating $dataset since it already exists"
  else
    echo "Creating dataset $project_id_tgt:$dataset with location: $location"
    bq --location="$location" mk --dataset "$project_id_tgt:$dataset"
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

views_dir=${@:$OPTIND:1}

set +o errexit +o noclobber +o nounset +o pipefail

#--------------------
# Main Logic
#--------------------

validate

# helpful for debugging
echo "Running with the following parameters:"
echo "source-project: ${project_id_src}"
echo "target-project: ${project_id_tgt}"
echo "cdc-processed-dataset: ${dataset_cdc_processed}"
echo "raw-landing-dataset: ${dataset_raw_landing}"
echo "target-reporting-dataset: ${dataset_reporting_tgt}"
echo "target-models-dataset: ${dataset_models_tgt}"
echo "location: ${location}"
echo "mandt: ${mandt}"
echo "sql-flavour: ${sql_flavour}"
echo "views_dir: ${views_dir}"

success=0
bq_safe_mk "${dataset_reporting_tgt}"
bq_safe_mk "${dataset_models_tgt}"

for file_entry in $(cat ./dependencies.txt); do
  echo "Creating ${file_entry} on ${dataset_reporting_tgt}"
  gcloud builds submit --config=pipeline/cloudbuild.view.yaml --substitutions=_SQL_FILE="${file_entry}",_PJID_SRC="${project_id_src}",_PJID_TGT="${project_id_tgt}",_DS_CDC="${dataset_cdc_processed}",_DS_RAW="${dataset_raw_landing}",_DS_REPORTING="${dataset_reporting_tgt}",_DS_MODELS="${dataset_models_tgt}",_MANDT="${mandt}",_LOCATION="${location}",_SQL_FLAVOUR="${sql_flavour}" .

  if [ $? = 1 ]; then
    success=1
  fi

done

exit "${success}"
