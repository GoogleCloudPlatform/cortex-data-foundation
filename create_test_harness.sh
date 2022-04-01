#!/bin/bash

# Create test tables from GCS bucket
project="$1"
dataset="$2"
sql_fl=${3: ecc}
sql_flavor=$(echo "${sql_fl}" | tr '[:upper:]' '[:lower:]')
location=${4:-US}
location_low=$(echo "${location}" | tr '[:upper:]' '[:lower:]')

N=10

open_semaphore() {
  mkfifo pipe-$$
  exec 3<>pipe-$$
  rm pipe-$$
  local i=$1
  for (( ; i > 0; i--)); do
    printf %s 000 >&3
  done
}

process_table() {
  tab=$1

  tabname=$(basename "${tab}" .parquet)

  echo "Processing ${tabname}"
  exists=$(bq show --project_id "${project}" --location="${location}" "${dataset}.${tabname}")
  if [[ "${exists}" == *"Not found"* ]]; then
    bq load --location="${location}" --project_id "${project}" --noreplace --source_format=PARQUET "${dataset}.${tabname}" "${tab}"
    echo
  else
    echo "Table ${tabname} already exists, skipping"
  fi
}

run_with_lock() {
  local x
  # this read waits until there is something to read
  read -u 3 -n 3 x && ((0 == x)) || exit $x
  ( 
    ("$@")
    # push the return code of the command to the semaphore
    printf '%.3d' $? >&3
  ) &

}

open_semaphore "${N}"
for tab in $(gsutil ls "gs://kittycorn-test-harness-${location_low}/${sql_flavor}/"); do
  if [[  $tab != */ ]] ; then 
    run_with_lock process_table "${tab}"
  fi
done

wait
