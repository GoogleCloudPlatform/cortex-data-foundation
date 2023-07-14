#!/usr/bin/env bash

set -e

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
RUN_AS=$1
JOB_ID=$(python3 -c "import uuid; print(uuid.uuid4().hex)")

if [[ "${RUN_AS}" == "" || "${RUN_AS}" == "DirectRunner" || "${RUN_AS}" == "Direct" ]]
then
    runner="DirectRunner"
elif [[ "${RUN_AS}" == "DeployTemplate" ]]
then
    runner="DataflowRunner"
    template_param="--template_location gs://{{ dataflow_bucket }}/CATGAP/dataflow/{{ deployment_id }}/template.json"
elif [[ "${RUN_AS}" == "RunTemplate" ]]
then
    echo "Executing CATGAP pipeline from deployed template..."
    gcloud dataflow jobs run "catgap-${JOB_ID}" \
        --project "{{ project_id_src }}" \
        --region "{{ dataflow_region }}" \
        --gcs-location "gs://{{ dataflow_bucket }}/CATGAP/dataflow/{{ deployment_id }}/template.json" \
        --staging-location "gs://{{ dataflow_bucket }}/CATGAP/dataflow/{{ deployment_id }}/temp"
    exit 0
elif [[ "${RUN_AS}" == "Dataflow" || "${RUN_AS}" == "DataflowRunner" ]]
then
    runner="DataflowRunner"
else
    runner="${RUN_AS}"
fi

if [[ "${runner}" == "DataflowRunner" ]]
then
    if [[ "{{ dataflow_use_container }}" == "true" ]]
    then
        repo_name="{{ container_repo_name }}"
        image_name="{{ beam_image_name }}"
        repo_url="{{ dataflow_region }}-docker.pkg.dev/{{ project_id_src }}/${repo_name}"
        image_url="${repo_url}/${image_name}"
        deps_param="--sdk_container_image=${image_url} --sdk_location=container ${template_param}"
    else
        deps_param="--requirements_file=./requirements.in ${template_param}"
    fi
else
    echo "Checking pipeline's local Python dependencies..."
    python3 -m pip install --use-pep517 --require-hashes --no-deps -r "${SCRIPT_DIR}/catgap_pipeline/requirements.txt" 1>/dev/null
    deps_param="--requirements_file=./requirements.in"
fi

echo "Running or deploying Apache Beam pipeline..."
pushd "${SCRIPT_DIR}/catgap_pipeline" > /dev/null
python3 "${SCRIPT_DIR}/catgap_pipeline/main.py" \
    --project "{{ project_id_src }}" \
    --region "{{ dataflow_region }}" \
    --job_name "catgap-${JOB_ID}" \
    --staging_location "gs://{{ dataflow_bucket }}/CATGAP/dataflow/{{ deployment_id }}/staging" \
    --temp_location "gs://{{ dataflow_bucket }}/CATGAP/dataflow/{{ deployment_id }}/temp" \
    --service_account_email "{{ dataflow_sa }}" \
    --runner "${runner}" \
    --setup_file=./setup.py \
    ${deps_param} \
    --experiment=use_runner_v2 \
    --dataflow_service_options=enable_prime \
    --resource_hints=min_ram=128GB \
    --disk_size_gb=250 \
    --experiments=disable_worker_container_image_prepull \
    --source_project "{{ project_id_src }}" \
    --ads_project "{{ project_id_src }}" \
    --bigquery_location "{{ location }}" \
    --k9_processing_dataset "{{ dataset_k9_processing }}" \
    --ads_dataset "{{ dataset_ads }}" \
    --ads_customer_id "{{ ads_account }}" \
    --staging_dataset "{{ dataset_k9_processing }}" \
    --mapping_spreadsheet "{{ mapping_spreadsheet }}"
popd > /dev/null