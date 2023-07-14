#!/usr/bin/env bash

set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

REPO_NAME="{{ container_repo_name }}"
IMAGE_NAME="{{ beam_image_name }}"
REPO_URL="{{ dataflow_region }}-docker.pkg.dev/{{ project_id_src }}/${REPO_NAME}"
IMAGE_URL="${REPO_URL}/${IMAGE_NAME}"

existing_repo=$(gcloud artifacts repositories list \
    --filter "name=projects/{{ project_id_src }}/locations/{{ dataflow_region }}/repositories/${REPO_NAME}" \
    --project "{{ project_id_src }}" \
    --location "{{ dataflow_region }}" \
    --format="value(name)" -q 2>/dev/null)

if [[ "${existing_repo}" == "" ]]
then
    echo "Repostory ${REPO_NAME} doesn't exist in project {{ project_id_src }}. Creating one..."
    gcloud artifacts repositories create "${REPO_NAME}" \
        --repository-format docker \
        --project "{{ project_id_src }}" \
        --location "{{ dataflow_region }}"
fi

echo "Creating and pushing Apache Beam container image with dependencies..."
gcloud builds submit -t "${IMAGE_URL}:latest" --project="{{ project_id_src }}" "${SCRIPT_DIR}"
