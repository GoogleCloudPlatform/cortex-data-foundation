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
"""Resource Configuration execution routines"""


import logging
import typing

import googleapiclient.discovery
from googleapiclient.errors import HttpError
from google.api_core.exceptions import Unauthorized, Forbidden, Conflict
from google.cloud import bigquery, storage
from google.cloud.bigquery.enums import EntityTypes


SOURCE_PROJECT_APIS = ["cloudresourcemanager", "storage-component",
                        "bigquery", "cloudbuild"]
TARGET_PROJECT_APIS = ["storage-component", "bigquery"]
PROJECT_ROLES = ["roles/bigquery.user"]


def get_cloud_build_account(project_id: str) -> str:
    """Retrieves GCP project Cloud Build account principal by project name/id.

    Args:
        project_id (str): project id

    Returns:
        str: Cloud Build account principal
    """
    crm = googleapiclient.discovery.build("cloudresourcemanager", "v1",
                                          cache_discovery=False)
    project = crm.projects().get(projectId=project_id).execute()
    project_number = project["projectNumber"]
    return f"{project_number}@cloudbuild.gserviceaccount.com"


def add_bq_roles(client: bigquery.Client, dataset: bigquery.Dataset,
                service_account: str, roles: typing.List[str]):
    """Adds role bindings to a BigQuery dataset for a service account.

    Args:
        client (bigquery.Client): BigQuery client object
        dataset (bigquery.Dataset): BigQuery dataset
        service_account (str): Service Account principal (email)
        roles (typing.List[str]): List of roles as role/<rolename>
    """
    logging.info("\tConfiguring roles %s on dataset %s for %s.", str(roles),
                 dataset.full_dataset_id, service_account)
    entries = dataset.access_entries
    modified = False
    for role in roles:
        found = False
        all_role_names = [role]
        if role == "roles/bigquery.dataViewer":
            all_role_names.append("READER")
        elif role == "roles/bigquery.dataEditor":
            all_role_names.append("WRITER")
        elif role == "roles/bigquery.dataOwner":
            all_role_names.append("OWNER")
        for entry in entries:
            if (entry.entity_id in
                    [service_account, f"serviceAccount:{service_account}"]
                    and entry.role in all_role_names):
                found = True
                break
        if not found:
            modified = True
            entries.append(
                bigquery.AccessEntry(
                    role=role,
                    entity_type=EntityTypes.USER_BY_EMAIL,
                    entity_id=service_account,
                )
            )
    if modified:
        dataset.access_entries = entries
        dataset = client.update_dataset(dataset, ["access_entries"])


def add_project_roles(project_id: str, service_account: str,
                      roles: typing.List[str]):
    """Adds IAM role bindings to a Service Account in a Project.

    Args:
        project_id (str): project id
        service_account (str): servicce account principal (email)
        roles (typing.List[str]): list of roles
    """
    logging.info("Configuring roles %s on project %s for %s.", str(roles),
                 project_id, service_account)
    crm = googleapiclient.discovery.build("cloudresourcemanager", "v1",
                                        cache_discovery=False)
    service_account_name = f"serviceAccount:{service_account}"

    modified = False
    trying = True
    while modified or trying:
        trying = False
        policy = (crm.projects().getIamPolicy(
                        resource=project_id,
                        body={"options": {"requestedPolicyVersion": "1"}},
                 ).execute())
        for role in roles:
            role_binding = None
            if not "bindings" in policy:
                policy["bindings"] = []
            for binding in policy["bindings"]:
                if binding["role"] == role:
                    role_binding = binding
                    break
            if not role_binding:
                role_binding = {
                    "role": role,
                    "members": [service_account_name]
                    }
                modified = True
                policy["bindings"].append(role_binding)
            else:
                if service_account_name not in role_binding["members"]:
                    modified = True
                    role_binding["members"].append(service_account_name)

        if modified:
            try:
                crm.projects().setIamPolicy(
                        resource=project_id,
                        body={
                            "policy": policy
                        }
                    ).execute()
                break
            except (Conflict, HttpError) as ex:
                if isinstance(ex, HttpError) and ex.status_code != 409:
                    raise
                continue


def create_bq_dataset_with_roles(project_id: str,
                      location: str,
                      dataset_name: str,
                      service_account: str,
                      roles: typing.List[str]):
    """Creates a BigQuery dataset, and adds role bindings on it for
       a service account.
       If datasets already exists, only does role binding.

    Args:
        project_id (str): project id
        location (str): BigQuery location
        dataset_name (str): dataset name
        service_account (str): service account principal (email)
        roles (typing.List[str]): list of roles to bind
    """
    client = bigquery.Client(project=project_id, location=location)
    try:
        logging.info("Creating dataset %s.%s.", project_id, dataset_name)
        dataset = client.create_dataset(dataset_name, exists_ok=False)
    except Conflict:
        logging.info("\tDataset %s.%s already exists.", project_id,
                     dataset_name)
        dataset = client.get_dataset(dataset_name)
    add_bq_roles(client, dataset, service_account, roles)


def create_storage_bucket_with_roles(project_id: str,
                          location: str,
                          bucket_name: str,
                          service_account: str,
                          roles: typing.List[str]):
    """Creates a Storage Bucket, and adds role bindings on it for
       a service account.
       If bucket already exists, only does role binding.

    Args:
        project_id (str): project id
        location (str): location
        bucket_name (str): bucket name
        service_account (str): service account principal (email)
        roles (typing.List[str]): list of roles
    """
    client = storage.Client(project=project_id)
    try:
        bucket = client.create_bucket(bucket_name, location=location)
    except Conflict:
        # Bucket already exists, it's ok.
        logging.info("\tBucket %s already exists.", bucket_name)
        bucket = client.get_bucket(bucket_name)
    add_bucket_roles(client, bucket, service_account, roles)


def add_bucket_roles(client: storage.Client, bucket: storage.Bucket,
                service_account: str, roles: typing.List[str]):
    logging.info("\tConfiguring roles %s on bucket %s for %s.", str(roles),
                 bucket.name, service_account)
    service_account_name = f"serviceAccount:{service_account}"

    modified = False
    trying = True
    while modified or trying:
        trying = False
        policy = bucket.get_iam_policy(client=client)
        bindings = policy.bindings
        for role in roles:
            role_binding = None
            for binding in bindings:
                if binding["role"] == role:
                    role_binding = binding
                    break
            if not role_binding:
                role_binding = {
                    "role": role,
                    "members": [service_account_name]
                    }
                modified = True
                bindings.append(role_binding)
            else:
                if service_account_name not in role_binding["members"]:
                    modified = True
                    role_binding["members"].append(service_account_name)

        if modified:
            try:
                bucket.set_iam_policy(policy, client=client)
                break
            except Conflict:
                continue


def enable_apis(project_id: str, apis: typing.List[str]):
    """Enables APIs in Google Cloud project

    Args:
        project_id (str): Google Cloud project id
        apis (typing.List[str]): list of APIs to enable
    """

    client = googleapiclient.discovery.build("serviceusage", "v1",
                                            cache_discovery=False)

    for api in apis:
        api_name = (api if api.endswith(".googleapis.com")
                    else f"{api}.googleapis.com")
        response = (
            client.services()
            .get(name=f"projects/{project_id}/services/{api_name}")
            .execute()
        )
        state = response["state"]

        if state != "ENABLED":
            logging.info("Enabling %s API in project %s", api_name, project_id)
            client.services().enable(
                name=f"projects/{project_id}/services/{api_name}"
            ).execute()

        logging.info("\t%s API is enabled in project %s.", api_name, project_id)


def apply_all(config: typing.Dict[str, typing.Any]) -> bool:
    """Applies Cortex Data Foundation configuration changes:
        * enables APIs
        * adds necessary role bindings on projects for Cloud Build account
        * creates datasets
        * adds necessary role bindings on these datasets for Cloud Build account
        * creates buckets
        * adds necessary role bindings on these buckets for Cloud Build account

    Args:
        config (typing.Dict[str, typing.Any]): Data Foundation config dictionary

    Returns:
        bool: True if configuration was successful, False otherwise.
    """
    source_project = config["projectIdSource"]
    target_project = config["projectIdTarget"]
    location = config["location"]

    try:
        logging.info("Enabling APIs in %s.", source_project)
        try:
            enable_apis(source_project, SOURCE_PROJECT_APIS)
        except HttpError as ex:
            if ex.status_code == 400 and "billing account" in ex.reason.lower():
                logging.fatal(("Project %s doesn't have "
                            "a Billing Account linked to it."), source_project)
                return False
            else:
                raise
        if target_project != source_project:
            try:
                logging.info("Enabling APIs in %s.", target_project)
                enable_apis(target_project, TARGET_PROJECT_APIS)
            except HttpError as ex:
                if (ex.status_code == 400 and
                    "billing account" in ex.reason.lower()):
                    logging.fatal(("Project %s doesn't have "
                                "a Billing Account linked to it."),
                                source_project)
                    return False
                else:
                    raise

        cloud_build_account = get_cloud_build_account(source_project)
        logging.info("Using Cloud Build account %s.", cloud_build_account)

        # Add project-wide role binding for Cloud Build account
        add_project_roles(source_project, cloud_build_account,
                            PROJECT_ROLES)
        if target_project != source_project:
            add_project_roles(target_project, cloud_build_account,
                                PROJECT_ROLES)

        dataset_dicts = []
        source_datasets = []
        reporting_datasets = []

        dataset_dicts.append(config["k9"]["datasets"])
        if config.get("deploySAP"):
            dataset_dicts.append(config["SAP"]["datasets"])
        if config.get("deploySFDC"):
            dataset_dicts.append(config["SFDC"]["datasets"])
        if config.get("deployMarketing"):
            if config["marketing"].get("deployGoogleAds"):
                dataset_dicts.append(config["marketing"]["GoogleAds"]
                                     ["datasets"])
            if config["marketing"].get("deployCM360"):
                dataset_dicts.append(config["marketing"]["CM360"]["datasets"])
            if config["marketing"].get("deployTikTok"):
                dataset_dicts.append(config["marketing"]["TikTok"]["datasets"])
            if config["marketing"].get("deployLiveRamp"):
                dataset_dicts.append(
                    config["marketing"]["LiveRamp"]["datasets"])
            if config["marketing"].get("deployMeta"):
                dataset_dicts.append(config["marketing"]["Meta"]["datasets"])
            if config["marketing"].get("deploySFMC"):
                dataset_dicts.append(config["marketing"]["SFMC"]["datasets"])
        for dataset_dict in dataset_dicts:
            for ds in dataset_dict.items():
                add_to = (reporting_datasets
                            if ds[0] == "reporting" else source_datasets)
                if ds not in add_to: # type: ignore
                    if ds[1] != "":
                        add_to.append(ds[1]) # type: ignore

        # Create datasets (if needed),
        # and add "roles/bigquery.dataEditor" binding on them
        # for the source project's Cloud Build account.
        logging.info("Creating datasets in %s.", source_project)
        for ds in source_datasets:
            create_bq_dataset_with_roles(source_project, location, ds,
                            cloud_build_account,
                            ["roles/bigquery.dataEditor"])
        if target_project != source_project:
            # This check is only for logging.
            logging.info("Creating datasets in %s.", target_project)
        for ds in reporting_datasets:
            create_bq_dataset_with_roles(target_project, location, ds,
                            cloud_build_account,
                            ["roles/bigquery.dataEditor"])

        # Create target storage bucket (if needed),
        # and add "roles/storage.admin" binding on it for Cloud Build account.
        logging.info("Creating storage bucket %s.", config["targetBucket"])
        create_storage_bucket_with_roles(source_project, location,
                                         config["targetBucket"],
                                         cloud_build_account,
                                         ["roles/storage.admin"])

    except (HttpError, Forbidden, Unauthorized) as ex:
        if isinstance(ex, HttpError):
            message = ex.reason
            if ex.status_code not in (401, 403):
                raise
        else:
            message = ex.message
        logging.fatal("You do not have sufficient permissions: %s", message)
        return False

    return True

