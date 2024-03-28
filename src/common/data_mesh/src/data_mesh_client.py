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
"""Defines a CortexDataMeshClient that creates the data mesh resources."""
import asyncio
from collections import defaultdict
import copy
import enum
from exceptiongroup import ExceptionGroup
import logging
import re
from typing import Any, Awaitable, DefaultDict, Dict, Iterable, List, Optional, Set, Type

from google.api_core import exceptions as google_exc
from google.api_core import retry
from google.cloud import bigquery
from google.cloud.bigquery import table as bq_table
from google.cloud import bigquery_datapolicies_v1
from google.cloud import datacatalog_v1
from google.cloud import dataplex_v1
from google.cloud import exceptions as gcp_exc
from google.iam.v1 import iam_policy_pb2
from google.iam.v1 import policy_pb2
from google.longrunning import operations_pb2

from common.data_mesh.src import data_mesh_types as dmt
from common.data_mesh.src import data_mesh_types_util as dmt_util
from common.py_libs import cortex_exceptions as cortex_exc

_RETRY_TIMEOUT_SEC = 20 * 60.0
_ASSET_TAG_PREFIX = "asset"
_BQ_ROLES = {
    dmt.BqAssetRole.READER: "roles/bigquery.dataViewer",
    dmt.BqAssetRole.WRITER: "roles/bigquery.dataEditor",
    dmt.BqAssetRole.OWNER: "roles/bigquery.dataOwner"
}

# Type Aliases
AssetMemberSets = DefaultDict[dmt.BqAssetRole, Set[str]]


@enum.unique
class BqAssetType(enum.Enum):
    UNKNOWN = enum.auto()
    TABLE = enum.auto()
    VIEW = enum.auto()


def _create_set_iam_policy_request(
        resource_name: str, role: str,
        members: List[str]) -> iam_policy_pb2.SetIamPolicyRequest:
    binding = policy_pb2.Binding(role=role, members=members)  # type: ignore
    policy = policy_pb2.Policy(bindings=[binding])  # type: ignore
    return iam_policy_pb2.SetIamPolicyRequest(resource=resource_name,
                                              policy=policy)  # type: ignore


async def _gather_exceptions_async(*awaitables: Awaitable) -> List[Exception]:
    results = await asyncio.gather(*awaitables, return_exceptions=True)
    return [r for r in results if isinstance(r, Exception)]


def _concat_sql_strings(strings: List[str]) -> str:
    wrapped_strings = [f"'{s}'" for s in strings]
    return ", ".join(wrapped_strings)


def _get_create_asset_policy_query(policy: dmt.BqAssetPolicy, asset_id: str,
                                   asset_type: BqAssetType) -> str:
    return f"""
        GRANT `{_BQ_ROLES[policy.role]}`
        ON {asset_type.name} `{asset_id}`
        TO {_concat_sql_strings(policy.principals)};
    """


def _get_revoke_asset_policy_query(role: dmt.BqAssetRole, asset_id: str,
                                   asset_type: BqAssetType,
                                   principals: Iterable[str]) -> str:
    return f"""
        REVOKE `{_BQ_ROLES[role]}`
        ON {asset_type.name} `{asset_id}`
        FROM {_concat_sql_strings(list(principals))};
    """


def _get_grant_row_policy_query(asset_id: str, policy: dmt.BqRowPolicy,
                                overwrite: bool) -> str:
    if overwrite:
        policy_stmt = "CREATE OR REPLACE ROW ACCESS POLICY"
    else:
        policy_stmt = "CREATE ROW ACCESS POLICY IF NOT EXISTS"

    return f"""
        {policy_stmt} {policy.name}
        ON `{asset_id}`
        GRANT TO ({_concat_sql_strings(policy.readers)})
        FILTER USING ({policy.filter});
    """


def _get_drop_all_row_policy_query(asset_id: str) -> str:
    return f"""
        DROP ALL ROW ACCESS POLICIES ON `{asset_id}`;
    """


# TODO: consider moving to new util module.
def get_bq_entry_name(project_id: str, dataset_id: str, asset_id: str) -> str:
    return (f"//bigquery.googleapis.com/projects/{project_id}/datasets/"
            f"{dataset_id}/tables/{asset_id}")


def get_bq_dataset_name(project_id: str, dataset_id: str) -> str:
    return f"projects/{project_id}/datasets/{dataset_id}"


# TODO: Consider allowing enabling/disabling concurrent requests.
# TODO: Integration tests.
# TODO: Handle non-transient API request failures.
# TODO: Consider validating client, e.g project/location values.
# TODO: Consider option for acl dry run that logs queries
# TODO: Consider supporting deployment as an atomic transaction
# TODO: Log actual row access policy changes, currently only has REST API.
# TODO: Flags to enable modularized deployment for APIs and ACLs


class CortexDataMeshClient:
    """Class to help create Cortex Data Mesh resources.

    This class provides a unified interface composed of clients to all the
    relevent GCP products and sends requests to those APIs to create a holistic
    data mesh based on configuration files.

    Requests are issued concurrently to account for long running operations like
    creating lakes and zones. When exceptions are raised, all currently running
    tasks are allowed to complete, and then all exceptions are raised in an
    ExceptionGroup.

    See deploy_data_mesh.py for usage examples.

    Attributes:
        location: String representing the location where the data in the
            mesh is stored. This value should match the location field in Cortex
            config.json.
        overwrite: Boolean indicating whether existing resources and annotations
            should be overwritten. When this is false, existing resources will
            patch in new nested resources.
    """

    def __init__(self,
                 location: str,
                 overwrite: bool = False,
                 bq_client=bigquery.Client(),
                 bq_datapolicy_client=(
                     bigquery_datapolicies_v1.DataPolicyServiceClient()),
                 catalog_client=datacatalog_v1.DataCatalogClient(),
                 policy_tag_client=datacatalog_v1.PolicyTagManagerClient(),
                 dataplex_client=dataplex_v1.DataplexServiceClient(),
                 retry_timeout: float = _RETRY_TIMEOUT_SEC):
        """Initializes a client for the given project & location.

        Args:
            project: String representing the GCP project id.
            location: String representing the location where the data in the
                mesh is stored. This value should match the location field in
                Cortex config.json.
            overwrite: Boolean indicating whether existing resources and
                annotations should be overwritten.
            bq_datapolicy_client: BigQuery data policy client.
            catalog_client: Data Catalog client.
            policy_tag_client: Data Catalog policy tag client.
            dataplex_client: Dataplex client, which still behaves asynchronously
                for some long running requests which return ops.
            retry_timeout: Float specifying the timeout duration in seconds for
                API request retries. Sets google.api_core.retry.Retry.timeout on
                requests that create operations, and also sets the timeout and
                retry for polling the operation results.
        """

        self._location = location
        self.overwrite = overwrite

        self._bq_client = bq_client
        self._bq_datapolicy_client = bq_datapolicy_client
        self._catalog_client = catalog_client
        self._policy_tag_client = policy_tag_client
        self._dataplex_client = dataplex_client

        self._retry_timeout = retry_timeout
        self._retry_options = retry.Retry(timeout=retry_timeout)

        # Dictionary of {display_name: unique_name} containing existing tag
        # templates, which is populated before annotating BQ assets.
        self._existing_tag_templates: Optional[Dict[str, str]] = None

    @property
    def location(self) -> str:
        """The data location that matches the Cortex config.json field."""
        return self._location

    def _parent_project(self, project: str) -> str:
        """The full name of the parent project used by requests."""
        return self._policy_tag_client.common_location_path(
            project, self._location).lower()

    def _should_overwrite(self,
                          original: Any,
                          new: Any,
                          enable_delete: bool = False) -> bool:
        if new == original:
            return False
        # This case is needed in addition to the first for comparisons like
        # None != []
        if not new and not original:
            return False
        if not new and not enable_delete:
            return False
        if self.overwrite:
            return True
        if not self.overwrite and not original:
            return True
        return False

    def _list_resources(self, resource_type: Type[object],
                        parent: str) -> Dict[str, str]:
        """Gets a dictionary of the existing resources of the given type.

        Args:
            resource_type: The resource type to list. This supports a subset of
                data_mesh_types dataclasses.
            parent: String that uniquely identifies a parent resource.
                CatalogTagTemplate: project ID
                CatalogTag: entry/asset name
                PolicyTaxonomy: project location path
                PolicyTag: taxonomy name
                DataPolicy: project location path
                Lake: project location path of lake region
                Zone: lake name
                asset: zone name

        Returns:
            A dict representing existing resources that maps resource display
            names to their uniquely identifing name.
        """
        list_pager = None

        # Catalog tag templates are searched for using syntax described below:
        # https://cloud.google.com/data-catalog/docs/how-to/search-reference
        if resource_type == dmt.CatalogTagTemplate:
            scope = datacatalog_v1.SearchCatalogRequest.Scope(
                include_project_ids=[parent])
            list_pager = self._catalog_client.search_catalog(
                scope=scope,
                query="type=tag_template",
                retry=self._retry_options)
        elif resource_type == dmt.CatalogTag:
            list_pager = self._catalog_client.list_tags(
                parent=parent, retry=self._retry_options)
        elif resource_type == dmt.PolicyTaxonomy:
            list_pager = self._policy_tag_client.list_taxonomies(
                parent=parent, retry=self._retry_options)
        elif resource_type == dmt.PolicyTag:
            list_pager = self._policy_tag_client.list_policy_tags(
                parent=parent, retry=self._retry_options)
        elif resource_type == dmt.DataPolicy:
            list_pager = self._bq_datapolicy_client.list_data_policies(
                parent=parent, retry=self._retry_options)
        elif resource_type == dmt.Lake:
            list_pager = self._dataplex_client.list_lakes(
                parent=parent, retry=self._retry_options)
        elif resource_type == dmt.Zone:
            list_pager = self._dataplex_client.list_zones(
                parent=parent, retry=self._retry_options)
        elif resource_type == dmt.Asset:
            list_pager = self._dataplex_client.list_assets(
                parent=parent, retry=self._retry_options)
        else:
            raise cortex_exc.NotImplementedCError(f"Type: {resource_type}")

        resources = {}
        for resource in list_pager:
            # Catalog Tags need special handling because the returned results
            # can have multiple resources with the same display name for cases
            # where a single Tag Template is used at the asset and column level.
            if resource_type == dmt.CatalogTag:
                prefix = resource.column or _ASSET_TAG_PREFIX
                key = f"{prefix}:{dmt_util.get_display_name(resource)}"
            else:
                key = dmt_util.get_display_name(resource)

            resources[key] = dmt_util.get_unique_name(resource)
        return resources

    def _get_resource_name(
            self,
            resource_type: Type[object],
            display_name: str,
            parent: str,
            existing_resources: Optional[Dict[str, str]] = None) -> str:
        """Gets the full existing resource name from the display name.

        Args:
            resource_type: A data_mesh_types dataclass associated with a
                resource, e.g. PolicyTaxonomy.
            display_name: String representing the display_name to find.
            parent: String that uniquely identifies a parent resource.
                For catalog tag templates, this is the project ID.
                For taxonomies and data policies, this is the project path.
                For policy tags, this is the parent taxonomy.
            existing_resources: Optional dictionary of display_name: unique_name
                containing the existing resources. If it is not provided, a list
                method will be called.

        Returns:
            The uniquely identifing resource name.

        Raises:
            ValueError if the the resource cannot be found.
        """
        if existing_resources is None:
            existing_resources = self._list_resources(resource_type, parent)

        resource_name = existing_resources.get(display_name)
        if not resource_name:
            raise cortex_exc.NotFoundCError(
                f"{resource_type.__name__} {display_name} not found under "
                f"{parent}.")

        return resource_name

    def _list_bq_asset_types(self, project_id: str,
                             dataset_id: str) -> Dict[str, BqAssetType]:
        list_results = self._bq_client.list_tables(f"{project_id}.{dataset_id}")
        return {
            asset.table_id: BqAssetType[asset.table_type]
            for asset in list_results
        }

    # The following methods are specific to Lakes because they are needed to
    # handle edge cases where long running operations may try to create the same
    # resource from multiple concurrent processes.
    def _has_completed_create_lake_op(self, lake_spec: dmt.Lake,
                                      parent: str) -> bool:
        list_request = operations_pb2.ListOperationsRequest()
        list_request.name = parent  # type: ignore
        list_request.filter = "done=false"  # type: ignore
        response = self._dataplex_client.list_operations(list_request)

        for op in response.operations:  # type: ignore
            metadata_string = str(op.metadata)
            # TODO: Improve the filter search.
            # Either figure out how to construct a filter string that uses the
            # metadata.target, or improve how the metadata Any proto is parsed.
            # target = f"{parent}/lakes/{resource_spec.display_name}"
            if metadata_string.find(lake_spec.display_name) != -1:
                return False

        return True

    async def _wait_for_lake_creation(self, lake_spec: dmt.Lake,
                                      parent: str) -> str:

        sleep_generator = retry.exponential_sleep_generator(initial=1.0,
                                                            maximum=60.0)
        elapsed = 0

        # Poll the operation status because we can only get a
        # longrunning.operations_pb2.Operation, which doesn't provide a `result`
        # method to wait for completion like an api_core.operation.Operation.
        for interval in sleep_generator:
            if elapsed > self._retry_timeout:
                raise cortex_exc.TimeoutCError(
                    "Polling timed out while waiting for Lake: "
                    f"'{lake_spec.display_name}' to be created.")

            if self._has_completed_create_lake_op(lake_spec, parent):
                break

            await asyncio.sleep(interval)
            elapsed += interval

        return self._get_resource_name(dmt.Lake, lake_spec.display_name, parent)

    def _delete_resource(self, resource_type: Type[object], display_name: str,
                         name: str) -> None:
        """Deletes the specified resource.

        Args:
            resource_type: A data_mesh_types dataclass type associated with a
                resource, e.g. PolicyTaxonomy.
            name: String that uniquely identifies the resource.
        """
        if resource_type == dmt.CatalogTagTemplate:
            # Deletes all tags that use this template.
            self._catalog_client.delete_tag_template(name=name,
                                                     force=True,
                                                     retry=self._retry_options)
        elif resource_type == dmt.CatalogTag:
            self._catalog_client.delete_tag(name=name,
                                            retry=self._retry_options)
        elif resource_type == dmt.PolicyTaxonomy:
            self._policy_tag_client.delete_taxonomy(name=name,
                                                    retry=self._retry_options)
        elif resource_type == dmt.PolicyTag:
            self._policy_tag_client.delete_policy_tag(name=name,
                                                      retry=self._retry_options)
        elif resource_type == dmt.DataPolicy:
            self._bq_datapolicy_client.delete_data_policy(
                name=name, retry=self._retry_options)
        else:
            raise cortex_exc.NotImplementedCError(f"Type: {resource_type}")

        logging.info("Deleted existing %s: '%s'", resource_type.__name__,
                     display_name)

    async def _delete_resource_async(self, resource_type: Type[object],
                                     display_name: str, name: str) -> None:
        """Same as above but supports different resource async operations."""
        logging.info("Requesting deletion of %s: '%s'", resource_type.__name__,
                     display_name)

        # Send the delete request and get the long running operation.
        if resource_type == dmt.Lake:
            child_type = dmt.Zone
        elif resource_type == dmt.Zone:
            child_type = dmt.Asset
        elif resource_type == dmt.Asset:
            child_type = None
        else:
            raise cortex_exc.NotImplementedCError(f"Type: {resource_type}")

        # Existing child resources are recursively deleted here because requests
        # don't support cascading delete of nested resources.
        if child_type:
            existing_children = self._list_resources(child_type, name)
            awaitables = [
                self._delete_resource_async(child_type, display_name, name)
                for display_name, name in existing_children.items()
            ]
            exceptions = await _gather_exceptions_async(*awaitables)
            if exceptions:
                raise ExceptionGroup(
                    "Couldn't delete all children under "
                    f"{resource_type.__name__}: '{display_name}'.", exceptions)

        if resource_type == dmt.Lake:
            op = self._dataplex_client.delete_lake(name=name,
                                                   retry=self._retry_options)
        elif resource_type == dmt.Zone:
            op = self._dataplex_client.delete_zone(name=name,
                                                   retry=self._retry_options)
        else:
            op = self._dataplex_client.delete_asset(name=name,
                                                    retry=self._retry_options)

        # Wait for the long running operation to complete.
        try:
            await asyncio.to_thread(op.result,
                                    timeout=self._retry_timeout,
                                    retry=self._retry_options)
        except:
            logging.error("Failed to delete %s: '%s'", resource_type.__name__,
                          display_name)
            raise

        logging.info("Completed deletion of %s: '%s'", resource_type.__name__,
                     display_name)

    def _create_resource(self, resource_spec: Any, parent: str,
                         **kwargs: Any) -> str:
        """Returns the specified created resource unique name.

        Args:
            resource_spec: An instance of a data_mesh_types dataclass associated
                with a resource.
            parent: String representing the parent project or resource. This
                doesn't include the parent policy tag if that applies, which is
                a different arg.
            kwargs:
                DataPolicy: parent_policy
                PolicyTag: parent_policy (optional)
                CatalogTag: template_spec, template_name, column (optional)
        """
        resource_type = type(resource_spec)

        if resource_type == dmt.CatalogTagTemplate:
            resource = dmt_util.get_request_type(resource_spec)
            resource = self._catalog_client.create_tag_template(
                parent=parent,
                tag_template_id=resource_spec.display_name,
                tag_template=resource,
                retry=self._retry_options)
        elif resource_type == dmt.CatalogTag:
            resource = dmt_util.get_request_type(resource_spec, **kwargs)
            _ = self._catalog_client.create_tag(parent=parent,
                                                tag=resource,
                                                retry=self._retry_options)
        elif resource_type == dmt.PolicyTaxonomy:
            resource = dmt_util.get_request_type(resource_spec)
            resource = self._policy_tag_client.create_taxonomy(
                parent=parent, taxonomy=resource, retry=self._retry_options)
        elif resource_type == dmt.PolicyTag:
            resource = dmt_util.get_request_type(resource_spec, **kwargs)
            resource = self._policy_tag_client.create_policy_tag(
                parent=parent, policy_tag=resource, retry=self._retry_options)
        elif resource_type == dmt.DataPolicy:
            resource = dmt_util.get_request_type(resource_spec, **kwargs)
            resource = self._bq_datapolicy_client.create_data_policy(
                parent=parent, data_policy=resource, retry=self._retry_options)
        else:
            raise cortex_exc.NotImplementedCError(f"Type: {resource_type}")

        logging.info("Created %s: '%s'", resource_type.__name__,
                     resource_spec.display_name)
        return resource.name

    async def _create_resource_async(self, resource_spec: Any, parent: str,
                                     **kwargs: Any) -> str:
        """Same as above but supports different resource async operations.

        Args:
            resource_spec: An instance of a data_mesh_types dataclass associated
                with a resource.
            parent: String representing the parent project or resource.
            kwargs:
                Lake: none
                Zone: none
                Asset: resource_name (BQ dataset name)
        """
        resource_type = type(resource_spec)

        # Send the create request and get the long running operation.
        try:
            if resource_type == dmt.Lake:
                resource = dmt_util.get_request_type(resource_spec)
                op = self._dataplex_client.create_lake(
                    parent=parent,
                    lake=resource,
                    lake_id=resource_spec.display_name,
                    retry=self._retry_options)
            elif resource_type == dmt.Zone:
                resource = dmt_util.get_request_type(resource_spec)
                op = self._dataplex_client.create_zone(
                    parent=parent,
                    zone=resource,
                    zone_id=resource_spec.display_name,
                    retry=self._retry_options)
            elif resource_type == dmt.Asset:
                resource = dmt_util.get_request_type(resource_spec, **kwargs)
                op = self._dataplex_client.create_asset(
                    parent=parent,
                    asset=resource,
                    asset_id=resource_spec.display_name,
                    retry=self._retry_options)
            else:
                raise cortex_exc.NotImplementedCError(f"Type: {resource_type}")
            logging.info("Requesting creation of %s: '%s'",
                         resource_type.__name__, resource_spec.display_name)
        # Wait for the existing operation to complete.
        except google_exc.AlreadyExists:
            logging.info("Waiting for existing create operation on %s: '%s'",
                         resource_type.__name__, resource_spec.display_name)
            resource_name = await self._wait_for_lake_creation(
                resource_spec, parent)
            return resource_name

        # Wait for the newly created long running operation to complete.
        try:
            resource = await asyncio.to_thread(op.result,
                                               timeout=self._retry_timeout,
                                               retry=self._retry_options)
        except:
            logging.error("Failed to create %s: '%s'", resource_type.__name__,
                          resource_spec.display_name)
            raise
        logging.info("Completed creation of %s: '%s'", resource_type.__name__,
                     resource_spec.display_name)
        return resource.name  # type: ignore

    def _make_ready_to_create(
            self,
            resource_spec: Any,
            existing_resources: Optional[Dict[str,
                                              str]] = None) -> Optional[str]:
        """Make the specified resource ready for a create request.

        This involves tasks like deleting the existing resource for overwriting
        or marking the resource to skip for not overwriting.

        Returns:
            A string representing an existing resource ID that should not be
            overwritten. Otherwise returns None if the existing resource doesn't
            exist or was deleted to be overwritten.
        """
        resource_type = type(resource_spec)
        existing_id = None
        if existing_resources:
            existing_id = existing_resources.get(resource_spec.display_name)

        if not self.overwrite and existing_id:
            logging.info("Skipping existing %s: '%s'", resource_type.__name__,
                         resource_spec.display_name)
            return existing_id

        if self.overwrite and existing_id:
            self._delete_resource(resource_type, resource_spec.display_name,
                                  existing_id)

        return None

    async def _make_ready_to_create_async(
            self,
            resource_spec: Any,
            existing_resources: Optional[Dict[str,
                                              str]] = None) -> Optional[str]:
        """Same as above but supports different resource async operations."""
        resource_type = type(resource_spec)
        existing_id = None
        if existing_resources:
            existing_id = existing_resources.get(resource_spec.display_name)

        if not self.overwrite and existing_id:
            logging.info("Skipping existing %s: '%s'", resource_type.__name__,
                         resource_spec.display_name)
            return existing_id

        if self.overwrite and existing_id:
            await self._delete_resource_async(resource_type,
                                              resource_spec.display_name,
                                              existing_id)

        return None

    async def _execute_bq_query_async(self, query: str) -> bq_table.RowIterator:
        logging.debug("Executing SQL:\n%s", query)
        # Combines sequential whitespace into a single space. This simplifies
        # unit testing.
        compact_query = re.sub(r"\s+", " ", query)
        try:
            job = self._bq_client.query(query=compact_query)
            return await asyncio.to_thread(job.result)
        except gcp_exc.BadRequest as req_error:
            raise cortex_exc.CriticalError(
                f"Error executing SQL:\n{query}") from req_error

    def _get_asset_policy_member_sets(self, asset_id: str) -> AssetMemberSets:
        iam_policy = self._bq_client.get_iam_policy(asset_id)

        members: AssetMemberSets = defaultdict(set)
        for binding in iam_policy.bindings:
            if binding["role"] == _BQ_ROLES[dmt.BqAssetRole.READER]:
                members[dmt.BqAssetRole.READER] = set(binding["members"])
            elif binding["role"] == _BQ_ROLES[dmt.BqAssetRole.WRITER]:
                members[dmt.BqAssetRole.WRITER] = set(binding["members"])
            elif binding["role"] == _BQ_ROLES[dmt.BqAssetRole.OWNER]:
                members[dmt.BqAssetRole.OWNER] = set(binding["members"])
        return members

    async def _revoke_asset_readers_and_writers_async(
            self, asset_id: str, asset_type: BqAssetType,
            member_sets: AssetMemberSets) -> None:
        readers = member_sets[dmt.BqAssetRole.READER]
        writers = member_sets[dmt.BqAssetRole.WRITER]

        if readers:
            revoke_readers_query = _get_revoke_asset_policy_query(
                dmt.BqAssetRole.READER, asset_id, asset_type, readers)
            await self._execute_bq_query_async(revoke_readers_query)

        if writers:
            revoke_writers_query = _get_revoke_asset_policy_query(
                dmt.BqAssetRole.WRITER, asset_id, asset_type, writers)
            await self._execute_bq_query_async(revoke_writers_query)

    async def _delete_all_row_policies_async(self, asset_id: str) -> None:
        query = _get_drop_all_row_policy_query(asset_id)
        _ = await self._execute_bq_query_async(query)

    def create_catalog_tag_template(
            self,
            project: str,
            tag_template_spec: dmt.CatalogTagTemplate,
            existing_tag_templates: Optional[Dict[str, str]] = None) -> None:
        """Create the specified Catalog Tag Template.

        Args:
            project: String representing project to create resources in.
            tag_template_spec: data_mesh_types.CatalogTagTemplate that
                specifies how to create the Catalog Tag Template.
            existing_tag_templates: Dictionary of display_name: unique_name
                containing the existing tag templates.
        """
        if self._make_ready_to_create(tag_template_spec,
                                      existing_tag_templates):
            return

        _ = self._create_resource(tag_template_spec,
                                  self._parent_project(project))

    def create_catalog_tag_templates(
        self,
        tag_templates_spec: dmt.CatalogTagTemplates,
    ) -> None:
        """Create the specified Catalog Tag Templates.

        Args:
            catalog_tag_templates_spec: data_mesh_types.CatalogTagTemplates that
                specifies a list of Catalog Tag Templates to create.
        """
        existing_tag_templates = self._list_resources(
            dmt.CatalogTagTemplate, tag_templates_spec.project)

        for template in tag_templates_spec.templates:
            self.create_catalog_tag_template(tag_templates_spec.project,
                                             template, existing_tag_templates)

    def _set_data_policy_masked_readers(self, data_policy_name: str,
                                        readers: List[str]) -> None:
        if not readers:
            return

        request = _create_set_iam_policy_request(
            data_policy_name, "roles/bigquerydatapolicy.maskedReader", readers)
        _ = self._bq_datapolicy_client.set_iam_policy(request=request,
                                                      retry=self._retry_options)

    def create_data_policy(
            self,
            project: str,
            data_policy_spec: dmt.DataPolicy,
            parent_policy: str,
            existing_data_policies: Optional[Dict[str, str]] = None) -> None:
        """Create the specified BQ Data Policy.

        Args:
            project: String representing project to create resources in.
            data_policy_spec: data_mesh_types.DataPolicy that specifies how to
                create the data policy.
            parent_policy: String representing the parent policy tag where the
                data policy is applied.
            existing_data_policies: Dictionary of display_name: unique_name
                containing the existing data policies.
        """
        if self._make_ready_to_create(data_policy_spec, existing_data_policies):
            return

        data_policy_name = self._create_resource(data_policy_spec,
                                                 self._parent_project(project),
                                                 parent_policy=parent_policy)

        self._set_data_policy_masked_readers(data_policy_name,
                                             data_policy_spec.masked_readers)

    def _set_policy_tag_unmasked_readers(self, policy_tag_name: str,
                                         readers: List[str]) -> None:
        if not readers:
            return

        request = _create_set_iam_policy_request(
            policy_tag_name, "roles/datacatalog.categoryFineGrainedReader",
            readers)
        _ = self._policy_tag_client.set_iam_policy(request=request,
                                                   retry=self._retry_options)

    def create_policy_tag(
            self,
            project: str,
            policy_tag_spec: dmt.PolicyTag,
            taxonomy_name: str,
            parent_policy: str = "",
            existing_policy_tags: Optional[Dict[str, str]] = None) -> None:
        """Create the specified Policy Tag.

        Args:
            project: String representing project to create resources in.
            policy_tag_spec: data_mesh_types.PolicyTag that specifies how to
                create the policy tag.
            taxonomy_name: String that specifies the taxonomy the policy tag
                belongs to.
            parent_policy: Parent policy tag. Empty for top level policy tags.
            existing_policy_tags: Dictionary of display_name:unique name
                containing the existing policy tags.
        """
        existing_policy_tag_name = self._make_ready_to_create(
            policy_tag_spec, existing_policy_tags)
        if not existing_policy_tag_name:
            existing_policy_tag_name = self._create_resource(
                policy_tag_spec, taxonomy_name, parent_policy=parent_policy)
            self._set_policy_tag_unmasked_readers(
                existing_policy_tag_name, policy_tag_spec.unmasked_readers)

        # Oddly, BQ data policies are direct children of the project, despite
        # having no apparent way to reuse data policies across policy tags.
        existing_data_policies = self._list_resources(
            dmt.DataPolicy, self._parent_project(project))

        for data_policy in policy_tag_spec.data_policies:
            self.create_data_policy(project, data_policy,
                                    existing_policy_tag_name,
                                    existing_data_policies)

        for child_policy_tag in policy_tag_spec.child_policy_tags:
            self.create_policy_tag(project, child_policy_tag, taxonomy_name,
                                   existing_policy_tag_name,
                                   existing_policy_tags)

    def create_policy_taxonomy(
            self,
            project: str,
            policy_taxonomy_spec: dmt.PolicyTaxonomy,
            existing_taxonomies: Optional[Dict[str, str]] = None) -> None:
        """Create the specified policy taxonomy.

        Args:
            project: String representing project to create resources in.
            policy_taxonomy_spec: data_mesh_types.PolicyTaxonomy that specifies
                how to create the taxonomy.
            existing_taxonomies: Dictionary of display_name:unique name
                containing the existing policy taxonomies.
        """
        existing_taxonomy_name = self._make_ready_to_create(
            policy_taxonomy_spec, existing_taxonomies)
        if existing_taxonomy_name:
            existing_tags = self._list_resources(dmt.PolicyTag,
                                                 existing_taxonomy_name)
        else:
            existing_taxonomy_name = self._create_resource(
                policy_taxonomy_spec, self._parent_project(project))
            existing_tags = {}

        for policy_tag in policy_taxonomy_spec.policy_tags:
            self.create_policy_tag(project,
                                   policy_tag,
                                   existing_taxonomy_name,
                                   existing_policy_tags=existing_tags)

    def create_policy_taxonomies(
        self,
        policy_taxonomies_spec: dmt.PolicyTaxonomies,
    ) -> None:
        """Create the specified policy taxonomies.

        Args:
            policy_taxonomies_spec: data_mesh_types.PolicyTaxonomies that
                specifies a list of taxonomies to create.
        """
        existing_taxonomies = self._list_resources(
            dmt.PolicyTaxonomy,
            self._parent_project(policy_taxonomies_spec.project))

        for taxonomy in policy_taxonomies_spec.taxonomies:
            self.create_policy_taxonomy(policy_taxonomies_spec.project,
                                        taxonomy, existing_taxonomies)

    async def create_asset_async(
            self,
            project: str,
            asset_spec: dmt.Asset,
            parent_zone: str,
            existing_assets: Optional[Dict[str, str]] = None) -> None:
        """Asyncronously create the specified data mesh asset.

        Args:
            project: String representing project to create resources in.
            asset_spec: data_mesh_types.Asset specifing how to create the asset.
            parent_zone: String identifying the zone this asset belongs to.
            existing_assets: Dictionary of display_name:unique name
                containing the existing assets.
        """
        existing_asset_name = await self._make_ready_to_create_async(
            asset_spec, existing_assets)

        if existing_asset_name:
            return

        # TODO: verify jinja dict upstream.
        if not asset_spec.asset_name:
            raise cortex_exc.CriticalError(
                "No asset_name in the following asset spec. Check that the "
                f"config is set properly to enable deployment:\n{asset_spec}")

        resource_name = get_bq_dataset_name(project, asset_spec.asset_name)

        try:
            await self._create_resource_async(asset_spec,
                                              parent_zone,
                                              resource_name=resource_name)
        except google_exc.InvalidArgument as arg_error:
            if not arg_error.message:
                raise

            if re.search(r"BigQuery dataset location \w+ is invalid",
                         str(arg_error.message)):
                logging.warning(
                    "Skipping attempt to register BigQuery dataset '%s' in a "
                    "Lake with an mismatched region.", asset_spec.asset_name)

            if re.search(r"Provided resource .* is already attached",
                         str(arg_error.message)):
                logging.warning(
                    "Skipping attempt to register BigQuery dataset '%s', which "
                    "is already registered.", asset_spec.asset_name)
            else:
                raise

    # TODO: consider refactoring with create_lake_async.
    async def create_zone_async(
            self,
            project: str,
            zone_spec: dmt.Zone,
            parent_lake: str,
            existing_zones: Optional[Dict[str, str]] = None) -> None:
        """Asyncronously create the specified data mesh zone.

        Args:
            project: String representing project to create resources in.
            zone_spec: data_mesh_types.Zone specifing how to create the zone.
            parent_lake: String identifying the lake this zone belongs to.
            existing_zones: Dictionary of display_name:unique name
                containing the existing zones.
        """
        existing_zone_name = await self._make_ready_to_create_async(
            zone_spec, existing_zones)

        if existing_zone_name:
            existing_assets = self._list_resources(dmt.Asset,
                                                   existing_zone_name)
        else:
            existing_zone_name = await self._create_resource_async(
                zone_spec, parent_lake)
            existing_assets = {}

        # Create assets concurrently.
        awaitables = [
            self.create_asset_async(project, asset, existing_zone_name,
                                    existing_assets)
            for asset in zone_spec.assets
        ]

        exceptions = await _gather_exceptions_async(*awaitables)
        if exceptions:
            raise ExceptionGroup(
                "Couldn't create all assets under zone: "
                f"'{zone_spec.display_name}'.", exceptions)

    async def create_lake_async(
            self,
            project: str,
            lake_spec: dmt.Lake,
            existing_lakes: Optional[Dict[str, str]] = None) -> None:
        """Asyncronously create the specified data mesh lake.

        Args:
            project: String representing project to create resources in.
            lake_spec: data_mesh_types.Lake specifing how to create the lake.
            existing_lakes: Dictionary of display_name:unique name
                containing the existing lakes.

        Raises:
            An ExceptionGroup of any nested exceptions. When a child throws an
            exception, siblings are allowed to finish running.
        """
        existing_lake_name = await self._make_ready_to_create_async(
            lake_spec, existing_lakes)

        if existing_lake_name:
            existing_zones = self._list_resources(dmt.Zone, existing_lake_name)
        else:
            # TODO: programmatically find a region if deployment is multiregion.
            # Lakes can't be a multi region, so we need a specific one to be the
            # parent resource.
            parent = self._dataplex_client.common_location_path(
                project, lake_spec.region)
            existing_lake_name = await self._create_resource_async(
                lake_spec, parent)
            existing_zones = {}

        # Create zones concurrently.
        awaitables = [
            self.create_zone_async(project, zone, existing_lake_name,
                                   existing_zones) for zone in lake_spec.zones
        ]

        exceptions = await _gather_exceptions_async(*awaitables)
        if exceptions:
            raise ExceptionGroup(
                "Couldn't create all zones under lake: "
                f"'{lake_spec.display_name}'.", exceptions)

    async def create_lakes_async(
        self,
        lakes_spec: dmt.Lakes,
    ) -> None:
        """Asyncronously create the specified data mesh lakes.

        Args:
            lakes_spec: data_mesh_types.Lakes specifing how to create multiple
                lakes.

        Raises:
            An ExceptionGroup of any nested exceptions. When a child throws an
            exception, siblings are allowed to finish running.
        """
        # Lakes can't be a multi region, so we need a specific one to be the
        # parent resource.
        # TODO: consider not enforcing unique lakes across regions.
        regions = [l.region for l in lakes_spec.lakes]
        existing_lakes = {}
        for region in regions:
            parent = self._dataplex_client.common_location_path(
                lakes_spec.project, region)
            try:
                existing_lakes.update(self._list_resources(dmt.Lake, parent))
            except google_exc.InvalidArgument as arg_error:
                if (arg_error.message and re.search(r"Malformed name: .+/lakes",
                                                    str(arg_error.message))):
                    raise cortex_exc.CriticalError(
                        f"Invalid lake location, verify that '{region}' is an "
                        "individual region, not multi-region.") from arg_error
                else:
                    raise

        # Create lakes concurrently.
        awaitables = [
            self.create_lake_async(lakes_spec.project, lake, existing_lakes)
            for lake in lakes_spec.lakes
        ]
        exceptions = await _gather_exceptions_async(*awaitables)
        if exceptions:
            raise ExceptionGroup("Couldn't create all lakes.", exceptions)

    async def create_metadata_resources_async(
        self,
        tag_templates_specs: Optional[List[dmt.CatalogTagTemplates]] = None,
        policy_taxonomies_specs: Optional[List[dmt.PolicyTaxonomies]] = None,
        lakes_specs: Optional[List[dmt.Lakes]] = None,
    ) -> None:
        """Asyncronously create the specified data mesh metadata resources.

        Args:
            tag_templates_specs: List of data_mesh_types.CatalogTagTemplates
                specifing how to create multiple Catalog Tag Templates.
            policy_taxonomies_specs: List of data_mesh_types.PolicyTaxonomies
                specifing how to create multiple policy taxonomies.
            lakes_specs: List of data_mesh_types.Lakes specifing how to create
                multiple lakes.

        Raises:
            An ExceptionGroup of any nested exceptions. When a child throws an
            exception, siblings are allowed to finish running.
        """
        # Create awaitables for all resources that can be run concurrently for
        # each spec.
        awaitables = []
        if tag_templates_specs:
            logging.info("Creating Catalog Tag Templates.")
            awaitables.extend([
                asyncio.to_thread(self.create_catalog_tag_templates,
                                  tag_templates)
                for tag_templates in tag_templates_specs
            ])
        if policy_taxonomies_specs:
            logging.info("Creating Policy Tags.")
            awaitables.extend([
                asyncio.to_thread(self.create_policy_taxonomies,
                                  policy_taxonomies)
                for policy_taxonomies in policy_taxonomies_specs
            ])
        if lakes_specs:
            logging.info("Creating Lakes.")
            awaitables.extend(
                [self.create_lakes_async(lakes) for lakes in lakes_specs])

        exceptions = await _gather_exceptions_async(*awaitables)
        if exceptions:
            raise ExceptionGroup("Error encounted while creating data mesh.",
                                 exceptions)

    def create_catalog_tag(
            self,
            project: str,
            tag_spec: dmt.CatalogTag,
            template_spec: dmt.CatalogTagTemplate,
            table_entry_name: str,
            column: Optional[str] = None,
            existing_catalog_tags: Optional[Dict[str, str]] = None) -> None:
        """Create the specified Catalog Tag.

        Args:
            project: String representing project to create resources in.
            tag_spec: data_mesh_types.CatalogTag that specifies how to create
                the Catalog Tag.
            template_spec: data_mesh_types.CatalogTagTemplate that defines the
                given tag schema.
            table_entry_name: String representing the Data Catalog table entry
                that the tag is annotating.
            column: String representing the name of the column if the tag is
                column level rather than asset level. Nested fields are
                specified with '.' separators, e.g. `struct.nested`.
            existing_catalog_tags: Dictionary of
                template_display_name: unique_name containing the existing tags.
                Catalog Tags can be keyed by the template display name because
                there cannot be more than one tag on a particular entry for the
                same template.
        """
        relevant_existing_catalog_tags = {}
        if existing_catalog_tags:
            for k, v in existing_catalog_tags.items():
                prefix, _, tag_name = k.partition(":")
                # Add relevant column tags.
                if column is not None and prefix == column:
                    relevant_existing_catalog_tags[tag_name] = v
                # Add relevant asset tags.
                elif column is None and prefix == _ASSET_TAG_PREFIX:
                    relevant_existing_catalog_tags[tag_name] = v

        if self._make_ready_to_create(tag_spec, relevant_existing_catalog_tags):
            return

        template_name = self._get_resource_name(dmt.CatalogTagTemplate,
                                                template_spec.display_name,
                                                project,
                                                self._existing_tag_templates)
        _ = self._create_resource(tag_spec,
                                  table_entry_name,
                                  template_spec=template_spec,
                                  template=template_name,
                                  column=column)

    def create_catalog_tags(self,
                            bq_asset_annotation_spec: dmt.BqAssetAnnotation,
                            templates_spec: dmt.CatalogTagTemplates) -> None:
        """Create and annotate the catalog tags in the specified BQ asset.

        This creates the catalog tags at both the asset and field levels.

        Args:
            bq_asset_annotation_spec: data_mesh_types.BqAssetAnnotation that
                specifies a list of asset and field catalog tags to create.
            templates_spec: data_mesh_types.CatalogTagTemplates that specifies
                a list of templates that include all those referenced in the
                BqAssetAnnotation.
        """
        project, dataset, table = bq_asset_annotation_spec.name.split(".")
        entry_name = get_bq_entry_name(project, dataset, table)

        try:
            table_entry = self._catalog_client.lookup_entry(
                request={"linked_resource": entry_name})
        except google_exc.PermissionDenied:
            # TODO: collect all skipped tables into single location/msg.
            logging.warning(
                "Permission denied on asset '%s' or it does not exist, "
                "skipping catalog tags.", bq_asset_annotation_spec.name)
            return

        templates_spec_map = {
            template.display_name: template
            for template in templates_spec.templates
        }

        # This will include catalog tags at the asset and field levels.
        existing_catalog_tags = self._list_resources(dmt.CatalogTag,
                                                     table_entry.name)
        # TODO: check if tag is used at the wrong level (field vs asset).
        for asset_tag in bq_asset_annotation_spec.catalog_tags:
            template_spec = templates_spec_map[asset_tag.display_name]
            self.create_catalog_tag(project,
                                    asset_tag,
                                    template_spec,
                                    table_entry.name,
                                    existing_catalog_tags=existing_catalog_tags)

        for field in bq_asset_annotation_spec.fields:
            for field_tag in field.catalog_tags:
                template_spec = templates_spec_map[field_tag.display_name]
                self.create_catalog_tag(
                    project,
                    field_tag,
                    template_spec,
                    table_entry.name,
                    column=field.name,
                    existing_catalog_tags=existing_catalog_tags)

    def _is_matching_policy(
            self, catalog_tag: dmt.CatalogTag,
            tag_template: dmt.CatalogTagTemplate,
            filters: List[dmt.CatalogTagTemplatePolicyFilter]) -> bool:
        """Returns bool indicating if the catalog_tag matches a given policy."""

        template_field_names = {f.display_name for f in tag_template.fields}
        tag_values = {f.display_name: f.value for f in catalog_tag.fields}

        for filter_field in filters:
            if filter_field.field_name not in template_field_names:
                # TODO: validate data types.
                # TODO: refactor to ConfigSpec logical validation step.
                # That would allow this error to be caught in parsing, rather
                # than mid deployment.
                raise cortex_exc.CriticalError(
                    f"Cannot filter on '{filter_field.field_name}'. Not a "
                    "valid field in the tag template "
                    f"'{tag_template.display_name}'.")

            # TODO: support filters beyond equality.
            if filter_field.value != tag_values[filter_field.field_name]:
                return False

        return True

    def _get_matching_asset_policies(
            self, asset_catalog_tags: List[dmt.CatalogTag],
            templates_spec: dmt.CatalogTagTemplates) -> List[dmt.BqAssetPolicy]:
        templates_spec_map = {
            template.display_name: template
            for template in templates_spec.templates
        }

        asset_policies = []
        for catalog_tag in asset_catalog_tags:
            template_spec = templates_spec_map.get(catalog_tag.display_name)
            if not template_spec:
                raise cortex_exc.NotFoundCError(
                    "No matching template found for CatalogTag: "
                    f"'{catalog_tag.display_name}'.")
            for asset_policy in template_spec.asset_policies:
                if self._is_matching_policy(catalog_tag, template_spec,
                                            asset_policy.filters):
                    asset_policies.append(asset_policy.policy)

        return asset_policies

    def _get_matching_field_policy(
            self, field_catalog_tags: List[dmt.CatalogTag],
            templates_spec: dmt.CatalogTagTemplates
    ) -> Optional[dmt.PolicyTagId]:
        templates_spec_map = {
            template.display_name: template
            for template in templates_spec.templates
        }

        for catalog_tag in field_catalog_tags:
            template_spec = templates_spec_map.get(catalog_tag.display_name)
            if not template_spec:
                raise cortex_exc.NotFoundCError(
                    "No matching template found for CatalogTag: "
                    f"'{catalog_tag.display_name}'.")
            for field_policy in template_spec.field_policies:
                if self._is_matching_policy(catalog_tag, template_spec,
                                            field_policy.filters):
                    return field_policy.policy_tag_id

        return None

    def update_policies_from_catalog_tags(
            self, bq_asset_annotation_spec: dmt.BqAssetAnnotation,
            templates_spec: dmt.CatalogTagTemplates) -> dmt.BqAssetAnnotation:
        """Returns a BqAssetAnnotation that includes Catalog tag policies.

        Updates the annotation to include new asset and field level policies
        that satisfied the tag template policy filters.

        Asset policies will all be applied in the following order:
            * asset policies specified in the original annotation
            * matching CatalogTagTemplateAssetPolicy in the order defined
        Fields can only have one policy tag, the precedence for selection is:
            * policy tag specified in the original annotation
            * first matching CatalogTagTemplateFieldPolicy

        Args:
            bq_asset_annotation_spec: data_mesh_types.BqAssetAnnotation that
                specifies a list of asset and field policies.
            templates_spec: data_mesh_types.CatalogTagTemplates that specifies
                a list of templates that include all those referenced in the
                BqAssetAnnotation.

        Raises:
            ValueError: When no matching template is found or the template
            doesn't contain the specified field.
        """
        updated_spec = copy.deepcopy(bq_asset_annotation_spec)

        # Asset policies
        # TODO: Consider using catalog tags from API rather than spec.
        # Actual tags applied may not match spec since existing tags that don't
        # collide aren't removed.
        matching_asset_policies = self._get_matching_asset_policies(
            bq_asset_annotation_spec.catalog_tags, templates_spec)
        updated_spec.asset_policies.extend(matching_asset_policies)

        # Field policies
        for field in updated_spec.fields:
            # Use existing policy tag if specified.
            if field.policy_tag_id:
                continue

            matching_field_policy = self._get_matching_field_policy(
                field.catalog_tags, templates_spec)
            if matching_field_policy:
                field.policy_tag_id = matching_field_policy

        return updated_spec

    def _maybe_set_policy_tag(self, asset_id: str,
                              field_spec: dmt.BqAssetFieldAnnotation,
                              field_repr: Dict[str, Any]) -> bool:
        """Return a bool indicating if the policy tag was set.

        This method sets the policy_tag by mutating the field_repr argument.
        """
        policy_tag_id = field_spec.policy_tag_id
        existing_policy_tag = field_repr.get("policyTags") or {}
        existing_policy_tag_names = existing_policy_tag.get("names")
        existing_policy_tag_id = None

        if existing_policy_tag_names:
            existing_policy_tag_name = existing_policy_tag_names[0]
            name_parts = existing_policy_tag_name.split("/")
            existing_taxonomy_name = "/".join(name_parts[:-2])
            try:
                existing_policy_tag_id = dmt.PolicyTagId(
                    self._policy_tag_client.get_policy_tag(
                        name=existing_policy_tag_name).display_name,
                    self._policy_tag_client.get_taxonomy(
                        name=existing_taxonomy_name).display_name)
            except google_exc.NotFound as found_error:
                # If the policy taxonomy isn't found, consider it not to exist.
                # This can happen if the taxonomy was recently deleted and
                # reference deletes haven't yet had time to propagate. This
                # occurs when overwriting. In those cases we ignore the
                # exception and just overwrite obsolete policies.
                details_str = "; ".join([str(d) for d in found_error.details])
                if not re.search(r"Requested taxonomy id of \w+ was not found",
                                 details_str):
                    raise

        if self._should_overwrite(existing_policy_tag_id,
                                  policy_tag_id,
                                  enable_delete=True):
            if policy_tag_id:
                project = asset_id.split(".")[0]
                policy_taxonomy_name = self._get_resource_name(
                    dmt.PolicyTaxonomy, policy_tag_id.taxonomy,
                    self._parent_project(project))
                policy_tag_name = self._get_resource_name(
                    dmt.PolicyTag, policy_tag_id.display_name,
                    policy_taxonomy_name)
                # Oddly the schema expects a list of policy tags even though
                # only 1 can be assigned to the column.
                field_repr["policyTags"] = {"names": [policy_tag_name]}
                policy_tag_id_str = ":".join(
                    [policy_tag_id.taxonomy, policy_tag_id.display_name])
            else:
                field_repr["policyTags"] = {"names": []}
                policy_tag_id_str = "None"

            existing_policy_tag_id_str = "None"
            if existing_policy_tag_id:
                existing_policy_tag_id_str = ":".join([
                    existing_policy_tag_id.taxonomy,
                    existing_policy_tag_id.display_name
                ])

            logging.info("Setting %s.%s policy tag from %s to %s", asset_id,
                         field_spec.name, existing_policy_tag_id_str,
                         policy_tag_id_str)
            return True
        return False

    def annotate_bq_asset_schema(
            self,
            bq_asset_annotation_spec: dmt.BqAssetAnnotation,
            deploy_descriptions: bool = True,
            deploy_acls: bool = True) -> Set[str]:
        """Annotate the BQ asset schema with descriptions and policy tags.

        Other existing schema fields (e.g. field type) are untouched. Doesn't
        touch any fields or their field schemas (e.g. policy tag) if they are
        not specified in the spec.

        Args:
            bq_asset_annotation_spec: data_mesh_types.BqAssetAnnotation that
                specifies a list of asset and field descriptions to annotate.

        Returns:
            updated_annotations: Set of strings representing annotations
                that were updated after overwrite setting is applied, e.g.
                {"table_name.description", "table_name.field_name.description"}
        """

        # Schema is always included in fields to update because we determine
        # whether an update is needed per field + annotations, e.g. field
        # description, field policy, etc.
        fields_to_update = ["schema"]
        updated_annotations = set()
        asset_id = bq_asset_annotation_spec.name

        try:
            table = self._bq_client.get_table(asset_id)
        except google_exc.NotFound:
            # TODO: collect all skipped tables into single location/msg.
            logging.warning("Asset '%s' not found, skipping annotations.",
                            asset_id)
            return set()

        # Conditionally set the table description.
        if deploy_descriptions and self._should_overwrite(
                table.description, bq_asset_annotation_spec.description):
            fields_to_update.append("description")
            table.description = bq_asset_annotation_spec.description
            updated_annotations.add(f"{asset_id}:description")

        # Update field schema.
        original_schema = table.schema
        field_annotations_map = {
            field.name: field for field in bq_asset_annotation_spec.fields
        }

        # TODO: Support nested fields.
        new_schema = []
        for field in original_schema:
            field_annotation = field_annotations_map.get(field.name)
            full_field_name = f"{asset_id}.{field.name}"

            # Don't update field schema if no annotation exists.
            if not field_annotation:
                logging.warning("No annotation found for %s.", full_field_name)
                new_schema.append(field)
                continue

            field_repr = field.to_api_repr()

            # Conditionally set the field description.
            if deploy_descriptions and self._should_overwrite(
                    field_repr.get("description"),
                    field_annotation.description):
                field_repr["description"] = field_annotation.description
                updated_annotations.add(f"{full_field_name}:description")

            # Conditionally set the policy tag.
            if deploy_acls and self._maybe_set_policy_tag(
                    asset_id, field_annotation, field_repr):
                updated_annotations.add(f"{full_field_name}:policy_tag")

            new_field = bigquery.SchemaField.from_api_repr(field_repr)
            new_schema.append(new_field)

        table.schema = new_schema
        _ = self._bq_client.update_table(table,
                                         fields_to_update,
                                         retry=self._retry_options)
        return updated_annotations

    async def grant_bq_asset_policies_async(
            self, bq_asset_annotation_spec: dmt.BqAssetAnnotation,
            bq_asset_types: Dict[str, BqAssetType]) -> None:
        """Grants the appropriate BQ asset level policies.

        Unlike the behavior of other resoures and annotations, the overwrite
        flag will not cause existing owners to be removed. This is a safeguard
        to prevent unintended loss of access. Readers and writers will behave
        normally and will be removed on overwrite.

        Args:
            bq_asset_annotation_spec: data_mesh_types.BqAssetAnnotation that
                specifies a list of asset policies to create.
            bq_asset_types: A dictionary mapping each asset in the BQ dataset
                to its BqAssetType. The key is the table ID without project or
                dataset.
        """
        asset_id = bq_asset_annotation_spec.name
        table_id = asset_id.split(".")[-1]
        asset_type = bq_asset_types[table_id]
        member_sets = self._get_asset_policy_member_sets(asset_id)

        # If overwrite is enabled, remove existing readers & writers, even if no
        # new policies are specified.
        if self.overwrite:
            await self._revoke_asset_readers_and_writers_async(
                asset_id, asset_type, member_sets)

        # Policies are applied serially to prevent conflicts.
        # https://cloud.google.com/iam/docs/faq#policy-etag-conflict
        for policy in bq_asset_annotation_spec.asset_policies:
            if not policy.principals:
                continue
            query = _get_create_asset_policy_query(policy, asset_id, asset_type)
            _ = await self._execute_bq_query_async(query)

        new_member_sets = self._get_asset_policy_member_sets(asset_id)

        for role in dmt.BqAssetRole:
            added_members = new_member_sets[role].difference(member_sets[role])
            deleted_members = member_sets[role].difference(
                new_member_sets[role])

            if added_members:
                logging.info("%s:%s added members: %s.", asset_id, role.name,
                             ", ".join(added_members))
            if deleted_members:
                logging.info("%s:%s deleted members: %s.", asset_id, role.name,
                             ", ".join(deleted_members))

    async def grant_bq_asset_row_policies_async(
            self, bq_asset_annotation_spec: dmt.BqAssetAnnotation,
            bq_asset_types: Dict[str, BqAssetType]) -> None:
        """Grants the appropriate BQ asset row level policies.

        Row level access policies are only supported on tables and not views.

        Args:
            bq_asset_annotation_spec: data_mesh_types.BqAssetAnnotation that
                specifies a list of row policies to create.
            bq_asset_types: A dictionary mapping each asset in the BQ dataset
                to its BqAssetType. The key is the table ID without project or
                dataset.
        """
        asset_id = bq_asset_annotation_spec.name
        table_id = asset_id.split(".")[-1]

        if bq_asset_types[table_id] == BqAssetType.VIEW:
            if bq_asset_annotation_spec.row_policies:
                raise cortex_exc.CriticalError(
                    "Row policies are only supported on tables, "
                    f"'{asset_id}' is a view.")
            return

        # If overwrite is enabled, remove existing policies, even if no new
        # policies are specified.
        if self.overwrite:
            logging.info("Dropping all existing row level policies on %s.",
                         asset_id)
            await self._delete_all_row_policies_async(asset_id)

        for policy in bq_asset_annotation_spec.row_policies:
            if not policy.readers:
                continue
            query = _get_grant_row_policy_query(asset_id, policy,
                                                self.overwrite)
            logging.info("Potentially granting row level access with:\n%s",
                         query)
            _ = await self._execute_bq_query_async(query)

    async def annotate_bq_asset_async(
            self,
            bq_asset_annotation_spec: dmt.BqAssetAnnotation,
            templates_spec: dmt.CatalogTagTemplates,
            bq_asset_types: Dict[str, BqAssetType],
            deploy_descriptions: bool = True,
            deploy_catalog: bool = True,
            deploy_acls: bool = True) -> None:
        """Annotate the BQ asset.

        This includes:
            Table & field descriptions
            Field policy tags
            Table & field catalog tags.

        Args:
            bq_asset_annotation_spec: data_mesh_types.BqAssetAnnotation that
                specifies annotations for a particular BQ asset.
            templates_spec: data_mesh_types.CatalogTagTemplates that specifies
                a list of templates that include all those referenced in the
                BqAssetAnnotation.
            bq_asset_types: A dictionary mapping each asset in the BQ dataset
                to its BqAssetType. The key is the table ID without project or
                dataset.
        """
        asset_id = bq_asset_annotation_spec.name

        try:
            _ = self._bq_client.get_table(asset_id)
        except google_exc.NotFound:
            # TODO: collect all skipped tables into single location/msg.
            logging.warning("Asset '%s' not found, skipping annotations.",
                            asset_id)
            return

        logging.info("Beginning annotating BQ Asset: '%s'.", asset_id)
        try:
            bq_asset_annotation_spec = self.update_policies_from_catalog_tags(
                bq_asset_annotation_spec, templates_spec)

            if deploy_descriptions or deploy_acls:
                self.annotate_bq_asset_schema(
                    bq_asset_annotation_spec,
                    deploy_descriptions=deploy_descriptions,
                    deploy_acls=deploy_acls)

            if deploy_catalog:
                self.create_catalog_tags(bq_asset_annotation_spec,
                                         templates_spec)

            # TODO: glossary terms.

            if deploy_acls:
                await self.grant_bq_asset_policies_async(
                    bq_asset_annotation_spec, bq_asset_types)
                await self.grant_bq_asset_row_policies_async(
                    bq_asset_annotation_spec, bq_asset_types)
        except Exception as e:
            raise cortex_exc.CriticalError(
                f"Failed to annotate '{asset_id}'") from e

    # TODO: consider refactor so deployment options use different methods.
    async def annotate_bq_assets_async(
            self,
            bq_asset_annotation_specs: List[dmt.BqAssetAnnotation],
            templates_specs: List[dmt.CatalogTagTemplates],
            deploy_descriptions: bool = True,
            deploy_catalog: bool = True,
            deploy_acls: bool = True) -> None:
        """Annotate multiple BQ assets concurrently.

        All of the included resources must belong to the same project and
        dataset. This method does not use the templates_specs to create those
        resources. They should already be created. The specs are used to
        populate the tag instances properly.

        Args:
            bq_asset_annotation_specs: Optional list of
                data_mesh_types.BqAssetAnnotation that specifies annotations for
                a group of BQ assets.
            templates_spec: Optional list of data_mesh_types.CatalogTagTemplates
                that specifies a list of templates that include all those
                referenced in the BqAssetAnnotations.
        """
        if not bq_asset_annotation_specs:
            logging.warning("Attempting to deploy empty list of annotations.")
            return

        project, dataset, _ = bq_asset_annotation_specs[0].name.split(".")

        # Populate member w/ existing templates available for annotation.
        self._existing_tag_templates = self._list_resources(
            dmt.CatalogTagTemplate, project)

        # Flatten all CatalogTagTemplates into single one. If the annotations
        # don't include catalog tags, then tag templates aren't necessary.
        all_templates_spec = dmt.CatalogTagTemplates("", [])
        if templates_specs:
            all_templates_spec.project = templates_specs[0].project
            for templates_spec in templates_specs:
                all_templates_spec.templates.extend(templates_spec.templates)

        bq_asset_types = self._list_bq_asset_types(project, dataset)
        awaitables = [
            self.annotate_bq_asset_async(annotation_spec, all_templates_spec,
                                         bq_asset_types, deploy_descriptions,
                                         deploy_catalog, deploy_acls)
            for annotation_spec in bq_asset_annotation_specs
        ]

        exceptions = await _gather_exceptions_async(*awaitables)
        if exceptions:
            raise ExceptionGroup("Error encounted while annotating BQ assets.",
                                 exceptions)
