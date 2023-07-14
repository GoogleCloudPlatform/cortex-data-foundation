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
"""prompt_toolkit completers"""

import logging
import subprocess
import pathlib
from typing import Any, Iterable, List, Optional

from prompt_toolkit.completion import Completer, Completion, WordCompleter

import googleapiclient.discovery
from google.cloud import bigquery
from google.cloud import storage
from google.auth.credentials import Credentials
from googleapiclient.errors import HttpError


class GCPProjectCompleter(Completer):
    """Completer with GCP projects"""

    def __init__(self, credentials: Optional[Credentials] = None):
        self.service_v1 = googleapiclient.discovery.build(
            "cloudresourcemanager",
            "v1",
            credentials=credentials,
            cache_discovery=False)
        self.use_cli = False


    def get_completions(self, document, complete_event) -> Iterable[Completion]:
        return self.get_projects_completions(document.current_line,
                                             document.cursor_position)

    def get_projects_completions(self,
                                 filter_str: str,
                                 cursor_position: int) -> Iterable[Completion]:
        if len(filter_str) <= 1:
            return []
        if not self.use_cli:
            try:
                logging.disable(logging.ERROR)
                projects = self.get_projects(filter_str)
                has_projects = False
                for p in projects:
                    has_projects = True
                    yield Completion(p,
                                start_position=-cursor_position)
                if not has_projects:
                    try:
                        project = (self.service_v1.projects().get(
                                        projectId=filter_str).execute())
                        if project:
                            yield Completion(filter_str,
                                start_position=-cursor_position)
                    except: # pylint:disable=bare-except
                        # No project or access denied - expected
                        pass
            except HttpError as ex:
                # If Resource Manager API is not enabled, try using gcloud CLI
                if ex.status_code == 403:
                    self.use_cli = True
                    projects = self.get_projects_completions(filter_str,
                                                         cursor_position)
                    for p in projects:
                        yield p
                else:
                    raise
            finally:
                logging.disable(logging.NOTSET)
        else:
            projects = self.get_projects_cli(filter_str)
            for p in projects:
                yield Completion(p,
                            start_position=-cursor_position)


    def get_projects(self, filter_str: Optional[str] = None) -> Iterable[Any]:
        if filter_str is not None:
            query = (f"projectId:*{filter_str}* name:*{filter_str}* "
                    "lifecycleState:ACTIVE")
        else:
            query = "lifecycleState:ACTIVE"
        page_results = self.service_v1.projects().list(filter=query).execute()
        if page_results and "projects" in page_results:
            return [p["projectId"] for p in page_results["projects"]]
        else:
            return []


    def get_projects_cli(self, filter_str: Optional[str] = None) -> List[str]:
        if filter_str is not None:
            query = (f"projectId:*{filter_str}* name:*{filter_str}* "
                    "lifecycleState:ACTIVE")
        else:
            query = "lifecycleState:ACTIVE"
        gcloud_result = subprocess.run(("gcloud projects list --filter="
                                        f"'{query}' "
                                        "--format='value(projectId)' "
                                        "--limit=100 --quiet"),
                                        stdout=subprocess.PIPE,
                                        shell=True,
                                        check=False)
        if (gcloud_result and gcloud_result.stdout
                and gcloud_result.returncode == 0):
            projects_str = gcloud_result.stdout.decode("utf-8").strip()
            projects = [p.strip() for p in projects_str.split("\n")]
            projects = list(filter(lambda p: len(p) > 0, projects))
            return projects
        else:
            return []


    def get_project_number(self, project_id: str):
        project_result = (self.service_v1.projects().get(
            projectId=project_id).execute())
        return project_result["projectNumber"]


class BigQueryDatasetCompleter(WordCompleter):
    """Completer with BigQuery Tables"""

    def __init__(self, project_id: str,
                 client: Optional[bigquery.Client] = None):
        self.project = project_id
        self.client = client
        try:
            logging.disable(logging.ERROR)
            if self.client is None:
                self.client = bigquery.Client(self.project)
            super().__init__(
                words=[
                    d.reference.dataset_id
                    for d in self.client.list_datasets(self.project)
                ],
                ignore_case=True,
                match_middle=True,
            )
        finally:
            logging.disable(logging.NOTSET)


class StorageBucketCompleter(Completer):
    """Completer with Storage Buckets"""

    def __init__(self,
                 project_id: Optional[str] = None,
                 client: Optional[storage.Client] = None):
        if client is None:
            self.client = storage.Client(project_id)
        else:
            self.client = client

    def get_completions(self, document, complete_event):
        del complete_event
        filter_str = document.current_line
        if len(filter_str) > 1:
            try:
                logging.disable(logging.ERROR)
                buckets = self.get_buckets(filter_str)
            finally:
                logging.disable(logging.NOTSET)
            for p in buckets:
                yield Completion(p.name,
                                 start_position=-document.cursor_position)

    def get_buckets(self, filter_str: Optional[str] = None) -> Iterable[Any]:
        page_results = self.client.list_buckets(
            prefix=filter_str,
            page_size=100,
        )
        return page_results


class RegionsCompleter(WordCompleter):
    """Completer with GCP regions"""

    def __init__(self,
                 include_multiregions = True,
                 source_region: Optional[str] = None):
        self.regions = []
        regions_file = pathlib.Path(__file__).parent.joinpath("bq_regions.txt")
        regions_str = regions_file.read_text("utf-8")
        regions_lines = [line.lower().strip()
                                    for line in regions_str.split("\n")]
        # Filter out empty lines
        regions = filter(lambda line: len(line) > 0, regions_lines)
        # Exclude `us`` and `eu` if not needed.
        if not include_multiregions:
            regions = filter(lambda r: r != "us" and r != "eu", regions)
        # Sort.
        all_regions: list[str] = list(sorted(regions))

        if source_region is None or source_region == "":
            self.regions = all_regions
        else:
            location = source_region.lower()
            self.regions = list(
                filter(lambda r: r.startswith(location), all_regions))
        super().__init__(words=self.regions,
                         ignore_case=True,
                         match_middle=False)


class ServiceAccountsCompleter(WordCompleter):
    """Completer with Service Accounts"""

    def __init__(self,
                 project_id: str,
                 credentials: Optional[Credentials] = None):
        try:
            logging.disable(logging.ERROR)
            service = googleapiclient.discovery.build("iam",
                                                    "v1",
                                                    credentials=credentials)
            service_accounts = (service.projects().serviceAccounts().list(
                name="projects/" + project_id).execute())
            if "accounts" in service_accounts:
                self.accounts = [a["email"]
                                 for a in service_accounts["accounts"]]
            else:
                self.accounts = []
            super().__init__(words=self.accounts,
                            ignore_case=True,
                            match_middle=False)
        finally:
            logging.disable(logging.NOTSET)
