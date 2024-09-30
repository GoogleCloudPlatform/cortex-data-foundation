# Copyright 2024 Google
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Cortex BigQuery Client.
"""

# pylint: disable=line-too-long
# Disabled to accomodate docstring from base Client class

from typing import Optional
from typing import Sequence
from typing import Union

from google.api_core import retry as retries
from google.api_core.client_info import ClientInfo

from google.cloud import bigquery
from google.cloud.bigquery import job
from google.cloud.bigquery import table
from google.cloud.bigquery.client import TimeoutType
from google.cloud.bigquery.retry import DEFAULT_RETRY
from google.cloud.bigquery.retry import DEFAULT_TIMEOUT

from common.py_libs import configs
from common.py_libs import constants

class CortexBQClient(bigquery.Client):
    """
    Cortex specific BigQuery client.

    1. Makes sure that if default_query_job_config and default_load_job_config
        have Cortex job labels.
    2. Makes sure client_info has Cortex user-agent header.
    3. Maintains an instance variable named allow_telemetry that retrieves
       ``allowTelemetry`` config setting from config.json file to indicate if
       telemetry has been opted in.

    Passes arguments to parent class.

    Args:
        project (Optional[str]):
            Project ID for the project which the client acts on behalf of.
            Will be passed when creating a dataset / job. If not passed,
            falls back to the default inferred from the environment.
        credentials (Optional[google.auth.credentials.Credentials]):
            The OAuth2 Credentials to use for this client. If not passed
            (and if no ``_http`` object is passed), falls back to the
            default inferred from the environment.
        _http (Optional[requests.Session]):
            HTTP object to make requests. Can be any object that
            defines ``request()`` with the same interface as
            :meth:`requests.Session.request`. If not passed, an ``_http``
            object is created that is bound to the ``credentials`` for the
            current object.
            This parameter should be considered private, and could change in
            the future.
        location (Optional[str]):
            Default location for jobs / datasets / tables.
        default_query_job_config (Optional[google.cloud.bigquery.job.QueryJobConfig]):
            Default ``QueryJobConfig``.
            Will be merged into job configs passed into the ``query`` method.
        default_load_job_config (Optional[google.cloud.bigquery.job.LoadJobConfig]):
            Default ``LoadJobConfig``.
            Will be merged into job configs passed into the ``load_table_*`` methods.
        client_info (Optional[google.api_core.client_info.ClientInfo]):
            The client info used to send a user-agent string along with API
            requests. If ``None``, then default info will be used. Generally,
            you only need to set this if you're developing your own library
            or partner tool.
        client_options (Optional[Union[google.api_core.client_options.ClientOptions, Dict]]):
            Client options used to set user options on the client. API Endpoint
            should be set through client_options.

    Raises:
        google.auth.exceptions.DefaultCredentialsError:
            Raised if ``credentials`` is not specified and the library fails
            to acquire default credentials.
    """

    def __init__(
        self,
        project=None,
        credentials=None,
        _http=None,
        location=None,
        default_query_job_config=None,
        default_load_job_config=None,
        client_info=None,
        client_options=None,
    ) -> None:

        try:
            # Load config from environment variable
            cortex_config = configs.load_config_file_from_env()

            # Gets allowTelemetry config and defaults to True
            self.allow_telemetry = cortex_config.get("allowTelemetry", True)
        except (FileNotFoundError, PermissionError):
            # File used by telemetry only and the path assumes execution
            # using Cloud Build.
            #
            # Access while executing locally cannot be assumed. If file is not
            # available then continue execution telemetry opted-out.
            #
            # File not accessible or found, setting allow_telemetry to false.
            self.allow_telemetry = False

        if self.allow_telemetry:
            # Check for default_query_job_config and add cortex_runtime label

            if not default_query_job_config:
                default_query_job_config = bigquery.QueryJobConfig()

            default_query_job_config.labels.update(constants.CORTEX_JOB_LABEL)

            # Check for default_load_job config and add cortex_runtime label
            if not default_load_job_config:
                default_load_job_config = bigquery.LoadJobConfig()

            default_load_job_config.labels.update(constants.CORTEX_JOB_LABEL)

            # Check for existing client_info and add cortex user_agent header
            if not client_info:
                client_info = ClientInfo()

            client_info.user_agent = constants.CORTEX_USER_AGENT

        # Pass updated labels and user_agent to parent client class
        super().__init__(
            project=project,
            credentials=credentials,
            client_options=client_options,
            _http=_http,
            location=location,
            default_query_job_config=default_query_job_config,
            default_load_job_config=default_load_job_config,
            client_info=client_info,
        )

    def copy_table(
        self,
        sources: Union[
            bigquery.Table,
            bigquery.TableReference,
            table.TableListItem,
            str,
            Sequence[Union[bigquery.Table, bigquery.TableReference,
                           table.TableListItem, str]],
        ],
        destination: Union[bigquery.Table, bigquery.TableReference,
                           table.TableListItem, str],
        job_id: Optional[str] = None,
        job_id_prefix: Optional[str] = None,
        location: Optional[str] = None,
        project: Optional[str] = None,
        job_config: Optional[bigquery.CopyJobConfig] = None,
        retry: retries.Retry = DEFAULT_RETRY,
        timeout: TimeoutType = DEFAULT_TIMEOUT,
    ) -> job.CopyJob:

        """
        Wrapper method for google.cloud.bigquery.Client.copy_table
        that adds cortex_runtime label and user-agent header.

        Accepts same arguments as parent class method and
        returns google.cloud.bigquery.job.CopyJob object.
        """

        # Initialize job_config if it doesn't exist
        # and assign cortex_runtime label

        if self.allow_telemetry:
            if not job_config:
                job_config = bigquery.CopyJobConfig()

            job_config.labels.update(constants.CORTEX_JOB_LABEL)

        return super().copy_table(
            sources,
            destination,
            job_id,
            job_id_prefix,
            location,
            project,
            job_config,
            retry,
            timeout,
        )

    def extract_table(
        self,
        source: Union[bigquery.Table, bigquery.TableReference,
                      table.TableListItem, str],
        destination_uris: Union[str, Sequence[str]],
        job_id: Optional[str] = None,
        job_id_prefix: Optional[str] = None,
        location: Optional[str] = None,
        project: Optional[str] = None,
        job_config: Optional[bigquery.ExtractJobConfig] = None,
        retry: retries.Retry = DEFAULT_RETRY,
        timeout: TimeoutType = DEFAULT_TIMEOUT,
        source_type: str = "Table",
    ) -> job.ExtractJob:

        """
        Wrapper method for google.cloud.bigquery.Client.extract_table
        that adds cortex_runtime label and user-agent header.

        Accepts same arguments as parent class method and
        returns google.cloud.bigquery.job.ExtractJob object.
        """

        # Initialize job_config if it doesn't exist
        # and assign cortex_runtime label

        if self.allow_telemetry:
            if not job_config:
                job_config = bigquery.ExtractJobConfig()

            job_config.labels.update(constants.CORTEX_JOB_LABEL)

        return super().extract_table(
            source,
            destination_uris,
            job_id,  # type: ignore
            job_id_prefix,  # type: ignore
            location,  # type: ignore
            project,  # type: ignore
            job_config,
            retry,
            timeout,
            source_type,
        )

