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
#
"""This module handles authentication for Google Sheets."""

from google.oauth2 import credentials
from google.auth import default, transport, impersonated_credentials


def get_auth(project_id: str,
             service_account_principal: str = None) -> credentials.Credentials:
    """Generates authentication credentials,
    optionally impersonates a service account.

    Args:
        project_id (str): project id to use to generating credentials.
        service_account_principal (str, optional):
            service account principal to impersonate. Defaults to None.

    Returns:
        Credentials: generated credentials
    """
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/bigquery",
    ]
    request = transport.requests.Request()
    default_creds, _ = default(scopes=scopes, quota_project_id=project_id)
    if not default_creds.valid or default_creds.expired:
        default_creds.refresh(request)
    if service_account_principal:
        res_credentials = impersonated_credentials.Credentials(
            source_credentials=default_creds,
            target_principal=service_account_principal,
            quota_project_id=project_id,
            target_scopes=scopes,
        )
        if not res_credentials:
            res_credentials.refresh(request)
    else:
        res_credentials = default_creds

    return res_credentials