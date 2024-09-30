# Copyright 2023 Google LLC

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Utility functions for LiveRamp data extraction."""

import ast
from dataclasses import dataclass
from datetime import datetime
import json
from typing import Any, Dict, List, Tuple

from google.cloud import secretmanager
import requests
from requests.models import Response

# Configure how many seconds earlier invalidate the access token before the
# given value.
# It is required to avoid timeouts in the case when a token reaches its limit
# but the next call is happening in the next second.
_DELAY_BEFORE_EXPIRATION_SEC = 10


@dataclass
class AccessToken:
    """Represents access token with validation functionality.

    Every timestamp is in UTC and seconds.
    Token and expiration time are extracted from the authentication
    endpoint response.

    Attributes:
        token: Actual access token.
        expires_in_sec: Token expiration time in seconds.
        get_ts: Timestamp in UTC when the token was received.
    """
    token: str
    expires_in_sec: int
    get_ts: int

    def is_expired(self, utc_now_ts: int) -> bool:
        """Checks if the access token is expired or not.

        Calculates time since token was received and
        the given timestamp from the caller.

        Args:
            utc_now_ts (int): Given timestamp value in seconds and UTC timezone.

        Returns:
            bool: When the token older than expiration_time it is invalid.
        """
        time_since_token_get = utc_now_ts - self.get_ts
        expiration_time = self.expires_in_sec - _DELAY_BEFORE_EXPIRATION_SEC

        return time_since_token_get >= expiration_time


def _get_credentials_from_secret_manager(
        project_id: str,
        secret_name: str = "cortex-framework-liveramp") -> Dict[str, Any]:
    """Gets credentials from Google Secret Manager."""

    name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    secret_client = secretmanager.SecretManagerServiceClient()
    response = secret_client.access_secret_version(name=name)
    payload = response.payload.data.decode()
    credentials = ast.literal_eval(payload)
    return credentials


def _get_access_token(credentials: Dict[str, Any], url: str,
                      timeout: int) -> AccessToken:
    """Sends request to LiveRamp and gets token as response."""

    client_id = credentials["client_id"]
    client_secret = credentials["client_secret"]
    grant_type = credentials["grant_type"]

    data = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": grant_type
    }

    response: Response = requests.post(url=url, data=data, timeout=timeout)

    if response.status_code != 200:
        raise ConnectionError(
            f"LiveRamp authentication failed on {url}: {response.text}")

    response_json = response.json()

    token = response_json.get("access_token")
    expiration_time_sec = int(response_json.get("expires_in"))

    utc_now_ts = int(datetime.utcnow().timestamp())
    return AccessToken(token=token,
                       expires_in_sec=expiration_time_sec,
                       get_ts=utc_now_ts)


def get_token(project_id: str, url: str, timeout: int) -> AccessToken:
    """Gets a token from LiveRamp authentication endpoint."""

    credentials = _get_credentials_from_secret_manager(project_id=project_id)
    access_token = _get_access_token(credentials=credentials,
                                     url=url,
                                     timeout=timeout)
    return access_token


def validate_search_strings(batch: List) -> List[str]:
    """Validates search strings with endpoint routes."""
    hash_list = []
    for row in batch:
        search_string = row["search_string"]
        if not search_string:
            # TODO: Continue processing and skip only failing batches.
            raise KeyError(
                (f"Search string is not available for row: {row}. "
                 "Row should contain at least email or phone_number."))
        hash_list.append(search_string)

    return hash_list


def fetch_rampids_for_hashed_pii_details(access_token: AccessToken,
                                         hash_list: List, url: str,
                                         timeout: int) -> Response:
    """Resolves PII data into RampIDs.

    Fetches RampIDs for the given list of hashed PII strings from the LiveRamp
    Lookup API.

    Args:
        access_token (AccessToken): Auth token object for LookUp API.
        hash_list (List): Hashed strings to be resolved by LookUp API.
        timeout (int): Timeout of RampID resolution REST call.

    Raises:
        ValueError: When there are duplicates in the input table.
        ConnectionAbortedError: In case of other connection issues.

    Returns:
        Response: Resolved RampIDs.
    """
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token.token}",
    }

    payload = json.dumps(hash_list)

    response = requests.post(url=url,
                             headers=headers,
                             data=payload,
                             timeout=timeout)

    if response.status_code == 400:
        error_response = response.json()
        error_code = error_response.get("error").get("code")

        # Too few unique person records for the lookup endpoint in the request.
        if error_code == "600-0105":
            raise ValueError(
                ("There are significant amount of duplicated persons "
                 "in the input table! Please remove duplicates!"))

    if response.status_code != 200:
        raise ConnectionAbortedError(
            (f"LiveRamp API returned {response.status_code} "
             f"message: {response.text}"))

    return response


def extract_ramp_ids_from_response(segment_name: str,
                                   response: Response) -> List[Tuple[str, str]]:
    """Extracts LiveRamp IDs from LiveRamp LookUp API response and returns list
    of segment names and ramp_ids in tuples.
    """

    # Extract ramp_ids from response.
    id_list = [
        item["document"].get("person").get("anonymousAbilitec").get(
            "anonymousConsumerLink") for item in response.json()
    ]

    # Add ramp_id and segment_name to list.
    # Deduplicate the list of IDs.
    # TODO: Consider moving deduplication into the temporary input table.
    unique_ids = set(zip([segment_name] * len(id_list), id_list))

    return list(unique_ids)
