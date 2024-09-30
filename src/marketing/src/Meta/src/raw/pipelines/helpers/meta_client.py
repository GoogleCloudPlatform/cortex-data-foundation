# Copyright 2024 Google LLC

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Meta API client implementation."""

from datetime import date
from datetime import timedelta
import logging
from time import sleep
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from requests.exceptions import RequestException

_OBJECTS_PER_PAGE = 100
# As insights are requested with one-day interval,
# date_stop column can be taken as insights date.
_DATE_STOP_COLUMN = "date_stop"


class MetaClient():
    """Meta API Client.

    Args:
        access_token(str): an access token for Meta API calls.
        http_timeout(int): raises a Timeout error if a response have not been
            received after a given amount of seconds.
        api_version(str): version of the Meta API.
    """

    _REQUEST_LIMIT_ERROR_CODES = [17, 80000]

    def __init__(self, access_token: str, http_timeout: int,
                 next_request_delay_sec: float, api_version: str):
        self._access_token = access_token
        self._http_timeout = http_timeout
        self._next_request_delay_sec = next_request_delay_sec
        self.base_url = f"https://graph.facebook.com/{api_version}"

    def _request(
            self,
            url: str,
            params: Optional[Dict[str, str]] = None) -> List[Dict[str, Any]]:
        """Makes HTTP request to Meta API.

        Args:
            url(str): Meta API endpoint.
            params(Dict[str, str]): request parameters.

        Returns:
            List[Dict[str, Any]]: Parsed json response or
            str: LIMIT_REACHED if requests limit has been reached.
        """

        headers = {"Cache-Control": "no-store"}

        resp = requests.get(url,
                            params,
                            headers=headers,
                            timeout=self._http_timeout)

        logging.debug("%s: \n%s", url, resp.text)

        if resp.status_code == 200:
            return resp.json()
        else:
            logging.error(
                "Following url responded with error: %s\n" + "Headers: %r\n" +
                "Content: %r", url, resp.headers, resp.content)

            error_data = resp.json()
            if (resp.status_code == 400 and error_data["error"]["code"]
                    in self._REQUEST_LIMIT_ERROR_CODES):
                logging.error("Request limit has been reached. " +
                              "Data load has been aborted. " +
                              "Persisting data to the BQ.")
                raise RequestLimitReachedError()
            else:
                raise RequestException(resp.status_code, resp.text)

    def request_dim_data(self, account_id: str, endpoint: str,
                         fields: Iterable[str]):
        """Fetches data for all objects at the specified endpoint
        under a specified ad account id.

        Data for all child objects are requested and then emitted.

        Args:
            account_id(str): Parent account id, or "me" to request all
                accounts visible to the current user.
            endpoint(str): API endpoint that returns object data.
            fields(Iterable[str]): Requested object fields.

        Exceptions:
            RequestException: Response contains unexpected error.
            RequestLimitReachedError: Requests limit has been reached.
            Timeout: Response have not been received
                after a given amount of seconds.

        Yields:
            Dict containing fields of an ad object.
        """

        url = f"{self.base_url}/{account_id}/{endpoint}"
        params = {
            "access_token": self._access_token,
            "fields": ",".join(fields),
            "limit": _OBJECTS_PER_PAGE
        }

        # Looking for all possible status.
        if endpoint in ("ads", "adsets", "campaigns"):
            params["filtering"] = """[{'field':'effective_status',
                                     'operator':'IN', 'value':['ACTIVE',
                                     'PAUSED', 'DELETED', 'PENDING_REVIEW',
                                     'DISAPPROVED', 'PREAPPROVED',
                                     'PENDING_BILLING_INFO', 'CAMPAIGN_PAUSED',
                                     'ARCHIVED', 'ADSET_PAUSED']}]"""

        try:
            # Sending http request.
            response = self._request(url, params)
        except RequestLimitReachedError:
            # Abort requesting data if request limit is reached.
            return

        # Accumulating data in one batch and emit once data is loaded fully.
        batch = []
        while True:
            batch += response["data"]
            # Emitting result if no data left.
            if not "paging" in response or not "next" in response["paging"]:
                for ad_object in batch:
                    yield ad_object
                return
            # Requesting next page with delay.
            sleep(self._next_request_delay_sec)
            try:
                # Sending http request.
                next_url = response["paging"]["next"]
                response = self._request(next_url)
            except RequestLimitReachedError:
                # Abort requesting data if request limit is reached.
                # Last batch is dropped as it could be loaded partially.
                return

    def request_fact_data(self, id_and_load_range: Tuple[str, date, date],
                          fields: Iterable[str], breakdowns: Optional[str],
                          action_breakdowns: Optional[str],
                          days_per_request: int):
        """ Gets insights for the specified object id and date range.
        It divides the whole date range into smaller ranges,
        requests them consecutively and emits objects once
        data for each day are fully extracted.

        Args:
            id_and_load_range(Tuple[str, date, date]): A tuple
            containing an ad object id, start and finish dates to request.
            fields(Iterable[str]): Required object fields.
            breakdowns(Optional[str]): Insight breakdown dimensions,
                                       separated by comma.
            action_breakdowns(Optional[str]): Insight action breakdown
                                              dimensions, separated by comma.
            days_per_request: a number of days requested in each request.

        Exceptions:
            RequestException: response contains unexpected error.
            Timeout: response have not been received
                after a given amount of seconds.

        Yields:
            A dict containing fields of an ad object insights.
        """

        object_id, start_date, last_date = id_and_load_range

        url = f"{self.base_url}/{object_id}/insights"
        params = {
            "access_token": self._access_token,
            "fields": ",".join(fields),
            "sort": _DATE_STOP_COLUMN,
            "limit": _OBJECTS_PER_PAGE
        }
        # Grouping results using breakdowns.
        if breakdowns:
            params["breakdowns"] = breakdowns
        # Grouping results using action_breakdowns.
        if action_breakdowns:
            params["action_breakdowns"] = action_breakdowns

        # Extracting data over specified date range by days.
        while start_date <= last_date:
            # Generating next date range.
            # Subtracting one day, since in Meta the days are counted inclusive.
            end_date = min(start_date + timedelta(days=days_per_request - 1),
                           last_date)
            # Setting up API parameters.
            time_range = str({
                "since": start_date.isoformat(),
                "until": end_date.isoformat()
            })
            params["time_range"] = time_range
            params["time_increment"] = 1

            try:
                # Sending http request.
                response = self._request(url, params)
            except RequestLimitReachedError:
                # Abort requesting data if request limit is reached.
                return

            # Accumulating data within date in one batch and emit.
            batch = []
            batch_date = None
            while True:
                # Iterating over page.
                for ad_object in response["data"]:
                    # Processing date based on date_stop column.
                    object_date = ad_object[_DATE_STOP_COLUMN]
                    if batch_date and batch_date != object_date:
                        # We now have all dates in the current batch.
                        # Emit, then clear batch for next day.
                        for batch_object in batch:
                            yield batch_object
                        batch.clear()
                    # Add current record to batch and set batch date.
                    batch.append(ad_object)
                    batch_date = object_date
                # Emit result if no pages left.
                if not "paging" in response or not "next" in response["paging"]:
                    for ad_object in batch:
                        yield ad_object
                    break
                # Requesting next page with delay.
                sleep(self._next_request_delay_sec)
                try:
                    # Sending http request.
                    next_url = response["paging"]["next"]
                    response = self._request(next_url)
                except RequestLimitReachedError:
                    # Abort requesting data if request limit is reached.
                    # Last batch is dropped as it could be loaded partially.
                    return
            # Exiting loop once iterated over whole date range.
            if end_date >= last_date:
                break

            start_date = end_date + timedelta(days=1)
            # Requesting next date range with delay.
            sleep(self._next_request_delay_sec)


class RequestLimitReachedError(Exception):
    pass
