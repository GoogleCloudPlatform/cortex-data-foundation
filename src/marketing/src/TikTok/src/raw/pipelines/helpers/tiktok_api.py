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
""" TikTok API client implementation."""

from copy import deepcopy
import json
import logging
from typing import Any, Dict, List, Union
from urllib.parse import urlencode
from urllib.parse import urlunsplit

import requests


class TikTokAuthenticationError(SystemError):
    pass


class TikTokClient():
    """TikTok API Client."""
    API_VERSION = "v1.3"

    def __init__(self, access_token, app_id, secret_key):
        self.access_token = access_token
        self.app_id = app_id
        self.secret_key = secret_key

    def _build_url(self,
                   path: str = "",
                   query: Union[Dict[str, Any], None] = None) -> str:

        scheme = "https"
        netloc = "business-api.tiktok.com/open_api"

        if query:
            encoded_query = urlencode(query)
        else:
            encoded_query = ""

        return urlunsplit((scheme, netloc, path, encoded_query, ""))

    def _response_code_validation(self, response: requests.Response):
        """Logging about most common issues."""

        status_code = response.status_code
        if status_code in (401, 403):
            logging.error("Authentication error (%s)", status_code)
            raise TikTokAuthenticationError()
        if status_code > 299:
            logging.error(
                "Issue with TikTok connection (%s): %s",
                status_code,
                response,
            )
            raise ConnectionError(
                f"Issue with TikTok connection ({status_code}): {response}")

    def _match_advertisers_to_advertiser_info(
            self, advertisers: List[Dict[str, str]],
            advertisers_info: List[Dict[str, Any]]):
        """Matches advertisers with extra information like timezone.

        Fields are listed explicitly to limit merged data only to timezone info.

        Args:
            advertisers: advertiser_id and advertiser_name pairs.
            advertisers_info: advertiser_id and extra information.

        Yields:
            The matching advertisers with timezone information.
        """
        for advertiser in advertisers:
            for advertiser_info in advertisers_info:
                id_, name = advertiser["advertiser_id"], advertiser[
                    "advertiser_name"]

                info_id, timezone, display_timezone = advertiser_info[
                    "advertiser_id"], advertiser_info[
                        "timezone"], advertiser_info["display_timezone"]

                if id_ == info_id:
                    # Building result row from the two response.
                    yield {
                        "advertiser_id": id_,
                        "advertiser_name": name,
                        "timezone": timezone,
                        "display_timezone": display_timezone
                    }

    def _query_tiktok_api(self,
                          url: str,
                          headers: Dict[str, Any],
                          rest_client: Any = requests) -> List[Dict[str, Any]]:
        """Gets simple TikTok API data without pagination.

        Args:
            url (str): Target API url.
            headers (Dict[str, Any]): Request headers.
            rest_client (Any, optional): Client for REST calls.
                Defaults to requests.

        Raises:
            KeyError: In case of response does not contain required fields.

        Returns:
            List[Dict[str, Any]]: Result data as a list of dictionaries.
        """
        response = rest_client.get(url, headers=headers)

        self._response_code_validation(response=response)
        rsp_dict = response.json()

        try:
            result_data = rsp_dict["data"]["list"]
        except KeyError as e:
            logging.error("Error fetching data from Tiktok API.")
            logging.error(str(rsp_dict))
            raise KeyError("Missing data from response!") from e

        if not result_data:
            logging.warning("There is no any authorized advertiser!")

        return result_data

    def get_advertisers(self,
                        rest_client: Any = requests) -> List[Dict[str, str]]:
        """Gets all advertisers for the corresponding account.

        Ref: https://ads.tiktok.com/marketing_api/docs?id=1738455508553729

        Args:
            rest_client (Any, optional): Client for REST calls.
                Defaults to requests.

        Returns:
            List[Dict[str, str]]: Advertiser ID
                and name pairs as a list of dictionaries.
        """
        headers = {
            "Access-Token": self.access_token,
        }

        logging.info("Getting advertisers...")
        path = f"/{self.API_VERSION}/oauth2/advertiser/get/"
        query = {"app_id": self.app_id, "secret": self.secret_key}
        url = self._build_url(path=path, query=query)
        return self._query_tiktok_api(url=url,
                                      headers=headers,
                                      rest_client=rest_client)

    def get_advertisers_info(
            self,
            advertisers: List[Dict[str, str]],
            rest_client: Any = requests) -> List[Dict[str, Any]]:
        """Gets advertiser info for given advertisers.

        Ref: https://ads.tiktok.com/marketing_api/docs?id=1739593083610113

        Args:
            advertisers (List[Dict[str, str]]): Advertiser ID
                and name pairs as a list of dictionaries.
            rest_client (Any, optional): Client for REST calls.
                Defaults to requests.

        Returns:
            List[Dict[str, Any]]: List of dictionaries with advertiser ID, name
                and timezone information.
        """
        headers = {
            "Access-Token": self.access_token,
        }

        logging.info("Getting advertisers info...")
        advertiser_ids = [str(a["advertiser_id"]) for a in advertisers]
        path = f"/{self.API_VERSION}/advertiser/info/"
        query = {
            "app_id": self.app_id,
            "secret": self.secret_key,
            "advertiser_ids": json.dumps(advertiser_ids)
        }
        url = self._build_url(path=path, query=query)
        advertisers_info = self._query_tiktok_api(url=url,
                                                  headers=headers,
                                                  rest_client=rest_client)

        advertisers_with_info = self._match_advertisers_to_advertiser_info(
            advertisers=advertisers, advertisers_info=advertisers_info)
        return list(advertisers_with_info)

    def _generate_pages(self,
                        response_payload: dict,
                        page_info: dict,
                        base_query: dict,
                        headers: dict,
                        path: str,
                        rest_client: Any = requests):
        """Processing response pages.

        Args:
            response_payload (dict): API response.
            page_info (dict): API response page information.
            base_query (dict): query for the request.
            headers (dict): header for the request.
            path (str): API endpoint path.
            rest_client (Any, optional): REST client.

        Raises:
            KeyError: In case of response does not contain required fields.

        Yields:
            item(dict): one data item from the given page.
        """

        query = deepcopy(base_query)
        if page_info["page"] == 1 and page_info["total_number"] == 1:
            try:
                data = response_payload["data"]["list"]
            except KeyError as e:
                logging.error("Error fetching data from Tiktok API.")
                logging.error(str(response_payload))
                raise KeyError("Missing data from response!") from e

            for item in data:
                yield item
        else:
            for p_num in range(1, page_info["total_number"] + 1):
                query["page"] = p_num
                url = self._build_url(path=path, query=query)

                response = rest_client.get(url, headers=headers)

                self._response_code_validation(response=response)

                response_payload = response.json()

                try:
                    data = response_payload["data"]["list"]
                except KeyError as e:
                    logging.error("Error fetching data from Tiktok API.")
                    logging.error(str(response_payload))
                    raise KeyError("Missing data from response!") from e

                for item in data:
                    yield item

    def get_report(self,
                   advertiser_data: dict,
                   query: dict,
                   rest_client: Any = requests):
        """Getting TikTok reporting data.

        Args:
            advertiser_data (dict): Advertiser details.
            query (dict): Query details.
            rest_client (Any, optional): REST client.

        Raises:
            KeyError: If page info unavailable.

        Yields:
            item (dict): Reporting data with extended advertiser details.
        """

        headers = {
            "Access-Token": self.access_token,
        }
        path = f"/{self.API_VERSION}/report/integrated/get/"
        base_query = {
            "advertiser_id": advertiser_data["advertiser_id"],
        }

        full_query = {**query, **base_query}
        url = self._build_url(path=path, query=full_query)

        response: requests.Response = rest_client.get(url, headers=headers)

        self._response_code_validation(response=response)
        rsp_dict = response.json()

        try:
            page_info = rsp_dict["data"]["page_info"]
        except KeyError as e:
            logging.error("Error fetching data from Tiktok API.")
            logging.error(str(rsp_dict))
            raise KeyError("Missing data from response!") from e

        for item in self._generate_pages(response_payload=rsp_dict,
                                         page_info=page_info,
                                         base_query=full_query,
                                         path=path,
                                         headers=headers):
            # Extend items with advertiser data.
            item["dimensions"] = {**advertiser_data, **item["dimensions"]}

            yield item
