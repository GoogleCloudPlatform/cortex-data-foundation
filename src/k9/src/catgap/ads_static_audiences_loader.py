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
#
"""Ads common audiences loader.
Creates BigQuery tables from static Ads data on verticals, products and targets.
"""
from io import BytesIO
import logging
from urllib import parse
from pathlib import Path
import requests
import tempfile
import zipfile

from google.cloud import bigquery

_ads_audiences = [
    {
        "table":
            "ads_geo_targets",
        "url": ("https://developers.google.com/google-ads/"
                "api/latest_geotargets")
    },
    {
        "table":
            "ads_products_services",
        "url": ("https://developers.google.com/static/google-ads/"
                "api/data/tables/productsservices.csv")
    },
    {
        "table":
            "ads_verticals",
        "url": ("https://developers.google.com/static/google-ads/"
                "api/data/tables/verticals.csv")
    },
    {
        "table":
            "ads_affinity_categories",
        "url": ("https://developers.google.com/static/google-ads/"
                "api/data/tables/affinity-categories.tsv")
    },
    {
        "table":
            "ads_in_market_categories",
        "url": ("https://developers.google.com/static/google-ads/"
                "api/data/tables/in-market-categories.tsv")
    },
]


def load_ads_audiences(client: bigquery.Client, project_id: str,
                       dataset_name: str):
    """Loads static Ads audiences to BigQuery

    Args:
        client (bigquery.Client): BigQuery client
        project_id (str): target project id
        dataset_name (str): target dataset name
    """

    for item in _ads_audiences:
        csv_url = item["url"]
        parsed = parse.urlparse(csv_url)
        path = Path(parsed.path)
        ext = path.suffix.lower()
        table_name = item["table"]

        table = bigquery.TableReference(
            bigquery.DatasetReference(project_id, dataset_name), table_name)

        logging.info("Loading %s to %s.", csv_url, table)

        job_config = bigquery.LoadJobConfig(
            autodetect=True,
            source_format=bigquery.SourceFormat.CSV,
            field_delimiter="\t" if ext == ".tsv" else ",",
            null_marker="",
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )

        with tempfile.NamedTemporaryFile(mode="w",
                                         encoding="utf-8") as csv_file:
            with requests.get(csv_url, allow_redirects=True) as response:
                csv_text = ""
                # If file downloads as .zip,
                # extract the first .csv/.tsv file from it.
                # We check url from the response because redirects
                # may define the real url
                # (which is the case for geotargets already)
                if response.url.endswith(".zip"):
                    with zipfile.ZipFile(BytesIO(response.content),
                                         "r") as zip_file:
                        for name in zip_file.namelist():
                            if name.endswith(".csv") or name.endswith(".tsv"):
                                csv_bytes = zip_file.read(name)
                                csv_text = csv_bytes.decode("utf-8")
                                break
                else:
                    csv_text = response.text
                csv_file.write(csv_text)
                csv_file.flush()

            with open(csv_file.name, "rb") as file:
                job = client.load_table_from_file(
                    file,
                    table,
                    job_config=job_config,
                    project=table.project,
                )
                job.result()
            logging.info("%s has been loaded to %s.", csv_url, table)
