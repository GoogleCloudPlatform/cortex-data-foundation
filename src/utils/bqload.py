# Copyright 2022 Google LLC

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""BigQuery Parquet Loader"""

import argparse
import logging
import sys
import threading
import typing
from concurrent.futures import ThreadPoolExecutor, wait
from pathlib import Path
from urllib.parse import urlparse, unquote
from google.cloud import bigquery
from google.api_core import retry
from google.cloud.exceptions import Conflict


def run_load_job(
    uri: str,
    table: str,
    location: str,
    write_disposition: bigquery.WriteDisposition,
    clients: typing.Dict[int, bigquery.Client],
    stop_on_failure: bool,
) -> None:
    """Executes a BigQuery Parquet Load Job.

    Args:
        uri (str): source parquet file URI
        table (str): destination BigQuery table
        location (str): BigQuery load job location
        write_disposition (bigquery.WriteDisposition): load job write disposition
        clients (typing.Dict): shared BigQuery client dictionary
        stop_on_failure (bool): throw SystemExit on failure if True

    Raises:
        SystemExit: thrown if there is a failure and stop_on_failure is True
    """
    try:
        ident = threading.current_thread().ident
        if ident not in clients:
            clients[ident] = bigquery.Client()
        bq_client = clients[ident]

        job_config = bigquery.LoadJobConfig(
            autodetect=True,
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=write_disposition,
        )

        logging.info("Loading to %s.", table)
        load_job = bq_client.load_table_from_uri(
            source_uris=uri,
            destination=table,
            job_config=job_config,
            location=location,
            retry=retry.Retry(deadline=60),
        )
        # Waiting for the jon to finish.
        load_job.result()

        logging.info("✅ Table %s has been loaded.", table)
    except Conflict:
        logging.warning("⚠️ Table %s already exists. Skipping it.", table)
    except Exception as ex:
        logging.error(
            "⛔️ Failed to load table %s.\n",
            table,
            exc_info=True,
        )
        if stop_on_failure:
            raise SystemExit() from ex
        raise


def main(args: typing.Sequence[str]):
    """BigQuery load main function.

    Args:
        args (typing.Sequence[str]): command line arguments
    """
    parser = argparse.ArgumentParser(description="BigQuery Parquet Loader")
    parser.add_argument(
        "--debug",
        help="Debugging mode.",
        action="store_true",
        default=False,
        required=False,
    )
    parser.add_argument(
        "--source-list-file",
        help="Parquet URIs list file.",
        type=str,
        required=True,
    )
    parser.add_argument(
        "--dataset",
        help="BigQuery destination dataset id (project.dataset).",
        type=str,
        required=True,
    )
    parser.add_argument(
        "--location",
        help="BigQuery location.",
        type=str,
        required=True,
    )
    parser.add_argument(
        "--write-disposition",
        help="When to do with existing tables.",
        choices=["skip", "append", "truncate"],
        type=str,
        default="skip",
        required=False,
    )
    parser.add_argument(
        "--parallel-jobs",
        help="Number of parallel jobs.",
        type=int,
        default=10,
        required=False,
    )
    parser.add_argument(
        "--on-failure",
        help="What to do when a load job fails.",
        choices=["stop", "continue"],
        type=str,
        default="stop",
        required=False,
    )
    options, _ = parser.parse_known_args(args)

    logging.basicConfig(
        format="%(asctime)s | %(levelname)s | %(message)s",
        level=logging.DEBUG if options.debug else logging.INFO,
    )

    if options.write_disposition == "append":
        write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    elif options.write_disposition == "truncate":
        write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    else:
        write_disposition = bigquery.WriteDisposition.WRITE_EMPTY

    with open(options.source_list_file, "r", encoding="utf-8") as source_file:
        uris = source_file.readlines()

    threads = []
    clients = {}
    executor = ThreadPoolExecutor(int(options.parallel_jobs))

    for uri in uris:
        uri = uri.strip()
        if len(uri) == 0 or uri.endswith("/"):
            continue

        parsed_uri = urlparse(uri)
        filename = unquote(Path(parsed_uri.path).stem)
        table = f"{options.dataset}.{filename}"

        threads.append(
            executor.submit(
                run_load_job,
                uri,
                table,
                options.location,
                write_disposition,
                clients,
                options.on_failure,
            )
        )

    if len(threads) > 0:
        logging.info("Waiting for BigQuery loading jobs to finish...")
        wait(threads)

    logging.info("✅ Done.")


###############################################################
if __name__ == "__main__":
    main(sys.argv)
    sys.exit(0)
