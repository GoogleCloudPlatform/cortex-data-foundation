# pylint: disable=logging-fstring-interpolation consider-using-f-string
# pylint: disable=inconsistent-quotes

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
"""Creates hierarchy related DAG files and tables."""

import sys
import logging
import os
import pathlib
import datetime
import yaml

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from common.py_libs import configs
from common.py_libs import cortex_bq_client

import dag_hierarchies_module
import generate_query

_TARGET_BUCKET_FOLDER = "dags/sap/reporting/hier_reader"
_SETS_FILE = "sets.yaml"

def if_setnode_exists(client: bigquery.Client, source_dataset: str,
                      set_name: str, set_class: str, mandt: str,
                      org_unit: str) -> bigquery.job.QueryJob:
    """Check if the setnode exists

    Args:
        client: BigQuery client
        source_dataset: Dataset hosting setnode table
        set_name: Name of set
        set_class: Class of set
        mandt: Mandt column of setnode
        org_unit: Subclass of setnode

    Returns:
        QueryJob: Result of query execution
    """
    query = """SELECT  1
            FROM `{src_dataset}.setnode`
            WHERE setname = \'{setname}\'
            AND setclass = \'{setclass}\'
            AND subclass = \'{org_unit}\'
            AND mandt = \'{mandt}\'
            LIMIT 1 """.format(
        src_dataset=source_dataset,
        setname=set_name,
        mandt=mandt,
        setclass=set_class,
        org_unit=org_unit,
    )

    query_job = client.query(query)
    return query_job

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

bq_client = cortex_bq_client.CortexBQClient()

config = configs.load_config_file(sys.argv[1])

src_project = config["projectIdSource"]
src_dataset = src_project + "." + config["SAP"]["datasets"]["raw"]
tgt_project = config["projectIdTarget"]
tgt_dataset = tgt_project + "." + config["SAP"]["datasets"]["reporting"]
gcs_bucket = config["targetBucket"]

os.makedirs("generated_dag", exist_ok=True)

this_folder = pathlib.Path(__file__).parent.absolute()
sets_file = this_folder.joinpath(_SETS_FILE)

# Process hierarchies
with open(sets_file, encoding="utf-8") as f:
    datasets = yaml.load(f, Loader=yaml.SafeLoader)

    for dataset in datasets["sets_data"]:
        logging.info(f"== Processing dataset {dataset['setname']} ==")

        full_table = "{tgtd}.{tab}_hier".format(
            tgtd=tgt_dataset, tab=dataset["table"]
        )

        if not if_setnode_exists(bq_client, src_dataset, dataset["setname"],
                                 dataset["setclass"], dataset["mandt"],
                                 dataset["orgunit"]):
            logging.info(f"Dataset {dataset['setname']} not found in SETNODES")
            continue

        # Check if table exists, create if not and populate with full initial
        # load.
        try:
            generate_query.check_create_hiertable(full_table,
                                                  dataset["key_field"])

            logging.info(f"Generating dag for {full_table}")
            today = datetime.datetime.now()
            substitutes = {
                "setname": dataset["setname"],
                "full_table": full_table,
                "year": today.year,
                "month": today.month,
                "day": today.day,
                "src_project": src_project,
                "src_dataset": src_dataset,
                "setclass": dataset["setclass"],
                "orgunit": dataset["orgunit"],
                "mandt": dataset["mandt"],
                "table": dataset["table"],
                "select_key": dataset["key_field"],
                "where_clause": dataset["where_clause"],
                "load_frequency": dataset["load_frequency"],
            }

            dag_file_name = 'cdc_%s.py' % (full_table.replace(".", "_"))
            generate_query.generate_hier_dag_files(dag_file_name, **substitutes)
            dag_hierarchies_module.generate_hier(**substitutes)

            generate_query.copy_to_storage(
               gcs_bucket, _TARGET_BUCKET_FOLDER, "generated_dag", dag_file_name
            )

        except NotFound as e:
            # logging, but keep going
            logging.warning(f"Table {full_table} not found"
                            " or truncated - Skipping")

    # Copy the init file
    generate_query.copy_to_storage(
        gcs_bucket, _TARGET_BUCKET_FOLDER,
        "local_k9/hier_reader/template_dag", "__init__.py"
    )

    # Copy template python processor used by all into specific directory
    generate_query.copy_to_storage(
        gcs_bucket, _TARGET_BUCKET_FOLDER, this_folder,
        "dag_hierarchies_module.py"
    )
