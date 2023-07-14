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
"""
Generates DAG and related files needed execute
ML models for mapping Google Ads groups
to SAP product hierarchy.
"""

import argparse
import logging
from pathlib import Path
import shutil
import subprocess
import sys
import uuid
import tempfile
import typing

from google.cloud import bigquery

from jinja2 import Environment, FileSystemLoader

from py_libs.configs import load_config_file, load_setting_file
from py_libs.logging import initialize_console_logging

from ads_static_audiences_loader import load_ads_audiences
from catgap_pipeline.shared.sheets import (get_mapping_sheet,
                                           prepare_mapping_spreadsheet,
                                           MAPPING_SHEET_NAME, get_auth)
# from catgap_pipeline.shared.model_utils import pre_cache_models

_THIS_DIR = Path(__file__).resolve().parent
_CONFIG_FILE_NAME = "config.json"
_SETTING_FILE_NAME = "settings.yaml"

CONTAINER_REPO_NAME = "cortex"
BEAM_IMAGE_NAME = "cortex-catgap-beam"

# Directory under which all the generated dag files and related files
# will be created.
_GENERATED_DAG_DIR = "generated_dag"
# Directory that has all the dependencies for python dag code
_DEPENDENCIES_INPUT_DIR = Path(_THIS_DIR, "catgap_pipeline")
_DEPENDENCIES_OUTPUT_DIR = Path(_GENERATED_DAG_DIR, "catgap_pipeline")

_DAG_FILE_NAME = "catgap_dag.py"
_RUN_SCRIPT_FILE_NAME = "run_pipeline.sh"
_MAPPING_TABLE_NAME = "ads_hier_mapping_sheet"


def dag_gen_main(args: typing.Sequence[str]) -> int:
    parser = argparse.ArgumentParser(description="Cortex CATGAP DAG Generator.")

    parser.add_argument(
        "--debug",
        help="Debugging mode.",
        action="store_true",
        default=False,
        required=False,
    )
    parser.add_argument(
        "--skip-container",
        help="Skip building Apache Beam SDK container.",
        action="store_true",
        default=False,
        required=False,
    )
    parser.add_argument(
        "--skip-static-audiences",
        help="Skip loading Ads static audiences tables.",
        action="store_true",
        default=False,
        required=False,
    )
    parser.add_argument(
        "--beam-sdk-version",
        help="Apache Beam SDK version.",
        type=str,
        required=True,
    )

    options, _ = parser.parse_known_args(args)
    initialize_console_logging(False, options.debug)
    if options.debug:
        logging.debug("Running in debugging mode.")

    config_paths = [
        # config subfolder of the generator's folder
        Path(_THIS_DIR).joinpath("config", _CONFIG_FILE_NAME),

        # config subfolder of the current folder
        Path.cwd().joinpath("config", _CONFIG_FILE_NAME),
    ]
    beam_sdk_version = options.beam_sdk_version

    config = None
    for cfg_path in config_paths:
        if cfg_path.is_file():
            config = load_config_file(str(cfg_path))
    if not config:
        raise FileNotFoundError(f"Cannot locate {_CONFIG_FILE_NAME}")

    project_id = config["projectIdSource"]
    target_project_id = config["projectIdTarget"]
    location = config["location"]

    sap_config = config["SAP"]
    k9_config = config["k9"]
    cdc_dataset = sap_config["datasets"]["cdc"]
    k9_processing_dataset = k9_config["datasets"]["processing"]
    sap_reporting_dataset = sap_config["datasets"]["reporting"]
    k9_reporting_dataset = k9_config["datasets"]["reporting"]
    mandt = sap_config["mandt"]

    # settings.yaml must be in config subfolder of the generator's folder
    setting_path = Path(_THIS_DIR).joinpath("config", _SETTING_FILE_NAME)
    setting = load_setting_file(setting_path, "catgap")

    user_email = str(setting.get("user_email", "")).strip()
    if user_email == "":
        user_email = None

    dts_dataset = setting["ads"]["dts_dataset"]
    customer_id = str(setting["ads"]["customer_id"])
    dataflow_use_container = str(setting["dataflow"].get(
        "use_container", "false")).lower()
    dataflow_bucket = setting["dataflow"]["gcs_bucket"]
    dataflow_region = setting["dataflow"]["region"]
    dataflow_sa = setting["dataflow"]["service_account"]
    max_hier_level = setting["sap"]["max_prod_hierarchy_level"]

    deployment_id = uuid.uuid4().hex
    customer_and_mandt = f"{customer_id}-{mandt}"

    logging.info("Creating or getting Ads-to-SAP mapping spreadsheet.")
    mapping_url = get_mapping_sheet(project_id, customer_and_mandt, dataflow_sa)
    logging.info("Ads-to-SAP mapping spreadsheet has been created: %s",
                 mapping_url)

    bq_client = bigquery.Client(project=project_id,
                                location=location,
                                credentials=get_auth(project_id, dataflow_sa))

    logging.info("Creating Ads-to-SAP mapping external table in BigQuery.")
    dataset = bq_client.get_dataset(k9_processing_dataset)
    table_id = _MAPPING_TABLE_NAME
    bq_client.delete_table(bigquery.TableReference(dataset.reference, table_id),
                           not_found_ok=True)
    schema = [
        bigquery.SchemaField("campaign_name", "STRING"),
        bigquery.SchemaField("ad_group_name", "STRING"),
        bigquery.SchemaField("add_remove", "STRING"),
        bigquery.SchemaField("sap_hier", "STRING")
    ]
    table = bigquery.Table(dataset.table(table_id), schema=schema)
    external_config = bigquery.ExternalConfig("GOOGLE_SHEETS")
    external_config.source_uris = [mapping_url]
    external_config.options.skip_leading_rows = 1
    external_config.options.range = f"{MAPPING_SHEET_NAME}!A1:D"
    external_config.ignore_unknown_values = True
    table.external_data_configuration = external_config
    table = bq_client.create_table(table)
    logging.info("Creating Ads-to-SAP mapping table has been created.")

    if options.skip_static_audiences is False:
        logging.info("Loading Ads static audience tables.")
        load_ads_audiences(bq_client, project_id, k9_processing_dataset)
        logging.info("Ads static audience tables have been loaded.")

    python_version = f"{sys.version_info.major}.{sys.version_info.minor}"
    jinja_replacements = {
        "project_id_src":
            project_id,
        "project_id_tgt":
            target_project_id,
        "location":
            location,
        "dataset_cdc_processed":
            cdc_dataset,
        "dataset_reporting_tgt":
            sap_reporting_dataset,
        "dataset_k9_processing":
            k9_processing_dataset,
        "dataset_k9_reporting":
            k9_reporting_dataset,
        "dataset_ads":
            dts_dataset,
        "ads_account":
            customer_id,
        "mandt":
            mandt,
        "max_stufe":
            max_hier_level,
        "dataflow_use_container":
            dataflow_use_container,
        "dataflow_region":
            dataflow_region,
        "dataflow_bucket":
            dataflow_bucket,
        "dataflow_sa":
            dataflow_sa,
        "mapping_spreadsheet":
            mapping_url,
        "container_repo_name":
            CONTAINER_REPO_NAME,
        "beam_image_name":
            f"{BEAM_IMAGE_NAME}-sdk-{beam_sdk_version}-py-{python_version}",
        "beam_sdk_version":
            beam_sdk_version,
        "python_version":
            python_version,
        "deployment_id":
            deployment_id
    }

    logging.info("Creating Ads-to-SAP mapping views and tables.")
    env_sql = Environment(loader=FileSystemLoader(
        _THIS_DIR.joinpath("templates").joinpath("sql")))
    for template_name in [
            "ads_targeting_criterions.sql", "sap_prod_hierarchy.sql",
            "ads_to_sap_prod_hier.sql", "ads_to_sap_prod_hier_log.sql",
            "ads_to_sap_modified_mappings.sql",
            "reporting/sap_DailySalesByHierAndCountry.sql",
            "reporting/ads_AdsDailyStatsByHierAndCountry.sql",
            "reporting/ads_WeeklySAPSalesAndAdsStatsByHierAndCountry.sql"
    ]:
        sql = env_sql.get_template(template_name).render(jinja_replacements)
        bq_client.query(sql).result()
    logging.info("Ads-to-SAP mapping views and tables have been created.")

    logging.info("Formatting Ads-to-SAP mapping spreadsheet.")
    prepare_mapping_spreadsheet(mapping_url, project_id, k9_processing_dataset,
                                location, dataflow_sa, user_email)
    logging.info("Ads-to-SAP mapping spreadsheet has been formatted.")

    logging.info("Generating CATGAP DAG.")
    Path(_GENERATED_DAG_DIR).mkdir(exist_ok=True)
    env = Environment(loader=FileSystemLoader(_THIS_DIR.joinpath("templates")))
    dag_code = env.get_template(_DAG_FILE_NAME).render(jinja_replacements)
    with open(Path(_GENERATED_DAG_DIR, _DAG_FILE_NAME),
              mode="w",
              encoding="utf-8") as generated_file:
        generated_file.write(dag_code)
    dag_code = env.get_template(_RUN_SCRIPT_FILE_NAME).render(
        jinja_replacements)
    with open(Path(_GENERATED_DAG_DIR, _RUN_SCRIPT_FILE_NAME),
              mode="w",
              encoding="utf-8") as generated_file:
        generated_file.write(dag_code)
    logging.info("CATGAP DAG has been generated.")

    # Copy Dependencies for the DAG Python files.
    logging.info("Copying CATGAP DAG dependencies.")
    shutil.copytree(src=_DEPENDENCIES_INPUT_DIR,
                    dst=_DEPENDENCIES_OUTPUT_DIR,
                    ignore=shutil.ignore_patterns("__pycache__", "Dockerfile"),
                    dirs_exist_ok=True)

    if options.skip_container is False and dataflow_use_container == "true":
        logging.info("Building Dataflow container.")
        with tempfile.TemporaryDirectory() as tmp_dir:
            env_container = Environment(loader=FileSystemLoader(
                _THIS_DIR.joinpath("templates").joinpath("container")))
            for template_name in ["build_container.sh", "Dockerfile"]:
                code = env_container.get_template(template_name).render(
                    jinja_replacements)
                with open(Path(tmp_dir, template_name),
                          mode="w",
                          encoding="utf-8") as generated_file:
                    generated_file.write(code)
            shutil.copy(_DEPENDENCIES_INPUT_DIR.joinpath("requirements.txt"),
                        f"{tmp_dir}/requirements.txt")
            shutil.copy(_THIS_DIR.joinpath("requirements.txt"),
                        f"{tmp_dir}/requirements-beam.txt")
            cmd = ["bash", f"{tmp_dir}/build_container.sh"]
            subprocess.run(cmd, shell=False, check=True)
        logging.info("Dataflow container has been built.")

    logging.info("CATGAP has been deployed (%s).",
                 Path(_GENERATED_DAG_DIR).resolve())
    logging.info(("Use this spreadsheet to review and adjust mappings "
                  "between Ad Groups and SAP product hierarchy: %s"),
                 mapping_url)

    return 0


def main() -> int:
    """main function"""
    return dag_gen_main(sys.argv)


#################################################################
if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code if exit_code else 0)
