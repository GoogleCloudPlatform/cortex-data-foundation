# Copyright 2024 Google LLC
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
"""
K9 deployer.
"""

import argparse
import logging
from pathlib import Path
import sys
import typing
import json

from google.cloud import bigquery

from common.py_libs import bq_helper
from common.py_libs import configs
from common.py_libs import cortex_bq_client
from common.py_libs import k9_deployer

_DEFAULT_CONFIG = "config/config.json"  # data foundation config
_K9_MANIFEST = "manifest.yaml"

def _create_dataset(full_dataset_name: str, location: str,
                    bq_client: bigquery.Client,
                    label_dataset: bool = False) -> None:
    """Creates target (CDC/Reporting etc.) BQ target dataset if needed."""
    logging.info("Creating '%s' dataset if needed...", full_dataset_name)
    ds = bigquery.Dataset(full_dataset_name)
    ds.location = location
    ds = bq_client.create_dataset(ds, exists_ok=True, timeout=30)

    if label_dataset:
        bq_helper.label_dataset(bq_client=bq_client, dataset=ds)

# Loading k9 settings from config file
def _load_k9s_settings(config_file):
    logging.info("Loading K9 settings from the configuration file.")
    with open(config_file, "r", encoding="utf-8") as f:
        config = json.load(f)
        if "k9" not in config:
            logging.warning("No K9 configuration found.")
            return None
        return config["k9"]

def _should_skip_k9(k9_manifest: dict,
                    config: dict[str, typing.Any],
                    stage: str) -> bool:
    if (not k9_manifest["stage"]
        or k9_manifest["stage"] != stage):
        return True

    skip_this = False
    deps = k9_manifest.get("workload_dependencies", "").split(",")

    for dep in deps:
        dep = dep.strip().lower()
        if dep == "":
            continue
        if dep == "sap":
            skip_this = skip_this or not config["deploySAP"]
        elif dep == "sfdc":
            skip_this = skip_this or not config["deploySFDC"]
        elif dep == "oracleebs":
            skip_this = skip_this or not config["deployOracleEBS"]
        elif dep == "marketing":
            skip_this = skip_this or not config["deployMarketing"]
        elif dep == "googleads":
            skip_this = skip_this or (
                not config["deployMarketing"] or
                not config["marketing"]["deployGoogleAds"])
        elif dep == "cm360":
            skip_this = skip_this or (
                not config["deployMarketing"] or
                not config["marketing"]["deployCM360"])
        elif dep == "tiktok":
            skip_this = skip_this or (
                not config["deployMarketing"] or
                not config["marketing"]["deployTikTok"])
        elif dep == "liveramp":
            skip_this = skip_this or (
                not config["deployMarketing"] or
                not config["marketing"]["deployLiveRamp"])
        elif dep == "meta":
            skip_this = skip_this or (
                not config["deployMarketing"] or
                not config["marketing"]["deployMeta"])
        elif dep == "sfmc":
            skip_this = skip_this or (
                not config["deployMarketing"] or
                not config["marketing"]["deploySFMC"])
        elif dep == "dv360":
            skip_this = skip_this or (
                not config["deployMarketing"] or
                not config["marketing"]["deployDV360"])
        elif dep == "ga4":
            skip_this = skip_this or (
                not config["deployMarketing"] or
                not config["marketing"]["deployGA4"])
        else:
            raise ValueError(f"Invalid workload dependency {dep}")

    if skip_this:
        logging.warning(("Skipping K9 `%s` because one or more workload "
                         "dependencies are not deployed."), k9_manifest["id"])

    return skip_this

def main(args: typing.Sequence[str]) -> int:
    """Main function.

    Args:
        args (typing.Sequence[str]): command line arguments.

    Returns:
        int: return code (0 - success).
    """
    parser = argparse.ArgumentParser(description="K9 Deployer")
    parser.add_argument("--config-file",
                        type=str,
                        required=False,
                        default=_DEFAULT_CONFIG)
    parser.add_argument("--logs-bucket", type=str, required=True)
    parser.add_argument("--stage", choices=["pre", "post"], required=True)
    parser.add_argument(
        "--debug",
        help="Debugging mode.",
        action="store_true",
        default=False,
        required=False,
    )
    parser.add_argument("--worker-pool-name", required=False)
    parser.add_argument("--region", required=False)
    parser.add_argument("--build-account", required=False)
    params = parser.parse_args(args)

    logging.basicConfig(
        format="%(asctime)s | %(levelname)s | %(message)s",
        level=logging.DEBUG if params.debug else logging.INFO,
    )

    logging.info("============== 🐕 Deploying K9 stage `%s` 🐕 ==============",
                 params.stage)

    k9_root_path = Path(__file__).parent.absolute()
    manifest_file = k9_root_path.joinpath(_K9_MANIFEST)
    config_file = str(Path(params.config_file).absolute())
    logs_bucket = params.logs_bucket
    stage = params.stage.lower()

    config = configs.load_config_file(config_file)
    manifest = k9_deployer.load_k9s_manifest(manifest_file)
    k9_settings = _load_k9s_settings(config_file)

    source_project = config["projectIdSource"]
    target_project = config["projectIdTarget"]
    location = config["location"].lower()
    processing_dataset = config["k9"]["datasets"]["processing"]
    reporting_dataset = config["k9"]["datasets"]["reporting"]
    vertexai_dataset = config["VertexAI"]["processingDataset"]

    # Check if telemetry is allowed, default to True
    allow_telemetry = config.get("allowTelemetry", True)

    bq_client = cortex_bq_client.CortexBQClient(source_project,
                                                location=location)
    _create_dataset(f"{source_project}.{processing_dataset}", location,
                    bq_client, label_dataset=allow_telemetry)
    _create_dataset(f"{target_project}.{reporting_dataset}", location,
                    bq_client, label_dataset=allow_telemetry)
    _create_dataset(f"{source_project}.{vertexai_dataset}",
                    config["VertexAI"]["region"],
                    bq_client, label_dataset=allow_telemetry)

    # Validate all K9 config before deploying.
    k9s_to_deploy = []

    for config_key, config_value in k9_settings.items():
        k9_id = None
        if config_key.startswith("deploy") and config_value:

            k9_id = config_key.replace("deploy", "")
            if k9_id not in manifest:
                logging.error("%s is not a valid k9 id.", k9_id)
                return 1
            k9_definition = manifest[k9_id]

            if not _should_skip_k9(k9_definition, config, stage):

                k9_setting = k9_settings.get(k9_id)
                k9s_to_deploy.append([k9_definition, k9_setting])

    # Actual deployment
    for (k9_definition, k9_setting) in k9s_to_deploy:
        k9_deployer.deploy_k9(k9_definition, k9_root_path,
                              config_file, logs_bucket,
                              extra_settings=k9_setting,
                              worker_pool_name=params.worker_pool_name,
                              region=params.region,
                              build_account=params.build_account)

    logging.info(("==== 🦄🐕 Deploying K9 stage "
                  "`%s` is done! 🐕🦄 ====\n"), stage)
    return 0


################################################################################
if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
