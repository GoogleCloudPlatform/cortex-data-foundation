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
"""
K9 deployer.
"""

import argparse
import logging
from pathlib import Path
import sys
import typing
import yaml

from google.cloud import bigquery

from common.py_libs import configs
from common.py_libs import k9_deployer

_DEFAULT_CONFIG = "config/config.json"  # data foundation config
_K9_SETTINGS = "config/k9_settings.yaml"  # relative to k9
_K9_MANIFEST = "manifest.yaml"


def _create_dataset(full_dataset_name: str, location: str,
                    bq_client: bigquery.Client) -> None:
    """Creates target (CDC/Reporting etc.) BQ target dataset if needed."""
    logging.info("Creating '%s' dataset if needed...", full_dataset_name)
    ds = bigquery.Dataset(full_dataset_name)
    ds.location = location
    ds = bq_client.create_dataset(ds, exists_ok=True, timeout=30)

def _load_k9s_settings(settings_file: str) -> dict:
    with open(settings_file, encoding="utf-8") as settings_fp:
        settings = yaml.safe_load(settings_fp)
        if "k9s" not in settings:
            logging.warning("No k9s listed to deploy.")
            return None
        return settings["k9s"]

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
        else:
            raise ValueError(f"Invalid workload dependency {dep}")

    if skip_this:
        logging.warning(("Skipping K9 `%s` because one or more workload "
                         "dependencies are not deployed."), k9_manifest["id"])

    return skip_this

def _remove_first_item(d: dict) -> None:
    """Removes the first item from a dict."""
    k = next(iter(d))
    d.pop(k)

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
    params = parser.parse_args(args)

    logging.basicConfig(
        format="%(asctime)s | %(levelname)s | %(message)s",
        level=logging.DEBUG if params.debug else logging.INFO,
    )

    logging.info("============== 🐕 Deploying K9 stage `%s` 🐕 ==============",
                 params.stage)

    k9_root_path = Path(__file__).parent.absolute()
    settings_file = k9_root_path.parent.joinpath(_K9_SETTINGS)
    manifest_file = k9_root_path.joinpath(_K9_MANIFEST)
    config_file = str(Path(params.config_file).absolute())
    logs_bucket = params.logs_bucket
    stage = params.stage.lower()

    config = configs.load_config_file(config_file)
    manifest = k9_deployer.load_k9s_manifest(manifest_file)
    k9s_settings = _load_k9s_settings(settings_file)

    source_project = config["projectIdSource"]
    target_project = config["projectIdTarget"]
    location = config["location"].lower()
    processing_dataset = config["k9"]["datasets"]["processing"]
    reporting_dataset = config["k9"]["datasets"]["reporting"]

    bq_client = bigquery.Client(source_project, location=location)
    _create_dataset(f"{source_project}.{processing_dataset}", location,
                    bq_client)
    _create_dataset(f"{target_project}.{reporting_dataset}", location,
                    bq_client)

    # Validate all K9 config before deploying.
    k9s_to_deploy = []
    for k9_setting in k9s_settings:
        k9_id = k9_deployer.get_k9_id(k9_setting)
        if k9_id not in manifest:
            logging.error("%s is not a valid k9 id.", k9_id)
            return 1
        k9_definition = manifest[k9_id]

        if not _should_skip_k9(k9_definition, config, stage):
            # Some of the K9s may not have their own settings file,
            # but use the k9 settings file instead.
            # Example with a customizable Weather:
            #    - weather:
            #        first_day_of_week: Monday
            # In such cases yaml library parses weather node into a dictionary,
            # with weather to be the first key and None as it's value:
            # { "weather": None, "first_day_of_week": "Monday" }
            #
            # To account for that, we remove the first item and pass the rest
            # over as settings.
            if isinstance(k9_setting, dict):
                _remove_first_item(k9_setting)
            # Otherwise, there are no additional settings for this k9.
            else:
                k9_setting = None

            k9s_to_deploy.append([k9_definition, k9_setting])

    # Actual deployment
    for (k9_definition, k9_setting) in k9s_to_deploy:
        k9_deployer.deploy_k9(k9_definition, k9_root_path,
                              config_file, logs_bucket,
                              extra_settings=k9_setting)

    logging.info(("==== 🦄🐕 Deploying K9 stage "
                  "`%s` is done! 🐕🦄 ====\n"), stage)
    return 0


################################################################################
if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
