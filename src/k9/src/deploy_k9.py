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
from jinja2 import Environment, FileSystemLoader
import logging
from pathlib import Path
import subprocess
import sys
import tempfile
import typing
import yaml

from google.cloud import bigquery

from common.py_libs import configs, jinja, bq_helper, test_harness

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


def simple_process_and_upload(k9_id: str, k9_dir: str, jinja_dict: dict,
                              target_bucket: str, bq_client: bigquery.Client):
    """Recursively processes all files in k9_dir,
       executes all sql files in alphabetical order,
       uploads all processed files recursively
       to gs://{target_bucket}/dags/{k9_id}"""

    logging.info("Deploying simple k9 `%s`.", k9_id)

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_dir_path = Path(tmp_dir)
        k9_files = sorted(Path(k9_dir).rglob("*"))
        for path in k9_files:
            rel_path = path.relative_to(k9_dir)
            if path.is_dir() or str(rel_path).startswith("reporting/"):
                continue
            tgt_path = tmp_dir_path.joinpath(rel_path)
            tgt_dir = tgt_path.parent
            if not tgt_dir.exists():
                tgt_dir.mkdir(parents=True)
            env = Environment(
                loader=FileSystemLoader(str(path.parent.absolute())))
            input_template = env.get_template(path.name)
            output_text = str(input_template.render(jinja_dict))
            with open(str(tgt_path), mode="w", encoding="utf-8") as output:
                output.write(output_text)
            if tgt_path.suffix.lower() == ".sql":
                logging.info("Executing %s", str(tgt_path.relative_to(tmp_dir)))
                bq_helper.execute_sql_file(bq_client, str(tgt_path))
        # make sure every DAG folder has __init__.py
        if "__init__.py" not in [str(p.relative_to(k9_dir)) for p in k9_files]:
            with open(f"{tmp_dir}/__init__.py", "w", encoding="utf-8"):
                pass
        logging.info("Copying generated files to gs://%s/dags/%s",
                     target_bucket, k9_id)
        subprocess.check_call([
            "gsutil", "-m", "cp", "-r", f"{tmp_dir}/",
            f"gs://{target_bucket}/dags/{k9_id}"
        ])


def main(args: typing.Sequence[str]) -> int:
    """main function

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

    this_folder = Path(__file__).parent.absolute()
    settings_file = this_folder.parent.joinpath(_K9_SETTINGS)
    manifest_file = this_folder.joinpath(_K9_MANIFEST)
    config_file = str(Path(params.config_file).absolute())
    logs_bucket = params.logs_bucket

    config = configs.load_config_file(config_file)
    with open(settings_file, encoding="utf-8") as settings_fp:
        settings = yaml.safe_load(settings_fp)
    with open(manifest_file, encoding="utf-8") as manifest_fp:
        manifest_dawgs = yaml.safe_load(manifest_fp)

    manifest = {}
    for dawg in manifest_dawgs["dawgs"]:
        manifest[dawg["id"]] = dawg

    source_project = config["projectIdSource"]
    target_project = config["projectIdTarget"]
    deploy_test_data = config["testData"]
    location = config["location"].lower()
    target_bucket = config["targetBucket"]
    processing_dataset = config["k9"]["datasets"]["processing"]
    reporting_dataset = config["k9"]["datasets"]["reporting"]

    stage = params.stage.lower()
    if "k9s" not in settings:
        logging.warning("No k9s listed to deploy.")
        return 0
    k9s_settings = settings["k9s"]

    bq_client = bigquery.Client(source_project, location=location)
    _create_dataset(f"{source_project}.{processing_dataset}", location,
                    bq_client)
    _create_dataset(f"{target_project}.{reporting_dataset}", location,
                    bq_client)

    for k9 in k9s_settings:
        # Some of the K9s may not have their own settings file,
        # but use the k9 settings file instead.
        # Example with a customizable Weather:
        #    - weather:
        #        first_day_of_week: Monday
        # In such cases yaml library parses weather node into a dictionary,
        # with weather to be the first key and None as it's value:
        # { "weather": None, "first_day_of_week": "Monday" }
        #
        # To account for that, we check whether the value is a dict.
        # If so, using the first key is the k9's id.
        if isinstance(k9, dict):
            k9_id = list(k9.keys())[0]
        else:
            k9_id = k9
        if k9_id not in manifest:
            logging.error("%s is not a valid k9 id.", k9_id)
            return 1
        k9_manifest = manifest[k9_id]

        if k9_manifest["stage"] != stage:
            continue

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
            else:
                logging.error("Invalid workload dependency %s", dep)
                return 1

        if skip_this:
            logging.warning(("Skipping K9 `%s` because one or more "
                             "workload dependencies are not deployed."), k9_id)
            continue

        logging.info("🐕 Deploying k9 %s 🐕", k9_id)

        if deploy_test_data and "test_data" in k9_manifest:
            logging.info("Loading test data for k9 %s", k9_id)
            tables = [t.strip() for t in k9_manifest["test_data"].split(",")]
            target_type = k9_manifest.get("test_data_target",
                                          "k9.datasets.processing")
            components = target_type.split(".")
            project = (target_project
                       if components[-1] == "reporting" else source_project)
            # Starting with the whole config,
            # then iterating over dictionaries tree.
            ds_cfg = config
            for comp in components:
                ds_cfg = ds_cfg[comp]
            target_dataset = ds_cfg
            test_data_dataset = test_harness.get_test_harness_dataset(
                "k9", "processing", location)
            sources = [
                f"{config['testDataProject']}.{test_data_dataset}.{table}"
                for table in tables
            ]
            targets = [
                f"{project}.{target_dataset}.{table}" for table in tables
            ]
            bq_helper.load_tables(bq_client,
                                  sources,
                                  targets,
                                  location=location,
                                  skip_existing_tables=True)

        k9_path = this_folder.joinpath(k9_manifest["path"])
        if "entry_point" in k9_manifest:
            # call a custom entry point
            entry_point = k9_path.joinpath(k9_manifest["entry_point"])
            ext = entry_point.suffix.lower()
            exec_params = [str(entry_point), str(config_file), logs_bucket]
            if ext == ".py":
                exec_params.insert(0, "python3")
            elif ext == ".sh":
                exec_params.insert(0, "bash")
            logging.info("Executing k9 `%s` deployer: %s.", k9_id, entry_point)
            subprocess.check_call(exec_params)
        else:
            # simple deployment
            jinja_dict = jinja.initialize_jinja_from_config(config)
            simple_process_and_upload(k9_id, str(k9_path), jinja_dict,
                                      target_bucket, bq_client)

        if "reporting_settings" in k9_manifest:
            reporting_settings_file = k9_path.joinpath(
                k9_manifest["reporting_settings"])
            logging.info("k9 `%s` has Reporting views.", k9_id)
            logging.info("Executing Materializer for `%s` with `%s`.", k9_id,
                         reporting_settings_file)
            exec_params = [
                "./src/common/materializer/deploy.sh", "--gcs_logs_bucket",
                logs_bucket, "--gcs_tgt_bucket", target_bucket, "--config_file",
                str(config_file), "--materializer_settings_file",
                str(reporting_settings_file), "--target_type", "Reporting",
                "--module_name", "k9"
            ]
            subprocess.check_call(exec_params)

        logging.info("🐕 k9 `%s` has been deployed! 🐕", k9_id)

    logging.info(("==== 🦄🐕 Deploying K9 stage "
                  "`%s` is done! 🐕🦄 ====\n"), stage)
    return 0


################################################################################
if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
