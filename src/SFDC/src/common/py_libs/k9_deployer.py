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
"""Helper library for K9 deployment functions."""

from jinja2 import Environment, FileSystemLoader
import json
import logging
from pathlib import Path
import subprocess
import tempfile
import typing
import yaml

from google.cloud import bigquery

from common.py_libs import configs, jinja, bq_helper, test_harness
from common.py_libs.test_harness import TEST_HARNESS_VERSION

def _simple_process_and_upload(k9_id: str, k9_dir: str, jinja_dict: dict,
                              target_bucket: str, bq_client: bigquery.Client,
                              data_source = "k9", dataset_type="processing"):
    """Process and upload simple (traditional) K9.

    Recursively processes all files in k9_dir,
    Executes all sql files in alphabetical order,
    Rename .templatesql to .sql but do not execute them during deployment.
    Then uploads all processed files recursively to:
    - K9 pre and post:
        gs://{target_bucket}/dags/{k9_id}
    - Localized K9 (i.e. for a given data source):
        gs://{target_bucket}/dags/{data_source}/{dataset_type}/{k9_id}
    """

    logging.info("Deploying simple k9 `%s`.", k9_id)

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_dir_path = Path(tmp_dir)
        k9_files = sorted(Path(k9_dir).rglob("*"))
        for path in k9_files:
            rel_path = path.relative_to(k9_dir)
            if path.is_dir() or str(rel_path).startswith("reporting/"):
                continue
            tgt_path = tmp_dir_path.joinpath(rel_path)
            if tgt_path.suffix.lower() == ".templatesql":
                tgt_file_name = rel_path.stem + ".sql"
                logging.info("Renaming SQL template %s to %s without "
                             "executing.", rel_path.name, tgt_file_name)
                tgt_path = tmp_dir_path.joinpath(rel_path.parent, tgt_file_name)
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

        if data_source == "k9":
            target_path =  f"gs://{target_bucket}/dags/{k9_id}"
        else:
            # Only use actual data source name. Example: for data source
            # "marketing.CM360" we only want to use "cm360"
            ds_short_name = data_source.split(".")[-1].lower()
            target_path =  (f"gs://{target_bucket}/dags/{ds_short_name}/"
                                 f"{dataset_type}/{k9_id}")
        logging.info("Copying generated files to %s",
                     target_path)
        subprocess.check_call([
            "gsutil", "-m", "cp", "-r", f"{tmp_dir}/", target_path
        ])

def load_k9s_manifest(manifest_file:str) -> dict:
    """Load K9 Manifest and format into dict.

    Args:
        manifest_file (str): Rel path to manifest file.

    Returns:
        All K9s described in the manifest as a dict.
    """
    with open(manifest_file, encoding="utf-8") as manifest_fp:
        manifest_dawgs = yaml.safe_load(manifest_fp)

    manifest = {}
    for dawg in manifest_dawgs["dawgs"]:
        manifest[dawg["id"]] = dawg

    return manifest



def get_k9_id(k9: typing.Union[str, dict]) -> str:
    """Get K9 id from a standard manifest specification.

    Args:
        k9 (str|dict): K9 info from manifest.

    Returns:
        k9 id as a string.
    """
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
    return list(k9.keys())[0] if isinstance(k9, dict) else k9

def deploy_k9(k9_manifest: dict,
              k9_root_path: str,
              config_file: str,
              logs_bucket: str,
              data_source: str = "k9",
              dataset_type: str = "processing",
              extra_settings: dict = None) -> None:
    """Deploy a single K9 given manifest, settings, and parameters.

    Args:
        k9_manifest (dict): Dict containing manifest for the current K9.
        k9_root_path (str): Rel path to the current K9.
        config_file (str): Rel path to config.json file.
        logs_bucket (str): Log bucket name.
        data_source (str): Data source in config.json. Case sensitive.
                        Example: "sap", "marketing.CM360", "k9".
        dataset_type (str): Dataset type. Case sensitive.
                            Example: "cdc", "reporting", "processing".
        extra_settings (dict, optional): Ext settings that may apply to the k9.

    Returns:
        None.
    """

    config = configs.load_config_file(config_file)

    source_project = config["projectIdSource"]
    target_project = config["projectIdTarget"]
    deploy_test_data = config["testData"]
    location = config["location"].lower()
    target_bucket = config["targetBucket"]
    test_harness_version = config.get("testHarnessVersion",
                                      TEST_HARNESS_VERSION)
    bq_client = bigquery.Client(source_project, location=location)

    k9_id = k9_manifest["id"]
    logging.info("🐕 Deploying k9 %s 🐕", k9_id)

    if deploy_test_data and "test_data" in k9_manifest:
        logging.info("Loading test data for k9 %s", k9_id)
        #TODO Switch to YAML array instead of comma-delimited string.
        tables = [t.strip() for t in k9_manifest["test_data"].split(",")]
        target_type = k9_manifest.get("test_data_target",
                                      f"{data_source}.datasets.{dataset_type}")
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
            data_source,
            dataset_type,
            location,
            test_harness_version)
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

    k9_path = k9_root_path.joinpath(k9_manifest["path"])
    if "entry_point" in k9_manifest:
        # Call a custom entry point.
        entry_point = k9_path.joinpath(k9_manifest["entry_point"])
        ext = entry_point.suffix.lower()
        exec_params = [str(entry_point), str(config_file),
                                  logs_bucket]
        # Add JSON-based extra settings to command line if they exist.
        if extra_settings:
            exec_params.append(json.dumps(extra_settings))

        if ext == ".py":
            exec_params.insert(0, "python3")
        elif ext == ".sh":
            exec_params.insert(0, "bash")
        logging.info("Executing k9 `%s` deployer: %s.", k9_id, entry_point)
        subprocess.check_call(exec_params)
    else:
        # Perform simple deployment.
        jinja_dict = jinja.initialize_jinja_from_config(config)
        _simple_process_and_upload(k9_id, str(k9_path), jinja_dict,
                                  target_bucket, bq_client, data_source,
                                  dataset_type)

    # The following applies only if Reporting is specified from K9 standpoint,
    # i.e. during K9 pre or K9 post deployments.
    # For K9 invoked from Materializer, "reporting_settings" section is
    # ignored if it exists.

    if data_source == "k9" and "reporting_settings" in k9_manifest:
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
