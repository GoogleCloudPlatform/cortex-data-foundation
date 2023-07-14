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
"""Data Foundation Deployment UI main module"""


import argparse
import json
import logging
import os
import pathlib
import subprocess
import sys
import typing

from prompt_toolkit import PromptSession
from prompt_toolkit.formatted_text import HTML

from completers import WordCompleter
from configure import configure
from apply_config import (apply_all, SOURCE_PROJECT_APIS, TARGET_PROJECT_APIS,
                          PROJECT_ROLES)
from prompt import get_value, print_formatted, print_formatted_json, yes_no
from constants import DF_TITLE, CONFIG_MESSAGE
from datasets import get_all_datasets

_CONFIG_INPUT_FILE = "./config/config_default.json"
_CONFIG_OUTPUT_FILE = "./config/config.json"


def _initialize_console_logging(message_only: bool = True,
                                debug: bool = False,
                                min_level: int = logging.INFO):
    """Initializes Python root logger making sure Debug and Info
    messages go to STDOUT, while WARNING and above go to STDERR

    Args:
        message_only (bool, optional): True if only need to log messages.
            If False, the logging format will be
            "%(asctime)s | %(levelname)s | %(message)s"
        debug (bool, optional): True if logging level must be set to DEBUG.
            It will be set to INFO otherwise. Defaults to False.
    """
    h_info_and_below = logging.StreamHandler(sys.stdout)
    h_info_and_below.setLevel(logging.DEBUG)
    h_info_and_below.addFilter(lambda record: record.levelno <= logging.INFO)
    h_warn_and_above = logging.StreamHandler(sys.stderr)
    h_warn_and_above.setLevel(logging.WARNING)

    handlers = [h_info_and_below, h_warn_and_above]
    log_format = ("%(message)s" if message_only else
                  "%(asctime)s | %(levelname)s | %(message)s")
    logging.basicConfig(format=log_format,
                        level=logging.DEBUG if debug else min_level,
                        handlers=handlers,
                        force=True)


def main(args: typing.Sequence[str]) -> int:
    """Data Foundation Deployment UI main function"""

    parser = argparse.ArgumentParser(
        description="Cortex Data Foundation Configuration and Deployment")
    parser.add_argument(
        "--debug",
        help="Debugging mode.",
        action="store_true",
        default=False,
        required=False,
    )
    parser.add_argument(
        "--project",
        help="Default Google Cloud Project.",
        type=str,
        required=False,
        default=""
    )

    options, _ = parser.parse_known_args(args)

    _initialize_console_logging(False, options.debug, logging.INFO)

    existing_config = {}
    default_config = {}

    # Existing config in config.json
    if pathlib.Path(_CONFIG_OUTPUT_FILE).is_file():
        with open(_CONFIG_OUTPUT_FILE, "r", encoding="utf-8") as f:
            existing_config = json.load(f)

    # Default config in config_default.json
    if pathlib.Path(_CONFIG_INPUT_FILE).is_file():
        with open(_CONFIG_INPUT_FILE, "r", encoding="utf-8") as f:
            default_config = json.load(f)

    default_project = options.project
    # `--project` option may be used from a tutorial
    # without an initialized project.
    # In such case the value will be "<PROJECT-ID>"
    if default_project == "<PROJECT-ID>":
        default_project = ""
    if default_project == "":
        gcloud_result = subprocess.run(("gcloud config list --format "
                                        "'value(core.project)'"),
                                        stdout=subprocess.PIPE,
                                        shell=True,
                                        check=False)
        if (gcloud_result and gcloud_result.stdout
                and gcloud_result.returncode == 0):
            project_candidate = gcloud_result.stdout.decode("utf-8").strip()
            if " " not in project_candidate:
                default_project = project_candidate

    in_cloud_shell = os.environ.get("CLOUD_SHELL", "false").lower() == "true"

    if not in_cloud_shell and not yes_no(DF_TITLE,
                                         HTML(
                                          "This program should only be used "
                                          "in <b>Google Cloud Shell</b>.\n\n"
                                          "Are you sure you want to continue?"),
                                         full_screen=True):
        print_formatted("See you next time! 🦄")
        return 1

    config = configure(in_cloud_shell,
                       default_config,
                       existing_config,
                       default_project)

    if config:
        with open(_CONFIG_OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(config, f, indent=4)
        print_formatted("\nConfiguration:", bold=True)
        print_formatted_json(config)

        projects_text = config["projectIdSource"]
        if config["projectIdSource"] != config["projectIdTarget"]:
            projects_text += " and " + config["projectIdTarget"]
        buckets = set([config["targetBucket"]])
        if ("marketing" in config and "CM360" in config["marketing"] and
                config["marketing"]["CM360"].get(
                        "dataTransferBucket", "") != ""):
            buckets.add(config["marketing"]["CM360"]["dataTransferBucket"])
        buckets_text = ", ".join(buckets)
        apis_text = (f"<b>{', '.join(SOURCE_PROJECT_APIS)}</b> in "
            f"project <b>{config['projectIdSource']}</b>")
        if config["projectIdSource"] != config["projectIdTarget"]:
            apis_text += (f"\n    <b>{', '.join(TARGET_PROJECT_APIS)}</b> in "
            f"project <b>{config['projectIdTarget']}</b>")
        config_message = CONFIG_MESSAGE % (
            config["projectIdSource"],
            config["projectIdTarget"],
            config["location"],
            apis_text,
            "\n        ‣ " + "\n        ‣ ".join(get_all_datasets(config)),
            buckets_text,
            config["projectIdSource"],
            ", ".join(PROJECT_ROLES), projects_text,
            "roles/bigquery.dataEditor",
            "roles/storage.admin"
        )
        print_formatted(
                    (f"{DF_TITLE} is ready for deployment.\n\n"
                    "<b>This program will enable necessary APIs, "
                    "create resources and datasets,\nassign permissions, "
                    "and run deployment scripts on your behalf.</b>\n\n"
                    "<i>Your account must have "
                    "necessary permissions and roles assigned.</i>\n\n"
                    "The following changes will be made prior"
                    " to the deployment:\n") +
                    config_message +
                    "\n\nThese changes will be applied to your project."
                    "\nSome changes may incur additional costs."
                    "\n\n<b><u><ansibrightred>DO NOT PROCEED IF DEPLOYING INTO "
                    "PRODUCTION PROJECTS AND/OR "
                    "WITH PRODUCTION DATA!</ansibrightred></u></b>\n")
        yes_no_answer = get_value(PromptSession(),
                                  "Proceed? [Yes/No]",
                                  WordCompleter(["yes", "no"],
                                                ignore_case=True)).lower()
        if yes_no_answer == "no":
            print_formatted("Please read Data Foundation documentation. "
                                "See you next time! 🦄")
            return 1

        # Applying resource configuration
        print_formatted(f"\n\n🦄 Applying {DF_TITLE} Configuration 🦄\n")
        result = apply_all(config)
        if result:
            print_formatted("🦄 Done! 🦄\n", bold=True)
        return 0 if result else 1
    else:
        return 1


###############################################################
if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
