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
"""Data Foundation Deployment UI"""


import logging
import os
import subprocess
import typing

from prompt_toolkit import PromptSession
from prompt_toolkit.cursor_shapes import CursorShape, to_cursor_shape_config
from prompt_toolkit.shortcuts import checkboxlist_dialog, radiolist_dialog
from prompt_toolkit.styles import Style
from prompt_toolkit.formatted_text import HTML

import google.auth

from completers import (GCPProjectCompleter,
                        RegionsCompleter,
                        StorageBucketCompleter)
from prompt import (get_value, yes_no, print_formatted, print_formatted_json)
from datasets import (prompt_for_datasets, check_datasets_locations,
                      clear_dataset_names)
from name_checker import is_bucket_name_valid
from constants import DF_TITLE


def configure(in_cloud_shell: bool,
              default_config: typing.Dict[str, typing.Any],
              existing_config: typing.Dict[str, typing.Any],
              default_project: str) -> typing.Optional[
                                            typing.Dict[str,
                                                typing.Any]]:
    """UI driver for Data Foundation configurator.

    Args:
        in_cloud_shell (bool): True if running in Cloud Shell.
        default_config (typing.Dict[str, typing.Any]): default configuration
                                                       dictionary
        existing_config (typing.Dict[str, typing.Any]): loaded configuration
                                                        dictionary
        default_project (str): default project id to use

    Returns:
        typing.Optional[ typing.Dict[str, typing.Any]]: configured configuration
                                                        dictionary
    """

    config = default_config
    went_with_existing = False
    # If source project, target project, location and target bucket
    # are specified in config.json,
    # we assume it's initialized, and use existing config.
    if (existing_config.get("projectIdSource", "") != "" and
        existing_config.get("projectIdTarget", "") != "" and
         existing_config.get("location", "") != "" and
          existing_config.get("targetBucket", "") != ""):
        if yes_no(
            f"{DF_TITLE} Configuration",
            HTML("There is an existing Data Foundation configuration "
            "in <b>config/config.json:</b>\n"
            f"   Source Project: <b>{existing_config['projectIdSource']}</b>\n"
            f"   Target Project: <b>{existing_config['projectIdTarget']}</b>\n"
            f"   Location: <b>{existing_config['location']}</b>"
            "\n\nWould you like to load it?"),
            full_screen=True
            ):
            print_formatted(
                "\n\n🦄 Using existing configuration in config.json:",
                bold=True)
            print_formatted_json(existing_config)
            config = existing_config
            went_with_existing = True

    if not default_project:
        default_project = os.environ.get("GOOGLE_CLOUD_PROJECT",
                                         None)  # type: ignore
    else:
        os.environ["GOOGLE_CLOUD_PROJECT"] = default_project

    print_formatted("\nInitializing...", italic=True, end="")
    try:
        logging.disable(logging.WARNING)
        credentials, _ = google.auth.default()
    finally:
        logging.disable(logging.NOTSET)
    project_completer = GCPProjectCompleter(credentials)
    if not default_project:
        default_project = ""

    session = PromptSession()
    session.output.erase_screen()
    print("\r", end="")
    print_formatted(f"{DF_TITLE}\n")

    defaults = []
    if config.get("deploySAP"):
        defaults.append("deploySAP")
    if config.get("deploySFDC"):
        defaults.append("deploySFDC")
    if (config.get("deployMarketing") and
        config["marketing"].get("deployGoogleAds")):
        defaults.append("deployGoogleAds")
    if (config.get("deployMarketing") and
        config["marketing"].get("deployCM360")):
        defaults.append("deployCM360")
    if (config.get("deployMarketing") and
        config["marketing"].get("deployTikTok")):
        defaults.append("deployTikTok")
    if (config.get("deployMarketing") and
        config["marketing"].get("deployLiveRamp")):
        defaults.append("deployLiveRamp")
    if (config.get("deployMarketing") and
        config["marketing"].get("deployMeta")):
        defaults.append("deployMeta")
    if (config.get("deployMarketing") and
        config["marketing"].get("deploySFMC")):
        defaults.append("deploySFMC")

    while True:
        dialog = checkboxlist_dialog(
            title=HTML(DF_TITLE),
            text=HTML(
                f"Welcome to {DF_TITLE}.\n\n"
                "Please select the workloads for deployment."),
            values=[
                ("deploySAP", "SAP"),
                ("deploySFDC",
                 "Salesforce.com (SFDC)"),
                ("deployGoogleAds",
                 "Marketing with Google Ads"),
                ("deployCM360",
                 "Marketing with Campaign Manager 360 (CM360)"),
                ("deployTikTok",
                 "Marketing with TikTok"),
                ("deployLiveRamp",
                 "Marketing with LiveRamp"),
                ("deployMeta",
                 "Marketing with Meta"),
                ("deploySFMC",
                 "Marketing with SFMC"),
            ],
            default_values=defaults,
            style=Style.from_dict({
                "checkbox-selected": "bg:lightgrey",
                "checkbox-checked": "bold",
            }))
        dialog.cursor = to_cursor_shape_config(CursorShape.BLINKING_UNDERLINE)
        results = dialog.run()

        if not results:
            print_formatted("See you next time! 🦄")
            return None

        config["deploySAP"] = "deploySAP" in results
        config["deploySFDC"] = "deploySFDC" in results
        config["deployMarketing"] = ("deployGoogleAds" in results or
                                     "deployCM360" in results or
                                     "deployTikTok" in results or
                                     "deployLiveRamp" in results or
                                     "deployMeta" in results or
                                     "deploySFMC" in results)
        config["marketing"]["deployGoogleAds"] = "deployGoogleAds" in results
        config["marketing"]["deployCM360"] = "deployCM360" in results
        config["marketing"]["deployTikTok"] = "deployTikTok" in results
        config["marketing"]["deployLiveRamp"] = "deployLiveRamp" in results
        config["marketing"]["deployMeta"] = "deployMeta" in results
        config["marketing"]["deploySFMC"] = "deploySFMC" in results

        if (config["deploySAP"] is False and config["deploySFDC"] is False
            and config["deployMarketing"] is False):
            if yes_no(
                    f"{DF_TITLE} Configuration",
                    "Please select one or more Data Foundation workloads.",
                    yes_text="Try again",
                    no_text="Cancel"
            ):
                continue
            else:
                print_formatted("Please check the documentation for "
                                "Cortex Data Foundation. "
                                "See you next time! 🦄")
                return None
        break

    auto_names = True
    dialog = radiolist_dialog(HTML(f"{DF_TITLE} Configuration"),
                     HTML("How would you like to configure Data Foundation "
                     "(e.g. BigQuery datasets, Cloud Storage buckets, APIs)\n"
                     "and configure permissions for deployment?\n\n"
                     "Auto-configuration will create all necessary resources "
                     "and configure permissions.\n"
                     "\n<b><u><ansibrightred>This creates a demo deployment "
                     "with test data, not valid for production "
                     "environments.</ansibrightred></u></b>"),
                     values=[
                ("test",
                 HTML(
                    ("<b>Use pre-configured BigQuery datasets and Storage "
                     "buckets</b> "
                    "to deploy demo environment with test data and "
                    "auto-configuration."
                        if went_with_existing else
                    "<b>Use default BigQuery datasets and Storage bucket</b> "
                    "to deploy demo environment with test data and "
                    "auto-configuration."))),
                ("testChooseDatasets",
                 HTML(
                    "<b>Let me choose BigQuery datasets and "
                    "Storage buckets</b> to deploy demo with test data, "
                    " auto-configuration.")),
                ("deployManual",
                 HTML(
                    "<b>Manual configuration and deployment</b>. "
                    "Let me configure and deploy everything manually "
                    "(Coming soon).")),
            ],
            style=Style.from_dict({
                "radio-selected": "bg:lightgrey",
                "radio-checked": "bold",
            }),
            default="test")
    dialog.cursor = to_cursor_shape_config(CursorShape.BLINKING_UNDERLINE)
    choice = dialog.run()
    if not choice or choice == "deployManual":
        print_formatted(
            "The guided deployment option is coming soon. "
            "Please check Data Foundation documentation: \n"
            "https://github.com/GoogleCloudPlatform/"
            "cortex-data-foundation/blob/main/README.md 🦄")
        if in_cloud_shell:
            subprocess.run("cloudshell edit config/config.json",
                           shell=True, check=False)
            # TODO (vladkol): launch guided deployment tutorial when ready
            #subprocess.run("cloudshell launch-tutorial docs/guide.md",
            #               shell=True, check=False)
        return None
    elif choice == "test":
        auto_names = True
    elif choice == "testChooseDatasets":
        auto_names = False

    source_project = config.get("projectIdSource", default_project)
    if source_project == "":
        source_project = default_project
    target_project = config.get("projectIdTarget", source_project)
    bq_location = config.get("location", "").lower()
    config["location"] = bq_location

    if source_project == "":
        source_project = None
    if target_project == "":
        target_project = source_project or None
    if bq_location == "":
        bq_location = "us"

    print_formatted("\nEnter or confirm Data Foundation configuration values:",
                    bold=True)

    old_source_project = source_project
    source_project = get_value(
        session,
        "Source GCP Project",
        project_completer,
        source_project or "",
        description="Specify Data Foundation Source Project (existing).",
        allow_arbitrary=False,
    )
    os.environ["GOOGLE_CLOUD_PROJECT"] = source_project
    if not target_project or target_project == old_source_project:
        target_project = source_project
    target_project = get_value(
        session,
        "Target GCP Project",
        project_completer,
        target_project,
        description="Specify Data Foundation Target Project (existing).",
        allow_arbitrary=False,
    )

    config["projectIdSource"] = source_project
    config["projectIdTarget"] = target_project

    print_formatted("Retrieving regions...", italic=True, end="")
    regions_completer = RegionsCompleter()
    print("\r", end="")


    bq_location = get_value(
            session,
            "BigQuery Location",
            regions_completer,
            default_value=bq_location.lower(),
            description="Specify GCP Location for BigQuery Datasets.",
        )
    bq_location = bq_location.lower()
    config["location"] = bq_location

    bucket_name = config.get("targetBucket", "")
    if bucket_name == "":
        bucket_name = f"cortex-{source_project}-{bq_location}"

    datasets_wrong_locations = []
    if auto_names:
        datasets_wrong_locations = check_datasets_locations(config)
        if len(datasets_wrong_locations):
            auto_names = False

    if not auto_names:
        while True:
            if len(datasets_wrong_locations) > 0:
                ds_list_str = "\n".join(
                    [f"   - {ds}" for ds in datasets_wrong_locations])
                if not yes_no(DF_TITLE,
                          HTML("These datasets already exist in a location "
                               f"different than `{bq_location}`:\n"
                               f"{ds_list_str}"
                               "\n\n<b>Please choose different datasets.</b>"
                               ),
                          full_screen=True,
                          yes_text="OK",
                          no_text="CANCEL"):
                    config = None
                    break
                config = clear_dataset_names(config)
            config = prompt_for_datasets(session, config) # type: ignore
            datasets_wrong_locations = check_datasets_locations(
                                            config) # type: ignore
            if len(datasets_wrong_locations) == 0:
                break
        if not config:
            print_formatted(
                "Please check the documentation for Cortex Data Foundation. "
                "See you next time! 🦄")
            return None
        if "targetBucket" not in config or config["targetBucket"] == "":
            config["targetBucket"] = f"cortex-{source_project}-{bq_location}"
        bucket_completer = StorageBucketCompleter(source_project)
        while True:
            bucket_name = get_value(session,
                                    "Target Storage Bucket",
                                    bucket_completer,
                                    bucket_name,
                                    description="Specify Target Storage Bucket",
                                    allow_arbitrary=True)
            if not is_bucket_name_valid(bucket_name):
                if yes_no(
                    "Cortex Data Foundation Configuration",
                    f"{bucket_name} is not a valid bucket name.",
                    yes_text="Try again",
                    no_text="Cancel"
                ):
                    bucket_name = ""
                    continue
                else:
                    return None
            else:
                break
    config["targetBucket"] = bucket_name

    if config["deployMarketing"]:
        dataflow_region = bq_location.lower()
        # Dataflow region cannot be a multi-region.
        # If BQ location is a multi-region,
        # default to a region with the majority of services available
        # in the same multi-region.
        if dataflow_region == "us":
            dataflow_region = "us-central1"
        elif dataflow_region == "eu":
            dataflow_region = "europe-west4"
        config["marketing"]["dataflowRegion"] = dataflow_region
        if not auto_names:
            dataflow_completer = RegionsCompleter(False, bq_location)
            config["marketing"]["dataflowRegion"] = get_value(session,
                                        "Dataflow Region",
                                        dataflow_completer,
                                        dataflow_region,
                                        description="Dataflow Compute Region")
        if config["marketing"].get("deployCM360"):
            cm360 = config["marketing"]["CM360"]
            if cm360.get("dataTransferBucket", "") == "":
                bucket_name = f"cortex-{source_project}-{bq_location}"
            while not auto_names:
                bucket_completer = StorageBucketCompleter(source_project)
                bucket_name = get_value(session,
                                "CM360 Data Transfer Bucket",
                                bucket_completer,
                                bucket_name,
                                description="Specify "
                                "CM360 Data Transfer Bucket",
                                allow_arbitrary=True)
                if not is_bucket_name_valid(bucket_name):
                    if yes_no(
                        "Cortex Data Foundation Configuration",
                        f"{bucket_name} is not a valid bucket name.",
                        yes_text="Try again",
                        no_text="Cancel"
                    ):
                        bucket_name = ""
                        continue
                    else:
                        return None
                else:
                    break
            config["marketing"]["CM360"]["dataTransferBucket"] = bucket_name
        if config["marketing"].get("deploySFMC"):
            cm360 = config["marketing"]["SFMC"]
            if cm360.get("fileTransferBucket", "") == "":
                bucket_name = f"cortex-{source_project}-{bq_location}"
            while not auto_names:
                bucket_completer = StorageBucketCompleter(source_project)
                bucket_name = get_value(session,
                                "SFMC File Transfer Bucket",
                                bucket_completer,
                                bucket_name,
                                description="Specify "
                                "SFMC File Transfer Bucket",
                                allow_arbitrary=True)
                if not is_bucket_name_valid(bucket_name):
                    if yes_no(
                        "Cortex Data Foundation Configuration",
                        f"{bucket_name} is not a valid bucket name.",
                        yes_text="Try again",
                        no_text="Cancel"
                    ):
                        bucket_name = ""
                        continue
                    else:
                        return None
                else:
                    break
            config["marketing"]["SFMC"]["fileTransferBucket"] = bucket_name

    return config
