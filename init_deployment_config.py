# Copyright 2022 Google LLC
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
Data foundation's cloudbuild.yaml, step 0 (validate_deploy_config.sh)
loads config/config.env to environment variables (CFG_).
Cloudbuild.yaml receives parameters via --substitutions.
These are mostly for SAP and are kept for backwards compatibility
If a value is present both from a parameter in --substitutions
and the config json file, the --substitutions parameter is preferred.

Input: Parameters (optional), config/config.json
Output: config.json and config.env in workspace/config/

Paths are relative to the root of the cortex-data-foundation.
"""

import argparse
import json
import logging
import shutil
import sys

from jinja2 import Environment, FileSystemLoader
from pathlib import Path

# Config and template file paths
_CONFIG_INPUT_FILE = "./config/config.json"
_CONFIG_TEMPLATE = "./config/.config_template.json"
_CONFIG_JINJA = ".config_template.jinja"
_SAP_CONFIG_JINJA = ".config_template_sap.jinja"
_SFDC_CONFIG_JINJA = ".config_template_sfdc.jinja"
_SAP_CONFIG_OUTPUT = "./src/SAP/SAP_REPORTING/config/sap_config.json"
_SFDC_CONFIG_OUTPUT = "./src/SFDC/config/sfdc_config.json"

# Global Config initial variables
CFG_TEST_DATA = ""
CFG_GEN_EXT = ""
CFG_DEPLOY_SAP = ""
CFG_DEPLOY_SFDC = ""
CFG_TURBO = ""
CFG_DEPLOY_CDC = ""
CFG_PJID_SRC = ""
CFG_PJID_TGT = ""
CFG_SAP_DS_CDC = ""
CFG_SAP_DS_RAW = ""
CFG_SAP_DS_CDC_ECC = ""
CFG_SAP_DS_CDC_S4 = ""
CFG_SAP_DS_RAW_ECC = ""
CFG_SAP_DS_RAW_S4 = ""
CFG_SAP_DS_REPORTING = ""
CFG_SAP_DS_MODELS = ""
CFG_LOCATION = ""
CFG_SAP_SQL_FLAVOUR = ""
CFG_SAP_MANDT = ""
CFG_SAP_MANDT_ECC = ""
CFG_SAP_MANDT_S4 = ""
CFG_CURRENCY = ""
CFG_LANGUAGE = ""
CFG_SFDC_DS_CDC = ""
CFG_SFDC_DS_RAW = ""
CFG_SFDC_DS_REPORTING = ""
CFG_SFDC_CREATE_MAPPING_VIEWS = ""


def load_config(config_file):

    if not Path(config_file).is_file():
        raise FileNotFoundError(f"Config file '{config_file}' does not exist.")

    with open(config_file, mode="r", encoding="utf-8") as cf:
        try:
            cfg = json.load(cf)
        except json.JSONDecodeError:
            e_msg = f"Config file '{config_file}' is malformed or empty."
            raise Exception(e_msg) from None

    return cfg


def check_create_file(config_template, config_output):
    """
    In backwards-compatible execution ("TL;DR" in GitHub),
    config files may not be found. Use the templates to
    create them in the right output directory.
    """
    if Path(config_output).is_file():
        logging.info("Config file %s found, updating", config_output)
    else:
        logging.info(
            "No config file  found, creating one "
            "from config template %s", config_output)
        shutil.copyfile(config_template, config_output)


def build_jinja_from_json(template_cfg, rules, jinja_substitutions):
    """
    Create jinja substitutions from config template
    and values in rules to use in final config.json
    """
    for k, v in template_cfg.items():
        if isinstance(v, dict):
            build_jinja_from_json(v, rules, jinja_substitutions)
        else:
            #v has the name of the rule from the config template map
            try:

                value_set = "false"
                for rule_value in rules[v]:
                    if rule_value is not None and rule_value != "":
                        value_set = "true"
                        logging.info("Setting value %s to variable %s",
                                     rule_value, v)
                        jinja_substitutions[v] = json.dumps(rule_value)
                        break

                if value_set == "false":
                    jinja_substitutions[v] = '""'
                    logging.warning("WARNING : no value assigned to %s", v)

            except Exception as e:
                logging.error("No rule found! Program error for %s", str(e))
                sys.exit(1)

    return jinja_substitutions


def create_config(config_template_path, config_input_path, rules):
    """
    Use the config templates in /config to get the values
    from the rules and replace the .env files in the root
    SAP or SFDC directory.
    """

    check_create_file(config_template_path, config_input_path)

    cfg = load_config(config_template_path)

    jinja_replacements = {}
    jinja_replacements = build_jinja_from_json(cfg, rules, jinja_replacements)

    return jinja_replacements


def write_json_config(template, jinja_replacements, output_config_path):

    env = Environment(loader=FileSystemLoader("./config/"))
    template = env.get_template(template)
    output_config_json = template.render(jinja_replacements)

    with open(output_config_path, mode="w",
              encoding="utf-8") as output_json_file:
        output_json_file.write(output_config_json)


def main():

    logging.info("Starting validation of config files 🦄🔪")

    #TODO: save potential missing keys in the file
    cfg = load_config(_CONFIG_INPUT_FILE)
    CFG_TEST_DATA = cfg["testData"]
    CFG_GEN_EXT = cfg["generateExtraData"]
    CFG_DEPLOY_SAP = cfg["deploySAP"]
    CFG_DEPLOY_SFDC = cfg["deploySFDC"]
    CFG_TURBO = cfg["turboMode"]
    CFG_DEPLOY_CDC = cfg["deployCDC"]
    CFG_LOCATION = cfg["location"]
    CFG_CURRENCY = cfg["currencies"]
    CFG_LANGUAGE = cfg["languages"]
    CFG_PJID_SRC = cfg["projectIdSource"]
    CFG_PJID_TGT = cfg["projectIdTarget"]
    CFG_SAP_DS_CDC = cfg["SAP"]["datasets"]["cdc"]
    CFG_SAP_DS_RAW = cfg["SAP"]["datasets"]["raw"]
    CFG_SAP_DS_CDC_ECC = cfg["SAP"]["datasets"]["cdcECC"]
    CFG_SAP_DS_CDC_S4 = cfg["SAP"]["datasets"]["cdcS4"]
    CFG_SAP_DS_RAW_ECC = cfg["SAP"]["datasets"]["rawECC"]
    CFG_SAP_DS_RAW_S4 = cfg["SAP"]["datasets"]["rawS4"]
    CFG_SAP_DS_REPORTING = cfg["SAP"]["datasets"]["reporting"]
    CFG_SAP_DS_MODELS = cfg["SAP"]["datasets"]["ml"]
    CFG_SAP_SQL_FLAVOUR = cfg["SAP"]["SQLFlavor"]
    CFG_SAP_MANDT = cfg["SAP"]["mandt"]
    CFG_SAP_MANDT_ECC = cfg["SAP"]["mandtECC"]
    CFG_SAP_MANDT_S4 = cfg["SAP"]["mandtS4"]
    CFG_SFDC_CREATE_MAPPING_VIEWS = cfg["SFDC"]["createMappingViews"]
    CFG_SFDC_DS_CDC = cfg["SFDC"]["datasets"]["cdc"]
    CFG_SFDC_DS_RAW = cfg["SFDC"]["datasets"]["raw"]
    CFG_SFDC_DS_REPORTING = cfg["SFDC"]["datasets"]["reporting"]

    # Parse arguments from cloudbuild.yaml --substitutions
    parser = argparse.ArgumentParser()
    parser.add_argument("--PJID_SRC",
                        help="Source Dataset Project ID.",
                        required=False)
    parser.add_argument("--PJID_TGT",
                        help="Target Dataset Project ID.",
                        required=False)
    parser.add_argument("--DS_CDC",
                        help="SAP CDC Dataset Name.",
                        required=False)
    parser.add_argument("--DS_RAW",
                        help="SAP Raw Landing Dataset Name.",
                        required=False)
    parser.add_argument("--DS_REPORTING",
                        help="Target Dataset Name for SAP Reporting.",
                        required=False)
    parser.add_argument("--DS_MODELS",
                        help="Target Dataset Name for ML.",
                        required=False)
    parser.add_argument("--LOCATION",
                        help="Dataset Location (Default: US)",
                        required=False)
    parser.add_argument("--MANDT",
                        help="SAP Mandant. (Default: 100)",
                        required=False)
    parser.add_argument(
        "--SQL_FLAVOUR",
        help="SQL Flavor Selection for SAP (ECC or S4 or UNION).",
        required=False)
    parser.add_argument("--GCS_BUCKET", help="Bucket for DAGs", required=False)
    parser.add_argument("--TEST_DATA",
                        help="Generate test data",
                        required=False)
    parser.add_argument("--TURBO", help="Use TURBO mode", required=False)
    parser.add_argument("--GEN_EXT", help="Generate extra data", required=False)
    parser.add_argument("--DEPLOY_SAP",
                        help="Deploy SAP modules",
                        required=False)
    parser.add_argument("--DEPLOY_SFDC",
                        help="Deploy Salesforce.com",
                        required=False)
    parser.add_argument("--DEPLOY_CDC", help="Deploy CDC", required=False)
    args = parser.parse_args()

    # Not all rules apply to every variable, but here is the priority:
    # 1. Value from --substitutions (command line or default in cloudbuild.yaml)
    # 2. Value from config.json file
    # 3. Value equated from other present values (e.g, PJID_TGT  = PJID_SRC,
    #    SFDC from SAP's)
    # 4. Default value (e.g., LOCATION = US, TURBO = true)
    rules = {
        "PJID_SRC": [args.PJID_SRC, CFG_PJID_SRC],
        # If no target project is provided, assume same as source
        "PJID_TGT": [args.PJID_TGT, CFG_PJID_TGT, args.PJID_SRC, CFG_PJID_SRC],
        "DS_CDC": [args.DS_CDC, CFG_SAP_DS_CDC],
        "DS_RAW": [args.DS_RAW, CFG_SAP_DS_RAW],
        "DS_REPORTING": [args.DS_REPORTING, CFG_SAP_DS_REPORTING, "REPORTING"],
        "DS_MODELS": [args.DS_MODELS, CFG_SAP_DS_MODELS, "ML_MODELS"],
        "LOCATION": [args.LOCATION, CFG_LOCATION, "US"],
        "SQL_FLAVOUR": [args.SQL_FLAVOUR, CFG_SAP_SQL_FLAVOUR, "ecc"],
        "TEST_DATA": [args.TEST_DATA, CFG_TEST_DATA, True],
        "GEN_EXT": [args.GEN_EXT, CFG_GEN_EXT, True],
        "DEPLOY_CDC": [args.DEPLOY_CDC, CFG_DEPLOY_CDC, True],
        "DEPLOY_SAP": [args.DEPLOY_SAP, CFG_DEPLOY_SAP, True],
        "DEPLOY_SFDC": [args.DEPLOY_SFDC, CFG_DEPLOY_SFDC, True],
        "TURBO": [args.TURBO, CFG_TURBO, True],
        "MANDT": [args.MANDT, CFG_SAP_MANDT, "100"],
        # SQL Flavor-specific parameters (_ECC, _S4)
        # are not passed in parameters.
        # If env file doesn't have them, try generic parameter
        "DS_CDC_ECC": [CFG_SAP_DS_CDC_ECC, args.DS_CDC, CFG_SAP_DS_CDC],
        "DS_CDC_S4": [CFG_SAP_DS_CDC_S4, args.DS_CDC, CFG_SAP_DS_CDC],
        "DS_RAW_ECC": [CFG_SAP_DS_RAW_ECC, args.DS_RAW, CFG_SAP_DS_RAW],
        "DS_RAW_S4": [CFG_SAP_DS_RAW_S4, args.DS_RAW, CFG_SAP_DS_RAW],
        "MANDT_ECC": [CFG_SAP_MANDT_ECC, args.MANDT, CFG_SAP_MANDT, "100"],
        "MANDT_S4": [CFG_SAP_MANDT_S4, args.MANDT, CFG_SAP_MANDT, "100"],
        "CURRENCY": [CFG_CURRENCY, "USD"],
        "LANGUAGE": [CFG_LANGUAGE, '"E", "S" '],
        ## Salesforce-specific parameters
        # For backwards compatibility with TL;DR command in the GitHub README,
        # these will default to SAP's assigned values from gcloud builds
        # --substitutions
        "SFDC_CREATE_MAPPING_VIEWS": [CFG_SFDC_CREATE_MAPPING_VIEWS, True],
        "DS_CDC_SFDC": [CFG_SFDC_DS_CDC, args.DS_CDC, CFG_SAP_DS_CDC],
        "DS_RAW_SFDC": [CFG_SFDC_DS_RAW, args.DS_RAW, CFG_SAP_DS_RAW],
        "DS_REPORTING_SFDC": [
            CFG_SFDC_DS_REPORTING, args.DS_REPORTING, CFG_SAP_DS_REPORTING,
            "REPORTING"
        ]
    }

    if CFG_DEPLOY_SAP or args.DEPLOY_SAP == "true":
        if rules["SQL_FLAVOUR"] == "union" and (
                rules["DS_CDC_ECC"] is None or rules["DS_CDC_S4"] is None or
                rules["DS_RAW_ECC"] is None or rules["DS_RAW_S4"] is None or
                rules["MANDT_ECC"] is None or rules["MANDT_S4"] is None):

            logging.error("ERROR: 🛑🔪 SQL_FLAVOUR=union requires "
                          "all parameters for both ECC and S4 🔪🛑")
            sys.exit(1)

    jinja_replacements = create_config(_CONFIG_TEMPLATE, _CONFIG_INPUT_FILE,
                                       rules)

    if not jinja_replacements:
        logging.error("FATAL PROGRAM ERROR: 🛑🔪 Jinja replacements could not "
                      "be generated.  🔪🛑")
        sys.exit(1)

    ## Overwrite config.json with validated configuration
    write_json_config(_CONFIG_JINJA, jinja_replacements, _CONFIG_INPUT_FILE)

    if CFG_DEPLOY_SFDC or args.DEPLOY_SFDC == "true":
        #TODO: add validation of mandatory parameters for each flavor
        write_json_config(_SFDC_CONFIG_JINJA, jinja_replacements,
                          _SFDC_CONFIG_OUTPUT)
        logging.info("Generated config for ✨Salesforce.com✨")

    if CFG_DEPLOY_SAP or args.DEPLOY_SAP == "true":

        write_json_config(_SAP_CONFIG_JINJA, jinja_replacements,
                          _SAP_CONFIG_OUTPUT)

        logging.info("Generated config for ✨SAP✨")

    #TODO: dotenv config will be replaced by json config in SAP modules
    # in next release
    jinja_replacements["LANGUAGE"] = str(
        jinja_replacements["LANGUAGE"]).replace('"', "'")
    jinja_replacements["LANGUAGE"] = str(
        jinja_replacements["LANGUAGE"]).replace('[', "")
    jinja_replacements["LANGUAGE"] = str(
        jinja_replacements["LANGUAGE"]).replace(']', "")
    jinja_replacements["CURRENCY"] = str(
        jinja_replacements["CURRENCY"]).replace('"', "'")
    jinja_replacements["CURRENCY"] = str(
        jinja_replacements["CURRENCY"]).replace('[', "")
    jinja_replacements["CURRENCY"] = str(
        jinja_replacements["CURRENCY"]).replace(']', "")

    env = Environment(loader=FileSystemLoader("./config/"))
    template = env.get_template("config_env.jinja")
    output_config_env = template.render(jinja_replacements)
    with open("config/config.env", mode="w",
              encoding="utf-8") as output_env_file:
        output_env_file.write(output_config_env)


if __name__ == "__main__":
    main()
