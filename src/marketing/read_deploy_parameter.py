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
"""Reads deploy parameter for specified data provider from config.json."""

import json
import logging
import sys


def main():

    deploy_datasource = "deploy" + sys.argv[1]

    try:
        with open("config/config.json", encoding="utf-8") as cf:
            config = json.load(cf)
        print(config["marketing"][deploy_datasource])
    except FileNotFoundError:
        logging.error("config.json is not found")
        sys.exit(1)
    except KeyError as key:
        logging.error("%s parameter is not found in config.json", key)
        sys.exit(1)


if __name__ == "__main__":
    main()
