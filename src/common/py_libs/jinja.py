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
"""Library for Jinja related functions."""

from jinja2 import Environment, FileSystemLoader
import json
import logging

logger = logging.getLogger(__name__)


def apply_jinja_params_to_file(input_file: str, jinja_data_file: str) -> str:
    """Applies Jinja data file to the given file and returns rendered text.

    Args:
        input_file: File to be resolved with jinja parameters.
        jinja_data_file: File containing jinja parameters.

    Returns:
        Text from the input file after applying jinja parameters from the
        data file.
    """

    logger.debug("Applying jinja data file to '%s' file ...", input_file)

    with open(jinja_data_file, mode="r", encoding="utf-8") as jinja_f:
        jinja_data_dict = json.load(jinja_f)

    logger.debug("jinja_data_dict = %s", json.dumps(jinja_data_dict, indent=4))

    env = Environment(loader=FileSystemLoader("."))
    input_template = env.get_template(input_file)
    output_text = input_template.render(jinja_data_dict)
    logger.debug("Rendered text = \n%s", output_text)

    return output_text
