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
""" Library for Dag Generator"""

from string import Template
from pathlib import Path


def generate_file_from_template(template_file_path: Path,
                                output_file_path: Path, **subs: str):
    """Creates fully resolved file from template using substitutions.

    Args:
        template_file_path: Full template file path.
        output_file_path: Full output file path where resolved file will be
            created.
        subs: Substitutes to be applied to the template.
    """
    with open(template_file_path, mode="r", encoding="utf-8") as template_file:
        dag_template = Template(template_file.read())
    generated_code = dag_template.substitute(**subs)

    output_file_path.parent.mkdir(exist_ok=True, parents=True)
    with output_file_path.open(mode="w+", encoding="utf-8") as generated_file:
        generated_file.write(generated_code)
