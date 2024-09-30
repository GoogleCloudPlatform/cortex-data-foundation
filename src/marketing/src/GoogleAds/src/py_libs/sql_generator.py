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
"""Functions for rendering Jinja CDC SQL templates and writing them to target
directory."""

import logging
from pathlib import Path
from typing import Any, Dict

from jinja2 import Environment
from jinja2 import FileSystemLoader

logger = logging.getLogger(__name__)


def render_template_file(template_path: Path, subs: Dict[str, Any]) -> str:
    """Renders template from given Jinja .sql template file.
.
    Args:
        template_path (Path): Path of the processed Jinja template.
        subs (Dict[str, Any]): Template variables.

    Returns:
        str: Rendered SQL script to write out.
    """

    logger.debug("Rendering Jinja template file: '%s' ", template_path)

    env = Environment(
        loader=FileSystemLoader(str(template_path.parent.absolute())))
    input_template = env.get_template(template_path.name)

    final_subs = {**subs}

    output_sql = input_template.render(final_subs)

    logger.debug("Generated SQL from template: \n%s", output_sql)

    return output_sql


def write_generated_sql_to_disk(path: Path, generated_sql: Any) -> None:
    """Writes generated SQL object to the given path."""
    with open(path, "w", encoding="utf-8") as f:
        f.write(generated_sql)
