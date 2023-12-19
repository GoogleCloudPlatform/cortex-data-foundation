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
"""Module to help format and clean YAML dumps."""

import enum
from typing import Any, Callable, Dict, Iterable, List, Type
import yaml


class YamlStrValue(str):
    """class that is used to identify yaml str values (not keys)."""


def _visit_fields(fn: Callable,
                  container: Any,
                  parent_container: Any = None) -> Any:
    """Visit all fields in the container and set values with the provided fn.

    This traverses the container using DFS. A container in this context is a
    heirarchical structure that contain nested dictionaries, lists, and scalars
    that can be traversed as a tree.
    """
    # Expand nested dicts.
    if isinstance(container, Dict):
        for key, val in list(container.items()):
            container[key] = _visit_fields(fn, val, container)
    # Expand list elements.
    elif isinstance(container, List):
        for i, val in enumerate(container):
            container[i] = _visit_fields(fn, val, container)
    # Base case to return the fn called with a leaf value.
    else:
        return fn(container, parent_container)
    return container


def cast_container_types(container: Any, source_type: Type,
                         target_type: Type) -> Any:
    """Returns container with leaf fields cast from source to target type."""
    return _visit_fields(
        (lambda x, _: target_type(x) if isinstance(x, source_type) else x),
        container)


def get_container_enums_as_names(container: Any) -> Any:
    """Returns container with enum leaf fields cast to name string."""
    return _visit_fields(
        (lambda x, _: x.name if isinstance(x, enum.Enum) else x),
        container)


def remove_null_fields(container: Iterable) -> None:
    """Remove null fields from the container.

    This modifies the container in place and returns None.
    Field values (included nested fields) that are considered null are:
        * None values
        * empty lists
    """
    # Expand nested dicts.
    if isinstance(container, Dict):
        for key, val in list(container.items()):
            if val is None:
                del container[key]
            elif val == []:
                del container[key]
            elif val == "":
                del container[key]
            else:
                remove_null_fields(val)
    # Expand list elements.
    elif isinstance(container, List):
        for _, val in enumerate(container):
            remove_null_fields(val)


def quoted_presenter(dumper, data):
    """YAML Presenter that can be used to wrap a value type in double quotes.

    Usage Example:
        dict = yaml_util.cast_container_types(dict, str, yaml_util.YamlStrValue)
        yaml.add_representer(yaml_util.YamlStrValue, yaml_util.quoted_presenter)
    """
    return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="\"")


class IndentedDumper(yaml.Dumper):
    """This dumper can be used to have list dashes after the indent.

    Default formatting:
    parent:
    -   child

    Formatting with IndentedDumper:
    parent:
        - child
    """

    def increase_indent(self, flow=False, indentless=False):
        return super().increase_indent(flow, False)
