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
"""Defines a ConfigSpec base class that can be used to define yaml schemas.

Dataclasses can inherit from this base class to construct a series of classes
that store and codify the YAML schema. YAML files can be loaded into a
dictionary and passed to the `from_dict` constructor. See the unit test for
examples.
"""
# TODO: Consider using protos.
# TODO: Consider moving to common.
# Right now there's an import collision with py_libs.logging
import dataclasses
from exceptiongroup import ExceptionGroup
import enum
import logging
import typing
from typing import Any, Dict, List, Type, TypeVar, Union

from common.py_libs import cortex_exceptions as cortex_exc


def _get_value_as_type(value: Any, field_type: Type[object]) -> Any:
    """Returns the given value as the specified field_type."""
    if value is None:
        return None

    is_builtin = field_type.__module__ == "builtins"
    is_enum = field_type.__class__ == type(enum.Enum)

    if is_builtin or field_type == Any:  # pylint: disable=comparison-with-callable
        return value
    elif isinstance(value, field_type):
        return value
    elif is_enum:
        return field_type[value]  # type: ignore
    # Recursively expands nested types within field_type.
    else:
        return field_type.from_dict(value)  # type: ignore


def _get_value_as_types(value: Any, field_types: List[Type[object]]) -> Any:
    """Returns the given value as the first matching field_type."""
    exceptions = []
    for field_type in field_types:
        try:
            return _get_value_as_type(value, field_type)
        # Re-raising exception in group.
        except Exception as e:  # pylint: disable=broad-except
            exceptions.append(e)

    raise ExceptionGroup(
        f"Couldn't get value as any of the annotated types: {field_types}.",
        exceptions)


def _unwrap_field_type(field_type: Type[object]) -> List[Type[object]]:
    """Returns a list of potential field types wrapped by List and Union. """
    logging.debug("Unwrapping field_type: %s", field_type)
    is_list = typing.get_origin(field_type) == list
    is_union = typing.get_origin(field_type) == Union  # pylint: disable=comparison-with-callable

    # For lists, recursively unwrap the element type.
    if is_list:
        if not typing.get_args(field_type):
            raise cortex_exc.TypeCError(
                "Lists must define an element datatype.")

        field_type = typing.get_args(field_type)[0]
        return _unwrap_field_type(field_type)

    # For unions, recursively unwrap all potential types.
    all_field_types = []
    if is_union:
        field_types = typing.get_args(field_type)
        for ft in field_types:
            all_field_types.extend(_unwrap_field_type(ft))
    # For single types, still return in a list.
    else:
        all_field_types.append(field_type)

    logging.debug("potential field_types: %s", all_field_types)
    return all_field_types


def _get_name(d: Dict[str, Any]) -> str:
    name = d.get("name")
    display_name = d.get("display_name")
    if name:
        return name
    if display_name:
        return display_name

    raise cortex_exc.KeyCError(
        f"Dict doesn't contain a 'name' or 'display_name' field:\n{d}")


T = TypeVar("T", bound="ConfigSpec")


@dataclasses.dataclass
class ConfigSpec:
    """Base class that can be used to define yaml schemas.

    Dataclasses can inherit from this base class to construct a series of
    classes that store and codify the YAML schema. YAML files can be loaded into
    a dictionary and passed to the `from_dict` constructor.
    """

    @classmethod
    def from_dict(cls: Type[T], config: dict) -> T:
        """Constructor that expects a dict loaded from a YAML file."""
        logging.debug("Class: %s\n", cls.__name__)

        # Retrieve the field type hints so that we can dynamically create
        # appropriate nested types. An alternative way to retrieve field types
        # would be to use dataclasses.fields(cls), however that approach doesn't
        # resolve ForwardRefs for types that reference themselves.
        field_types = typing.get_type_hints(cls)

        attrs = {}
        for key, value in config.items():
            logging.debug("Key: %s", key)
            logging.debug("Value: %s", value)
            if key not in field_types:
                raise cortex_exc.KeyCError(
                    f"{cls.__name__}.{key} is undefined or "
                    "missing type annotations.")

            field_type = field_types[key]
            logging.debug("field_type: %s", field_type)

            possible_field_types = _unwrap_field_type(field_type)
            if isinstance(value, list):
                attrs[key] = [
                    _get_value_as_types(v, possible_field_types) for v in value
                ]
            else:
                attrs[key] = _get_value_as_types(value, possible_field_types)

        return cls(**attrs)

    @classmethod
    def merge_from_dict(cls: Type[T], a_dict: Dict[str, Any], b_dict: Dict[str,
                                                                           Any],
                        type_hints: Dict[str, Any]) -> Dict[str, Any]:
        """Returns a merged ConfigSpec represented as dictionaries.

        Both inputs and output are ConfigSpecs represented as dictionaries.
        Values from `a` will take precedence over `b` where they differ and are
        valid. For Lists, if the elements have a common `name` or `display_name`
        they will be considered the same element and will be merged.
        """
        for key, a_value in a_dict.items():
            if key not in b_dict:
                b_dict[key] = a_value
                continue

            # Ignore invalid values.
            if not a_value:
                continue

            field_type = type_hints[key]

            # For single values.
            if not isinstance(a_value, list):
                if issubclass(field_type, ConfigSpec):
                    item_type_hints = typing.get_type_hints(field_type)
                    nested_configspec = field_type.merge_from_dict(
                        a_value, b_dict[key], item_type_hints)
                    b_dict[key] = nested_configspec
                else:
                    b_dict[key] = a_value
                continue

            # Get element field types for lists.
            element_field_types = _unwrap_field_type(field_type)
            if len(element_field_types) == 0:
                raise cortex_exc.TypeCError(
                    f"Unable to get nested types for {field_type}")
            if len(element_field_types) > 1:
                raise cortex_exc.NotImplementedCError(
                    "ConfigSpec merge with union field types isn't supported.")
            element_field_type = element_field_types[0]

            # For valid non-ConfigSpec lists, a overwrites b.
            if not issubclass(element_field_type, ConfigSpec):
                b_dict[key] = a_value
                continue

            # For repeated ConfigSpecs:
            if issubclass(element_field_type, ConfigSpec):
                element_type_hints = typing.get_type_hints(element_field_type)

                # If the ConfigSpec doesn't have a name field, behaves the same
                # as non-ConfigSpec lists, where a just overwrites b.
                try:
                    _ = _get_name(element_type_hints)
                except cortex_exc.KeyCError:
                    b_dict[key] = a_value
                    continue

                # For named repeated ConfigSpecs, merge elements with same
                # name/display_name.
                b_element_map = {_get_name(e): e for e in b_dict[key]}
                for a_element in a_value:
                    name = _get_name(a_element)
                    if name in b_element_map:
                        merged_element = element_field_type.merge_from_dict(
                            a_element, b_element_map[name], element_type_hints)
                        b_element_map[name] = merged_element
                    # Add elements with new name/display_name.
                    else:
                        b_element_map[name] = a_element

                # Make sure the final list is ordered to be deterministic.
                ordered_elements = [v for k, v in sorted(b_element_map.items())]
                b_dict[key] = ordered_elements
                continue

            raise cortex_exc.TypeCError(
                f"Unexpected field type '{a_value.__class__.__name__}' while "
                f"merging instances of '{cls.__name__}'")

        return b_dict

    @classmethod
    def merge(cls: Type[T], a: T, b: T) -> T:
        """Returns a merged ConfigSpec from the two inputs.

        Values from `a` will take precedence over `b` where they differ and are
        valid. For Lists, if the elements have a common `name` or `display_name`
        they will be considered the same element and will be merged.
        """
        merge_dict = cls.merge_from_dict(dataclasses.asdict(a),
                                         dataclasses.asdict(b),
                                         typing.get_type_hints(cls))
        return cls.from_dict(merge_dict)
