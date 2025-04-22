# Copyright 2025 Google LLC
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
"""Defines types used by the DAG definitions in reporting settings files."""
import dataclasses
import enum
from typing import Optional

from common.py_libs import config_spec


@enum.unique
class BqObjectType(enum.Enum):
    BQ_OBJECT_TYPE_UNSPECIFIED = enum.auto()
    VIEW = enum.auto()
    TABLE = enum.auto()
    SCRIPT = enum.auto()
    K9_DAWG = enum.auto()


# TODO: Consider refactoring partition_type and time_grain into enums.
@dataclasses.dataclass
class Partition(config_spec.ConfigSpec):
    column: str
    partition_type: str
    time_grain: str


@dataclasses.dataclass
class Cluster(config_spec.ConfigSpec):
    columns: list[str]


@dataclasses.dataclass
class Dag(config_spec.ConfigSpec):
    name: str
    # This list specifies that the node should be run after all of the parents
    # are completed.
    # TODO: Consider supporting more advanced control flow.
    # E.g. waiting for any parent to complete.
    parents: Optional[list[str]] = dataclasses.field(default=None)


@dataclasses.dataclass
class Table(config_spec.ConfigSpec):
    """Defines table object settings.

    Use dag_setting to connect multiple tables together with task dependencies
    in a single composer DAG.
    If load_frequency is set, dag_setting.name can be specified to create a
    top level node, but dag_setting.parents must not be set.
    If load_frequency is not set, then dag_setting.parents must be specified.
    """
    load_frequency: Optional[str] = dataclasses.field(default=None)
    dag_setting: Optional[Dag] = dataclasses.field(default=None)
    partition_details: Optional[Partition] = dataclasses.field(default=None)
    cluster_details: Optional[Cluster] = dataclasses.field(default=None)


@dataclasses.dataclass
class BqObject(config_spec.ConfigSpec):
    type: BqObjectType
    description: Optional[str] = dataclasses.field(default=None)
    sql_file: Optional[str] = dataclasses.field(default=None)
    table_setting: Optional[Table] = dataclasses.field(default=None)
    k9_id: Optional[str] = dataclasses.field(default=None)


@dataclasses.dataclass
class ReportingObjects(config_spec.ConfigSpec):
    bq_independent_objects: Optional[list[BqObject]] = dataclasses.field(
        default=None)
    bq_dependent_objects: Optional[list[BqObject]] = dataclasses.field(
        default=None)
