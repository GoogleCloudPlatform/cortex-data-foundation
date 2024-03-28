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
"""Common types applicable to all of the Cortex Data Foundation."""

import dataclasses
import enum


@enum.unique
class Workload(enum.Enum):
    UNKNOWN = enum.auto()
    SAP_ECC = enum.auto()
    SAP_S4 = enum.auto()
    SFDC = enum.auto()
    MARKETING_CM360 = enum.auto()
    MARKETING_GA = enum.auto()
    MARKETING_TIKTOK = enum.auto()
    MARKETING_META = enum.auto()
    MARKETING_SFMC = enum.auto()

class DataLayer(enum.Enum):
    UNKNOWN = enum.auto()
    RAW = enum.auto()
    CDC = enum.auto()
    REPORTING = enum.auto()

@dataclasses.dataclass(frozen=True)
class Dataset:
    workload: Workload
    layer: DataLayer
