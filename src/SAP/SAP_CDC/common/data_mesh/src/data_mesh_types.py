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
"""Defines dataclasses used by the Cortex Data Mesh.

   Fields with defaults are optional, others are required.
"""
import dataclasses
import enum
from typing import Any, List, Union

from common.data_mesh.src import config_spec

# TODO: Consider using protos/pulling directly from internal GCP types.
# TODO: Should we default all fields to non-null?

# Catalog Tag classes.


# CatalogTagLevel is a Cortex construct and will be used to validate that
# catalog tags are being applied to assets as the template is intended. It i
# not a concept required or enforced by Data Catalog.
@enum.unique
class CatalogTagLevel(enum.Enum):
    UNKNOWN = enum.auto()
    ASSET = enum.auto()
    FIELD = enum.auto()
    ANY = enum.auto()


# Should match exactly:
# https://cloud.google.com/python/docs/reference/datacatalog/latest/google.cloud.datacatalog_v1.types.FieldType.PrimitiveType
@enum.unique
class PrimitiveType(enum.Enum):
    PRIMITIVE_TYPE_UNSPECIFIED = enum.auto()
    DOUBLE = enum.auto()
    STRING = enum.auto()
    BOOL = enum.auto()
    TIMESTAMP = enum.auto()
    RICHTEXT = enum.auto()


# This field name is slighly different because we remove a layer of nesting for
# usability.
# https://cloud.google.com/python/docs/reference/datacatalog/latest/google.cloud.datacatalog_v1.types.FieldType.EnumType
@dataclasses.dataclass
class EnumType(config_spec.ConfigSpec):
    enum_allowed_values: List[str]


# https://cloud.google.com/python/docs/reference/datacatalog/latest/google.cloud.datacatalog_v1.types.TagTemplateField
@dataclasses.dataclass
class CatalogTagTemplateField(config_spec.ConfigSpec):
    display_name: str
    field_type: Union[PrimitiveType, EnumType]
    is_required: bool = dataclasses.field(default=False)
    description: str = dataclasses.field(default="")


# https://cloud.google.com/python/docs/reference/datacatalog/latest/google.cloud.datacatalog_v1.types.TagField
@dataclasses.dataclass
class CatalogTagField(config_spec.ConfigSpec):
    display_name: str
    # Value type in YAML must match the associated CatalogTagTemplateField
    # primitive type or enum allowed value.
    value: Any


@dataclasses.dataclass
class CatalogTagTemplatePolicyFilter(config_spec.ConfigSpec):
    # Value must match an associated CatalogTagTemplateField.display_name.
    field_name: str
    # Value type in YAML must match the associated CatalogTagTemplateField
    # primitive type or enum allowed value.
    value: Any


@dataclasses.dataclass
class CatalogTagTemplateAssetPolicy(config_spec.ConfigSpec):
    policy: "BqAssetPolicy"
    filters: List[CatalogTagTemplatePolicyFilter] = dataclasses.field(
        default_factory=list)


@dataclasses.dataclass
class CatalogTagTemplateFieldPolicy(config_spec.ConfigSpec):
    policy_tag_id: "PolicyTagId"
    filters: List[CatalogTagTemplatePolicyFilter] = dataclasses.field(
        default_factory=list)


# https://cloud.google.com/python/docs/reference/datacatalog/latest/google.cloud.datacatalog_v1.types.TagTemplate
@dataclasses.dataclass
class CatalogTagTemplate(config_spec.ConfigSpec):
    display_name: str
    level: CatalogTagLevel = dataclasses.field(default=CatalogTagLevel.ANY)
    fields: List[CatalogTagTemplateField] = dataclasses.field(
        default_factory=list)
    asset_policies: List[CatalogTagTemplateAssetPolicy] = dataclasses.field(
        default_factory=list)
    field_policies: List[CatalogTagTemplateFieldPolicy] = dataclasses.field(
        default_factory=list)


# https://cloud.google.com/python/docs/reference/datacatalog/latest/google.cloud.datacatalog_v1.types.Tag
@dataclasses.dataclass
class CatalogTag(config_spec.ConfigSpec):
    display_name: str
    fields: List[CatalogTagField] = dataclasses.field(default_factory=list)


@dataclasses.dataclass
class CatalogTagTemplates(config_spec.ConfigSpec):
    project: str
    templates: List[CatalogTagTemplate]


# Policy Tag taxonomy classes.


# Should match exactly:
# https://cloud.google.com/bigquery/docs/reference/bigquerydatapolicy/rest/v1/projects.locations.dataPolicies#predefinedexpression
@enum.unique
class MaskingRule(enum.Enum):
    PREDEFINED_EXPRESSION_UNSPECIFIED = enum.auto()
    SHA256 = enum.auto()
    ALWAYS_NULL = enum.auto()
    DEFAULT_MASKING_VALUE = enum.auto()
    LAST_FOUR_CHARACTERS = enum.auto()
    FIRST_FOUR_CHARACTERS = enum.auto()
    EMAIL_MASK = enum.auto()
    DATE_YEAR_MASK = enum.auto()


# https://cloud.google.com/python/docs/reference/bigquerydatapolicy/latest/google.cloud.bigquery_datapolicies_v1.types.DataPolicy
@dataclasses.dataclass
class DataPolicy(config_spec.ConfigSpec):
    display_name: str
    masking_rule: MaskingRule
    masked_readers: List[str] = dataclasses.field(default_factory=list)


@dataclasses.dataclass
class PolicyTagId(config_spec.ConfigSpec):
    display_name: str
    taxonomy: str


# https://cloud.google.com/python/docs/reference/datacatalog/latest/google.cloud.datacatalog_v1.types.PolicyTag
@dataclasses.dataclass
class PolicyTag(config_spec.ConfigSpec):
    display_name: str
    description: str = dataclasses.field(default="")
    unmasked_readers: List[str] = dataclasses.field(default_factory=list)
    data_policies: List[DataPolicy] = dataclasses.field(default_factory=list)
    # TODO: consider handling cycles.
    child_policy_tags: List["PolicyTag"] = dataclasses.field(
        default_factory=list)


# https://cloud.google.com/python/docs/reference/datacatalog/latest/google.cloud.datacatalog_v1.types.Taxonomy
@dataclasses.dataclass
class PolicyTaxonomy(config_spec.ConfigSpec):
    display_name: str
    description: str = dataclasses.field(default="")
    policy_tags: List[PolicyTag] = dataclasses.field(default_factory=list)


@dataclasses.dataclass
class PolicyTaxonomies(config_spec.ConfigSpec):
    project: str
    taxonomies: List[PolicyTaxonomy] = dataclasses.field(default_factory=list)


# Data mesh classes.


@dataclasses.dataclass
class Label(config_spec.ConfigSpec):
    name: str
    value: str


# Should match exactly:
# https://cloud.google.com/dataplex/docs/reference/rpc/google.cloud.dataplex.v1#google.cloud.dataplex.v1.Asset.ResourceSpec.Type
@enum.unique
class AssetType(enum.Enum):
    TYPE_UNSPECIFIED = enum.auto()
    STORAGE_BUCKET = enum.auto()
    BIGQUERY_DATASET = enum.auto()


# https://cloud.google.com/python/docs/reference/dataplex/latest/google.cloud.dataplex_v1.types.Asset
@dataclasses.dataclass
class Asset(config_spec.ConfigSpec):
    display_name: str
    asset_name: str
    description: str = dataclasses.field(default="")
    labels: List[Label] = dataclasses.field(default_factory=list)
    asset_type: AssetType = dataclasses.field(
        default=AssetType.BIGQUERY_DATASET)


# Should match exactly:
# https://cloud.google.com/dataplex/docs/reference/rpc/google.cloud.dataplex.v1#type_5
@enum.unique
class ZoneType(enum.Enum):
    TYPE_UNSPECIFIED = enum.auto()
    RAW = enum.auto()
    CURATED = enum.auto()


# Should match exactly:
# https://cloud.google.com/dataplex/docs/reference/rpc/google.cloud.dataplex.v1#google.cloud.dataplex.v1.Zone.ResourceSpec.LocationType
@enum.unique
class ZoneLocationType(enum.Enum):
    LOCATION_TYPE_UNSPECIFIED = enum.auto()
    SINGLE_REGION = enum.auto()
    # Multi region is not supported in Asia.
    MULTI_REGION = enum.auto()


# https://cloud.google.com/python/docs/reference/dataplex/latest/google.cloud.dataplex_v1.types.Zone
@dataclasses.dataclass
class Zone(config_spec.ConfigSpec):
    display_name: str
    description: str = dataclasses.field(default="")
    zone_type: ZoneType = dataclasses.field(default=ZoneType.TYPE_UNSPECIFIED)
    labels: List[Label] = dataclasses.field(default_factory=list)
    location_type: ZoneLocationType = dataclasses.field(
        default=ZoneLocationType.LOCATION_TYPE_UNSPECIFIED)
    assets: List[Asset] = dataclasses.field(default_factory=list)


# https://cloud.google.com/python/docs/reference/dataplex/latest/google.cloud.dataplex_v1.types.Lake
@dataclasses.dataclass
class Lake(config_spec.ConfigSpec):
    display_name: str
    # If the project location is a region, this should be equivalent.
    # If the project location is multi-region, this should be a sub-region.
    region: str
    description: str = dataclasses.field(default="")
    labels: List[Label] = dataclasses.field(default_factory=list)
    zones: List[Zone] = dataclasses.field(default_factory=list)


@dataclasses.dataclass
class Lakes(config_spec.ConfigSpec):
    project: str
    lakes: List[Lake]


# TODO: Enable when there is API support.
# @dataclasses.dataclass
# class BusinessTerm(config_spec.ConfigSpec):
#     name: str
#     description: str

# BigQuery asset annotation classes.


@dataclasses.dataclass
class BqRowPolicy(config_spec.ConfigSpec):
    name: str
    readers: List[str]
    filter: str


@enum.unique
class BqAssetRole(enum.Enum):
    UNKNOWN = enum.auto()
    READER = enum.auto()
    WRITER = enum.auto()
    OWNER = enum.auto()


@dataclasses.dataclass
class BqAssetPolicy(config_spec.ConfigSpec):
    role: BqAssetRole
    principals: List[str]


@dataclasses.dataclass
class BqAssetFieldAnnotation(config_spec.ConfigSpec):
    name: str
    description: str = dataclasses.field(default="")
    policy_tag_id: PolicyTagId = dataclasses.field(default=None)  # type: ignore
    catalog_tags: List[CatalogTag] = dataclasses.field(default_factory=list)
    # TODO: Enable when there is API support.
    # business_terms: List[BusinessTerm] = dataclasses.field(
    #     default_factory=list)


# This top level dataclass is singular because we will only specify one SQL
# asset per YAML file. It doesn't map to a particular API object, but instead is
# used with several endpoints to register associations.
# TODO: potentially enable asset level zone assignment.
@dataclasses.dataclass
class BqAssetAnnotation(config_spec.ConfigSpec):
    # Name like my_project.my_dataset.my_table.
    name: str
    description: str = dataclasses.field(default="")
    catalog_tags: List[CatalogTag] = dataclasses.field(default_factory=list)
    fields: List[BqAssetFieldAnnotation] = dataclasses.field(
        default_factory=list)
    asset_policies: List[BqAssetPolicy] = dataclasses.field(
        default_factory=list)
    row_policies: List[BqRowPolicy] = dataclasses.field(default_factory=list)
