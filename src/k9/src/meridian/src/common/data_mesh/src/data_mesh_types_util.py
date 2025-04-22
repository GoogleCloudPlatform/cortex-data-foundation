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
"""Defines utilities for Cortex Data Mesh types."""
import datetime
import logging
from typing import Any, List, Optional, Union

from google.cloud import bigquery_datapolicies_v1
from google.cloud import datacatalog_v1
from google.cloud import dataplex_v1
from google.protobuf import timestamp_pb2

from common.data_mesh.src import data_mesh_types
from common.py_libs import cortex_exceptions as cortex_exc

# Request type aliases
SearchCatalogResult = datacatalog_v1.SearchCatalogResult
CatalogTagTemplate = datacatalog_v1.TagTemplate
CatalogTagTemplateField = datacatalog_v1.TagTemplateField
CatalogTagTemplateFieldType = datacatalog_v1.FieldType
CatalogTagTemplateFieldTypePrimitive = datacatalog_v1.FieldType.PrimitiveType
CatalogTagTemplateFieldTypeEnumType = datacatalog_v1.FieldType.EnumType
CatalogTagTemplateFieldTypeEnumVal = datacatalog_v1.FieldType.EnumType.EnumValue

CatalogTag = datacatalog_v1.Tag
CatalogTagField = datacatalog_v1.TagField
CatalogTagFieldEnumValue = datacatalog_v1.TagField.EnumValue

BqDataPolicy = bigquery_datapolicies_v1.DataPolicy
BqDataPolicyType = BqDataPolicy.DataPolicyType
BqDataMaskingPolicy = bigquery_datapolicies_v1.DataMaskingPolicy
BqPredefinedDataMaskingPolicy = BqDataMaskingPolicy.PredefinedExpression

PolicyTaxonomy = datacatalog_v1.Taxonomy
PolicyTag = datacatalog_v1.PolicyTag
PolicyType = PolicyTaxonomy.PolicyType

AssetResourceSpec = dataplex_v1.Asset.ResourceSpec
AssetResourceSpecType = dataplex_v1.Asset.ResourceSpec.Type
Asset = dataplex_v1.Asset
Zone = dataplex_v1.Zone
ZoneType = Zone.Type
ZoneLocationType = Zone.ResourceSpec.LocationType
Lake = dataplex_v1.Lake

# Types that have a field fulfilling the purpose of display_name but aren't
# using that name.
_ATYPICAL_DISPLAY_NAMES = {
    BqDataPolicy: "data_policy_id",
    CatalogTag: "template_display_name"
}

# Types that have a field fulfilling the purpose of unique name but aren't
# using "name".
_ATYPICAL_UNIQUE_NAMES = {SearchCatalogResult: "relative_resource_name"}

# Supported string formats for datetimes.
_DATETIME_FORMATS = ("%Y-%m-%d %H:%M:%S%z", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d")

# TODO: Establish way to do logic validations.
# Do Cortex Data Mesh validations, don't re-validate things that should be
# enforced by the APIs.

# TODO: have _get_*_as_request_type auto set fields that have matching names.


def _parse_datetime(dt_str: str) -> datetime.datetime:
    for fmt in _DATETIME_FORMATS:
        try:
            return datetime.datetime.strptime(dt_str, fmt)
        except ValueError:
            pass

    raise cortex_exc.CriticalError(f"No valid date format found for: {dt_str}")


def _get_catalog_tag_field_as_request_type(
        spec: data_mesh_types.CatalogTagField,
        field_attr: str) -> CatalogTagField:
    tag_field = CatalogTagField()
    value = None
    if field_attr == "enum_value":
        value = CatalogTagFieldEnumValue(display_name=spec.value)
    elif field_attr == "timestamp_value":
        ts = timestamp_pb2.Timestamp()
        ts.FromDatetime(_parse_datetime(spec.value))
        value = ts
    else:
        value = spec.value
    setattr(tag_field, field_attr, value)
    return tag_field


def _get_catalog_tag_as_request_type(
        tag_spec: data_mesh_types.CatalogTag,
        template_spec: data_mesh_types.CatalogTagTemplate,
        template: str,
        column: Optional[str] = None) -> CatalogTag:

    field_attr_map = {}
    for field in template_spec.fields:
        if isinstance(field.field_type, data_mesh_types.PrimitiveType):
            field_attr_map[
                field.display_name] = f"{field.field_type.name}_value".lower()
        else:
            field_attr_map[field.display_name] = "enum_value"

    fields = {}
    for field_spec in tag_spec.fields:
        field_attr = field_attr_map[field_spec.display_name]
        fields[field_spec.display_name] = (
            _get_catalog_tag_field_as_request_type(field_spec, field_attr))

    return CatalogTag(template=template, column=column, fields=fields)


def _get_catalog_tag_template_field_type_as_request_type(
        spec: Union[data_mesh_types.PrimitiveType, data_mesh_types.EnumType]):
    if isinstance(spec, data_mesh_types.PrimitiveType):
        return CatalogTagTemplateFieldType(
            primitive_type=CatalogTagTemplateFieldTypePrimitive[spec.name])
    elif isinstance(spec, data_mesh_types.EnumType):
        return CatalogTagTemplateFieldType(
            enum_type=CatalogTagTemplateFieldTypeEnumType(allowed_values=[
                CatalogTagTemplateFieldTypeEnumVal(display_name=name)
                for name in spec.enum_allowed_values
            ]))
    else:
        raise cortex_exc.TypeCError(
            f"Unsupported field type: {type(spec).__name__}")


def _get_catalog_tag_template_field_as_request_type(
        spec: data_mesh_types.CatalogTagTemplateField):
    return CatalogTagTemplateField(
        display_name=spec.display_name,
        type_=_get_catalog_tag_template_field_type_as_request_type(
            spec.field_type),
        is_required=spec.is_required,
        description=spec.description)


def _get_catalog_tag_template_as_request_type(
        spec: data_mesh_types.CatalogTagTemplate,
        is_publicly_readable: bool = True):
    return CatalogTagTemplate(
        display_name=spec.display_name,
        is_publicly_readable=is_publicly_readable,
        fields={
            field.display_name:
                _get_catalog_tag_template_field_as_request_type(field)
            for field in spec.fields
        })


# Policy Tag taxonomy methods.
def _get_masking_rule_as_request_type(spec: data_mesh_types.MaskingRule):
    return BqDataMaskingPolicy(
        predefined_expression=BqPredefinedDataMaskingPolicy[spec.name])


def _get_data_policy_spec_as_request_type(
        spec: data_mesh_types.DataPolicy,
        parent_policy: str,
        data_policy_type=BqDataPolicyType.DATA_MASKING_POLICY) -> BqDataPolicy:
    # Data policy is inconsistent with the other types and uses data_policy_id
    # as both a display name and unique name.
    return BqDataPolicy(data_policy_id=spec.display_name,
                        data_policy_type=data_policy_type,
                        policy_tag=parent_policy,
                        data_masking_policy=_get_masking_rule_as_request_type(
                            spec.masking_rule))


def _get_policy_tag_spec_as_request_type(
        spec: data_mesh_types.PolicyTag,
        parent_policy: str = "") -> datacatalog_v1.PolicyTag:
    return datacatalog_v1.PolicyTag(display_name=spec.display_name,
                                    description=spec.description,
                                    parent_policy_tag=parent_policy)


def _get_policy_taxonomy_spec_as_request_type(
    spec: data_mesh_types.PolicyTaxonomy,
    activated_policy_types: Optional[List[PolicyType]] = None
) -> PolicyTaxonomy:
    if not activated_policy_types:
        activated_policy_types = [PolicyType.FINE_GRAINED_ACCESS_CONTROL]

    return PolicyTaxonomy(display_name=spec.display_name,
                          description=spec.description,
                          activated_policy_types=activated_policy_types)


def _get_asset_spec_as_request_type(spec: data_mesh_types.Asset,
                                    resource_name: str) -> Asset:
    labels = {label.name: label.value for label in spec.labels}
    asset_resource_spec = AssetResourceSpec(
        name=resource_name, type_=AssetResourceSpecType.BIGQUERY_DATASET)
    return Asset(display_name=spec.display_name,
                 labels=labels,
                 description=spec.description,
                 resource_spec=asset_resource_spec)


def _get_zone_location_type_as_request_type(
        spec: data_mesh_types.ZoneLocationType) -> Zone.ResourceSpec:
    return Zone.ResourceSpec(location_type=ZoneLocationType[spec.name])


def _get_zone_spec_as_request_type(spec: data_mesh_types.Zone) -> Zone:
    labels = {label.name: label.value for label in spec.labels}
    return Zone(display_name=spec.display_name,
                labels=labels,
                description=spec.description,
                type_=ZoneType[spec.zone_type.name],
                resource_spec=_get_zone_location_type_as_request_type(
                    spec.location_type))


def _get_lake_spec_as_request_type(spec: data_mesh_types.Lake) -> Lake:

    labels = {label.name: label.value for label in spec.labels}
    return Lake(display_name=spec.display_name,
                labels=labels,
                description=spec.description)


def get_request_type(spec: Any, **kwargs) -> Any:
    """Returns the request type associated with the given ConfigSpec.

    Args:
        spec: A data_mesh_types dataclass associated with an API request.
        kwargs: Kwargs that are needed to construct specific request types.
            CatalogTagTemplate: is_publicly_readable (optional)
            CatlogTag:
                template_spec: Spec for the associated template definition.
                template: String representing the full template resource name.
                column (optional): String representing a column being attached.
            DataPolicy: parent_policy, data_policy_type (optional)
            PolicyTag: parent_policy (optional)
            PolicyTaxonomy: activated_policy_types (optional)
            Asset: resource_name
            Zone: none
            Lake: none
    """
    try:
        if isinstance(spec, data_mesh_types.CatalogTagTemplate):
            return _get_catalog_tag_template_as_request_type(spec, **kwargs)
        if isinstance(spec, data_mesh_types.CatalogTag):
            return _get_catalog_tag_as_request_type(spec, **kwargs)
        if isinstance(spec, data_mesh_types.DataPolicy):
            return _get_data_policy_spec_as_request_type(spec, **kwargs)
        if isinstance(spec, data_mesh_types.PolicyTag):
            return _get_policy_tag_spec_as_request_type(spec, **kwargs)
        if isinstance(spec, data_mesh_types.PolicyTaxonomy):
            return _get_policy_taxonomy_spec_as_request_type(spec, **kwargs)
        if isinstance(spec, data_mesh_types.Asset):
            return _get_asset_spec_as_request_type(spec, **kwargs)
        if isinstance(spec, data_mesh_types.Zone):
            return _get_zone_spec_as_request_type(spec)
        if isinstance(spec, data_mesh_types.Lake):
            return _get_lake_spec_as_request_type(spec)
    except:
        logging.error("Unable to get request type for spec:\n%s", spec)
        raise

    raise cortex_exc.NotImplementedCError(f"Type: {type(spec)}")


def get_display_name(resource: Any) -> str:
    atypical_display_name_field = _ATYPICAL_DISPLAY_NAMES.get(type(resource))
    if atypical_display_name_field:
        return getattr(resource, atypical_display_name_field)

    return resource.display_name


def get_unique_name(resource: Any) -> str:
    atypical_unique_name_field = _ATYPICAL_UNIQUE_NAMES.get(type(resource))
    if atypical_unique_name_field:
        return getattr(resource, atypical_unique_name_field)

    return resource.name
