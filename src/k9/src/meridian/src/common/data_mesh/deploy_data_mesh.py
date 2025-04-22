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
"""Deploys the Cortex Data Mesh.

Call this utility with a config.json file and lists of directories containing
metadata or annotation specifications. If specifying annotations that use
catalog tags, then the associated catalog tag template specs must also be
provided.

The overwrite flag can be set to overwrite any existing metadata resources or
annotations. If the overwrite flag is not set, then existing resources will not
be re-created, however newly defined nested sub-resources will be patched in.

Usage examples:
  $ python deploy_data_mesh.py \
        --config-file <path/to/config.json \
        --tag-template-directories \
            <path/to/tag/templates> \
        --policy-directories \
            <path/to/policy/taxonomies> \
        --lake-directories \
            <path/to/lakes>
        --annotation-directories \
            <path/to/annotations> \
        --overwrite
"""

import argparse
import asyncio
import dataclasses
import logging
import pathlib
import sys
from typing import Any, Dict, List, Sequence, Type, TypeVar
import yaml

# Add cortex /src to the sys path.
sys.path.append(str(pathlib.Path(__file__).parents[2]))

# pylint: disable=wrong-import-position
from common.data_mesh.src import data_mesh_client
from common.data_mesh.src import data_mesh_types as dmt
from common.py_libs import config_spec
from common.py_libs import configs
from common.py_libs import cortex_exceptions as cortex_exc
from common.py_libs import jinja

# pylint: enable=wrong-import-position

# TODO: If running on cloud build, integrate logging.
# https://cloud.google.com/logging/docs/setup/python

T = TypeVar("T")


# TODO: consider enabling overwrite flag in deployment config options.
# Right now its only accesible through calling this util directly.
@dataclasses.dataclass
class DeploymentOptions(config_spec.ConfigSpec):
    # These names don't follow style to conform with Cortex config norms.
    # pylint: disable=invalid-name
    deployDescriptions: bool = True
    deployLakes: bool = True
    deployCatalog: bool = True
    deployACLs: bool = True
    # pylint: enable=invalid-name


def _specs_from_directory(directory: str, spec_type: Type[T],
                          jinja_dict: Dict[str, Any]) -> List[T]:

    logging.info("Loading specs in directory: %s.", directory)

    dir_path = pathlib.Path(directory)
    if not dir_path.exists():
        logging.warning("The following spec directory doesn't exist: %s.",
                        directory)
        return []

    # Load all YAML files from a directory into a single list of specs.
    specs = []
    for file_path in dir_path.iterdir():
        if file_path.suffix != ".yaml":
            logging.info("Ignoring non-YAML file: '%s'", file_path)
            continue

        # TODO: catch cases where config isnt set properly.
        # Can result in missing jinja dict keys. Alternatively, set the jinja
        # dict vars regardless of config options.
        try:
            final_yaml = jinja.apply_jinja_params_dict_to_file(
                file_path, jinja_dict)
            spec_dict = yaml.safe_load(final_yaml)
            specs.append(spec_type.from_dict(spec_dict))  # type: ignore
        except Exception as e:
            raise cortex_exc.CriticalError(
                f"Error while attempting to load {file_path}.") from e

    return specs


def main(args: Sequence[str]) -> int:
    parser = argparse.ArgumentParser(description="Cortex Data Mesh Deployer")
    parser.add_argument("--config-file",
                        type=str,
                        required=True,
                        help="Path to Cortex config.json file.")
    parser.add_argument("--tag-template-directories",
                        nargs="*",
                        default=[],
                        required=False,
                        help="A space delimited list of directories to find "
                        "CatalogTagTemplates specs to deploy.")
    parser.add_argument("--policy-directories",
                        nargs="*",
                        default=[],
                        required=False,
                        help="A space delimited list of directories to find "
                        "PolicyTaxonomies specs to deploy.")
    parser.add_argument("--lake-directories",
                        nargs="*",
                        default=[],
                        required=False,
                        help="A space delimited list of directories to find "
                        "Lakes specs to deploy.")
    parser.add_argument("--annotation-directories",
                        nargs="*",
                        default=[],
                        required=False,
                        help="A space delimited list of directories to find "
                        "BqAssetAnnotation specs to deploy.")
    # TODO: Consider if skip flags are still useful despite deployment opts.
    parser.add_argument("--skip-resources",
                        action="store_true",
                        default=False,
                        required=False,
                        help="Skip deployment of metadata resources. "
                        "Metadata resources should already exist if deploying "
                        "annotations. Tag template directories used in "
                        "annotations must still be provided, despite not being "
                        "re-created")
    parser.add_argument("--skip-annotations",
                        action="store_true",
                        default=False,
                        required=False,
                        help="Skip deployment of asset annotations.")
    parser.add_argument("--overwrite",
                        action="store_true",
                        default=False,
                        required=False,
                        help="Overwrite conflicting existing resources.")
    parser.add_argument("--debug",
                        action="store_true",
                        default=False,
                        required=False)
    params = parser.parse_args(args)

    logging.basicConfig(format="%(asctime)s | %(levelname)s | %(message)s",
                        level=logging.DEBUG if params.debug else logging.INFO)

    config_file = str(pathlib.Path(params.config_file).absolute())
    config = configs.load_config_file(config_file)
    jinja_dict = jinja.initialize_jinja_from_config(config)

    deployment_options_config = config.get("DataMesh")
    if not deployment_options_config:
        raise cortex_exc.CriticalError(
            "DataMesh deployment options not specified in config.")

    deployment_options = DeploymentOptions.from_dict(deployment_options_config)
    logging.info("🔷️ Deploying Data Mesh with the following options: %s",
                 deployment_options)

    client = data_mesh_client.CortexDataMeshClient(config["location"],
                                                   params.overwrite)

    # These resources are parsed regardless of the deployment options in case
    # they are referenced by other specs.
    tag_templates = []
    for directory in params.tag_template_directories:
        tag_templates.extend(
            _specs_from_directory(directory, dmt.CatalogTagTemplates,
                                  jinja_dict))

    policy_taxonomies = []
    for directory in params.policy_directories:
        policy_taxonomies.extend(
            _specs_from_directory(directory, dmt.PolicyTaxonomies, jinja_dict))

    lakes = []
    for directory in params.lake_directories:
        lakes.extend(_specs_from_directory(directory, dmt.Lakes, jinja_dict))

    if not params.skip_resources:
        deployed_resources = {}
        if deployment_options.deployCatalog:
            deployed_resources["tag_templates_specs"] = tag_templates

        if deployment_options.deployACLs:
            deployed_resources["policy_taxonomies_specs"] = policy_taxonomies

        if deployment_options.deployLakes:
            deployed_resources["lakes_specs"] = lakes

        asyncio.run(
            client.create_metadata_resources_async(**deployed_resources))

    if not params.skip_annotations:
        for directory in params.annotation_directories:
            annotations = _specs_from_directory(directory,
                                                dmt.BqAssetAnnotation,
                                                jinja_dict)
            asyncio.run(
                client.annotate_bq_assets_async(
                    annotations,
                    tag_templates,
                    deploy_descriptions=deployment_options.deployDescriptions,
                    deploy_catalog=deployment_options.deployCatalog,
                    deploy_acls=deployment_options.deployACLs))

    logging.info("Data mesh successfully deployed!")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
