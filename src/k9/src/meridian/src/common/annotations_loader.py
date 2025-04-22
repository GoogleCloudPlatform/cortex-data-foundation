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
"""
BigQuery Annotations loader
"""

import argparse
from concurrent import futures
import logging
import pathlib
import sys
import typing
import yaml

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

sys.path.append(".")
sys.path.append("./src")
sys.path.append(str(pathlib.Path(__file__).parent))

# pylint:disable=wrong-import-position
from common.py_libs.configs import load_config_file
from common.py_libs.jinja import (apply_jinja_params_dict_to_file,
                                  initialize_jinja_from_config)

_PARALLEL_THREADS = 5


def _load_table_annotations(table_annotations: typing.Dict[str, typing.Any],
                            client: bigquery.Client):
    full_table_id = table_annotations["id"]
    table_description = table_annotations["description"]
    description_changed = False
    schema_changed = False
    try:
        table = client.get_table(full_table_id)
    except NotFound:
        logging.info("Table or view `%s` was not found. Skipping it.",
                     full_table_id)
        return
    table_description = table.description or table_description
    description_changed = table_description != table.description
    table.description = table_description
    annotation_fields = {
        field_item["name"]: field_item["description"]
        for field_item in table_annotations["fields"]
    }

    schema = table.schema.copy()
    for index, field in enumerate(schema):
        description = field.description or annotation_fields.get(field.name, "")
        if description != field.description:
            schema_changed = True
            field_dict = field.to_api_repr()
            field_dict["description"] = description
            schema[index] = bigquery.SchemaField.from_api_repr(field_dict)
    changes = []
    if schema_changed:
        table.schema = schema
        changes.append("schema")
    if description_changed:
        changes.append("description")
    if len(changes) > 0:
        client.update_table(table, changes)
        logging.info("Table/view `%s` has been updated.", full_table_id)
    else:
        logging.info("No changes in `%s`.", full_table_id)


def load_annotations(jinja_dict: typing.Dict[str, typing.Any],
                     client: bigquery.Client, annotations_file: pathlib.Path):
    annotations_yaml = apply_jinja_params_dict_to_file(annotations_file,
                                                       jinja_dict)
    annotations_dict = yaml.safe_load(annotations_yaml)
    if not annotations_dict:
        logging.warning("Annotations file `%s` has no parsable content.",
                        str(annotations_file))
        return
    logging.info("Loading annotations from `%s`.", str(annotations_file))

    threads = []
    executor = futures.ThreadPoolExecutor(_PARALLEL_THREADS)

    for table_item in annotations_dict:
        threads.append(
            executor.submit(_load_table_annotations, table_item, client))
    futures.wait(threads)


def main(args: typing.Sequence[str]) -> int:
    """BigQuery Annotations loader main"""

    parser = argparse.ArgumentParser(description="BigQuery Annotations Loader")
    parser.add_argument("--annotations-directory",
                        help="Annotation files directory",
                        type=str,
                        required=True)
    parser.add_argument("--debug",
                        help="Debugging mode.",
                        action="store_true",
                        default=False,
                        required=False)
    parser.add_argument("--config",
                        help="Data Foundation config.json.",
                        type=str,
                        required=False,
                        default="./config/config.json")
    options = parser.parse_args(args)
    logging.basicConfig(
        format="%(asctime)s | %(levelname)s | %(message)s",
        level=logging.INFO if not options.debug else logging.DEBUG,
    )

    logging.info("Cortex Annotations Loader for BigQuery.")

    config = load_config_file(options.config)

    logging.info("Loading BigQuery Annotations.")

    annotations_path = pathlib.Path(options.annotations_directory)
    if not annotations_path.exists():
        logging.critical("Directory `%s` doesn't exist.", str(annotations_path))
        return 1

    client = bigquery.Client(project=config["projectIdSource"],
                             location=config["location"])
    jinja_dict = initialize_jinja_from_config(config)

    for annotation_file in annotations_path.iterdir():
        load_annotations(jinja_dict, client, annotation_file.absolute())
    logging.info("BigQuery Annotations has been loaded!")

    return 0


###############################################################
if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
