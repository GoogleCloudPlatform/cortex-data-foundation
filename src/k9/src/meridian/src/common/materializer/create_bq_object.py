# Copyright 2024 Google LLC

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
Creates BQ object (table, view ) or executes a sql script based on the
input parameters.
"""

# We use errors from exceptions and surface them to logs when relevant.
# There is no need to raise from exceptions again. It can get confusing to
# our customers during deployment errors. Disabling those linting errors.
#pylint: disable=raise-missing-from

#TODO: Remove these. Only here to pass presubmit for intial changes.
#pylint: disable=redundant-returns-doc
#pylint: disable=unused-import

import argparse
import json
import logging
from pathlib import Path
import sys

from google.cloud.exceptions import BadRequest

from common.materializer import generate_assets
from common.py_libs import bq_helper
from common.py_libs import bq_materializer
from common.py_libs import cortex_bq_client
from common.py_libs import jinja

# NOTE: All paths here are relative to the root directory, unless specified
# otherwise.

# Directory where this file resides.
_THIS_DIR = Path(__file__).resolve().parent

# Directory containing various template files.
_TEMPLATE_DIR = Path(_THIS_DIR, "templates")


def _parse_args() -> tuple[str, str, str, str, dict, bool, bool, str, bool]:
    """Parses, validates and returns arguments, sets up logging."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--module_name",
        type=str,
        required=True,
        help="Module for which to generate BQ table/view. Required.")
    parser.add_argument(
        "--jinja_data_file",
        type=str,
        required=True,
        help=("Jinja data file containing replacement values for the sql_file "
              "file. settings file, with relative path. Required."))
    parser.add_argument(
        "--target_dataset_type",
        type=str,
        required=False,
        default="Reporting",
        help=("Type of dataset (CDC/Reporting) for which this table or view "
              " is created. Default value is 'Reporting'."))
    parser.add_argument(
        "--target_dataset",
        type=str,
        required=True,
        help=("Full name of BiQuery dataset in which this table or view will "
              "be created. Required."))
    parser.add_argument(
        "--bq_object_setting",
        type=str,
        required=True,
        help=("BQ Object Setting dictionary - containing value corresponding "
              "to the entry in the materializer settings file for the given "
              "table. Required."))
    parser.add_argument(
        "--load_test_data",
        default=False,
        action="store_true",
        help="Flag to indicate if test data should be loaded in the tables.")
    parser.add_argument(
        "--debug",
        default=False,
        action="store_true",
        help="Flag to set log level to DEBUG. Default is WARNING")
    parser.add_argument("--allow_telemetry",
                        default=False,
                        action="store_true",
                        help="Flag to indicate if telemetry is allowed.")
    parser.add_argument(
        "--location",
        type=str,
        required=True,
        help="Location to pass to BigQueryInsertJob operators in DAGs.")
    parser.add_argument(
        "--skip_dag",
        default=False,
        action="store_true",
        help="Flag to indicate if Composer DAG should not be generated.")

    args = parser.parse_args()

    enable_debug = args.debug
    logging.basicConfig(level=logging.DEBUG if enable_debug else logging.INFO)

    module_name = args.module_name
    jinja_data_file = args.jinja_data_file
    target_dataset_type = args.target_dataset_type.lower()
    target_dataset = args.target_dataset
    bq_object_setting_str = args.bq_object_setting
    load_test_data = args.load_test_data
    allow_telemetry = args.allow_telemetry
    location = args.location
    skip_dag = args.skip_dag

    logging.info("Arguments:")
    logging.info("  module_name = %s", module_name)
    logging.info("  jinja_data_file = %s", jinja_data_file)
    logging.info("  target_dataset_type = %s", target_dataset_type)
    logging.info("  target_dataset = %s", target_dataset)
    logging.info("  bq_object_setting_str = %s", bq_object_setting_str)
    logging.info("  load_test_data = %s", load_test_data)
    logging.info("  debug = %s", enable_debug)
    logging.info("  allow_telemetry = %s", allow_telemetry)
    logging.info("  location = %s", location)
    logging.info("  skip_dag = %s", skip_dag)

    if not Path(jinja_data_file).is_file():
        raise ValueError(
            f"🛑 jinja_data_file '{jinja_data_file}' does not exist.")

    try:
        bq_object_setting = json.loads(bq_object_setting_str)
    except Exception as e:
        raise ValueError(f"🛑 Failed to read table settings. Error = {e}.")

    return (module_name, jinja_data_file, target_dataset_type, target_dataset,
            bq_object_setting, load_test_data, allow_telemetry, location,
            skip_dag)


def main():

    # Parse and validate arguments.
    (module_name, jinja_data_file, target_dataset_type, target_dataset,
     bq_object_setting, load_test_data, allow_telemetry, location,
     skip_dag) = _parse_args()

    sql_file = bq_object_setting["sql_file"]
    if not Path(sql_file).is_file():
        raise ValueError(f"🛑 sql_file '{sql_file}' does not exist.")

    bq_client = cortex_bq_client.CortexBQClient()

    # Render core sql text from sql file after applying Jinja parameters.
    rendered_sql = jinja.apply_jinja_params_to_file(sql_file, jinja_data_file)
    logging.debug("Rendered SQL: %s", rendered_sql)
    # Validate core sql.
    generate_assets.validate_sql(bq_client, rendered_sql)

    object_type = bq_object_setting["type"]
    object_description = bq_object_setting.get("description")

    if object_type in ["table", "view"]:
        object_name = Path(sql_file).stem
        logging.info("Generating %s %s '%s'...", target_dataset_type,
                     object_type, object_name)
        object_name_full = target_dataset + "." + object_name

        if bq_helper.table_exists(bq_client, object_name_full):
            logging.info("%s %s '%s' already exists.", target_dataset_type,
                         object_type, object_name)
            # For non-reporting dataset types (e.g. cdc), if table or view
            # exists, we don't touch it.
            # NOTE: We can't generate DAG either, as for DAG generation, we
            # need table to be in place.
            if target_dataset_type != "reporting":
                logging.info("Skipping recreating %s.", object_type)
                sys.exit(0)
            # For "reporting" dataset type, we always create tables and views.
            # If reporting table or view exists, we need to drop it.
            else:
                logging.info("Dropping %s...", object_type)
                bq_client.delete_table(object_name_full)

        # Create view or table, based on object type.
        if object_type == "view":
            try:
                generate_assets.create_view(bq_client, object_name_full,
                                            object_description, rendered_sql)
            except BadRequest as e:
                if hasattr(e, "query_job") and e.query_job:  # type: ignore
                    query = e.query_job.query  # type: ignore
                    raise SystemExit(f"🛑 ERROR: Failed to create view. "
                                     f"Error = {e}. SQL: {query}") from e
                else:
                    raise SystemExit(f"🛑 ERROR: Failed to create view. "
                                     f"Error = {e}.") from e
            except Exception as e:
                raise SystemExit(f"ERROR: Failed to create view. Error = {e}.")
        else:
            try:
                table_setting = bq_object_setting["table_setting"]
                bq_materializer.validate_table_setting(table_setting)
                generate_assets.create_table(bq_client, object_name_full,
                                             object_description, rendered_sql,
                                             table_setting)
            except BadRequest as e:
                if hasattr(e, "query_job") and e.query_job:  # type: ignore
                    query = e.query_job.query  # type: ignore
                    raise SystemExit(f"🛑 ERROR: Failed to create table. "
                                     f"Error = {e}. SQL: {query}") from e
                else:
                    raise SystemExit(f"🛑 ERROR: Failed to create table. "
                                     f"Error = {e}.") from e
            except Exception as e:
                raise SystemExit(
                    f"🛑 ERROR: Failed to create table. Error = {e}.")

            table_refresh_sql = generate_assets.generate_table_refresh_sql(
                bq_client, object_name_full, rendered_sql)

            # If we create table, we may also need to generate DAG files unless
            # they are task dependent, which are generated by the parent
            # processes `generate_build_files`.
            if not skip_dag:
                generate_assets.generate_dag_files(
                    module_name, target_dataset_type, target_dataset,
                    object_name, table_setting, table_refresh_sql,
                    allow_telemetry, location, _TEMPLATE_DIR,
                    generate_assets.GENERATED_DAG_DIR_NAME)

            # If we create table, we also need to populate it with test data
            # if flag is set for that.
            if load_test_data:
                try:
                    logging.info("Populating table '%s' with test data...",
                                 object_name_full)
                    query_job = bq_client.query(table_refresh_sql)
                    # Wait for query to finish.
                    _ = query_job.result()
                except BadRequest as e:
                    if hasattr(e, "query_job") and e.query_job:  # type: ignore
                        query = e.query_job.query  # type: ignore
                        raise SystemExit(f"🛑 ERROR: Failed to load test data. "
                                         f"Error = {e}. SQL: {query}") from e
                    else:
                        raise SystemExit(f"🛑 ERROR: Failed to load test data. "
                                         f"Error = {e}.") from e
                except Exception as e:
                    raise SystemExit(
                        f"🛑 ERROR: Failed to load test data. Error = {e}.")

        logging.info("Generated %s %s '%s' successfully.", target_dataset_type,
                     object_type, object_name)

    # NOTE: For "script" type of object, we do not have a way to know if
    # underlying object (function or stored proc) already exists. Because of
    # this limitation, for non-reporting dataset types, we can't skip the step
    # of creating the object if it's already present. The script should check
    # for object existence if required.
    if object_type == "script":
        logging.info("Executing script '%s'...", sql_file)
        try:
            query_job = bq_client.query(query=rendered_sql)
            # Wait for query to finish.
            _ = query_job.result()
        except Exception as e:
            raise SystemExit("🛑 ERROR: Failed to run sql.\n"
                             "----\n"
                             f"SQL = \n{rendered_sql}\n"
                             "----\n"
                             f"Error = {e}")
        logging.info("Executed script '%s' successfully.", sql_file)


if __name__ == "__main__":
    main()
