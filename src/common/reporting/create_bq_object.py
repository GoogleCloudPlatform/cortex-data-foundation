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
Creates BQ object (table, view ) or executes a sql script based on the
input parameters.
"""

# We use errors from exceptions and surface them to logs when relevant.
# There is no need to raise from exceptions again. It can get confusing to
# our customers during deployment errors. Disabling those linting errors.
#pylint: disable=raise-missing-from

import argparse
import datetime
import json
import logging
import textwrap
import sys

from pathlib import Path

from common.py_libs import bq_helper
from common.py_libs import bq_materializer
from common.py_libs import jinja
from common.py_libs.dag_generator import generate_file_from_template

from google.cloud import bigquery

# NOTE: All paths here are relative to the root directory, unless specified
# otherwise.

# Directory this file is executed from.
_CWD = Path.cwd()

# Directory where this file resides.
_THIS_DIR = Path(__file__).resolve().parent

# Directory containing various template files.
_TEMPLATE_DIR = Path(_THIS_DIR, "templates")

# Directories where generated dag files and related files are created.
_GENERATED_DAG_PY_DIR = Path(_CWD, "generated_reporting_dag_py_files")
_GENERATED_DAG_SQL_DIR = Path(_CWD, "generated_reporting_dag_sql_files")


def _parse_args() -> tuple[str, str, str, dict, bool]:
    """Parses, validates and returns arguments, sets up logging."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--module_name",
        type=str,
        required=True,
        help="Module for which to generate reporting table/view. Required.")
    parser.add_argument(
        "--jinja_data_file",
        type=str,
        required=True,
        help=("Jinja data file containing replacement values for the sql_file "
              "file. settings file, with relative path. Required."))
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
              "to the entry in the reporting config file for the given table. "
              "Required."))
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

    args = parser.parse_args()

    enable_debug = args.debug
    logging.basicConfig(level=logging.DEBUG if enable_debug else logging.INFO)

    module_name = args.module_name
    jinja_data_file = args.jinja_data_file
    target_dataset = args.target_dataset
    bq_object_setting_str = args.bq_object_setting
    load_test_data = args.load_test_data

    logging.info("Arguments:")
    logging.info("  module_name = %s", module_name)
    logging.info("  jinja_data_file = %s", jinja_data_file)
    logging.info("  target_dataset = %s", target_dataset)
    logging.info("  bq_object_setting_str = %s", bq_object_setting_str)
    logging.info("  load_test_data = %s", load_test_data)
    logging.info("  debug = %s", enable_debug)

    if not Path(jinja_data_file).is_file():
        raise ValueError(f"jinja_data_file '{jinja_data_file}' does not exist.")

    try:
        bq_object_setting = json.loads(bq_object_setting_str)
    except Exception as e:
        raise ValueError(f"Failed to read table settings. Error = {e}.")

    return (module_name, jinja_data_file, target_dataset, bq_object_setting,
            load_test_data)


def _generate_dag_files(module_name: str, target_dataset: str, table_name: str,
                        table_setting: dict, table_refresh_sql: str) -> None:
    """Generates necessary DAG files."""

    dag_full_name = (module_name + "_reporting_" +
                     target_dataset.replace(".", "_") + "_" + table_name)

    # SQL file
    sql_file_name = dag_full_name + ".sql"
    output_sql_file = Path(_GENERATED_DAG_SQL_DIR, sql_file_name)
    with open(output_sql_file, mode="w+", encoding="utf-8") as sqlf:
        sqlf.write(table_refresh_sql)

    logging.info("Generated DAG SQL file : %s", output_sql_file)

    # Python file
    python_dag_template_file = Path(_TEMPLATE_DIR,
                                    "airflow_dag_template_reporting.py")
    output_py_file_name = dag_full_name + ".py"
    output_py_file = Path(_GENERATED_DAG_PY_DIR, output_py_file_name)

    today = datetime.datetime.now()
    load_frequency = table_setting["load_frequency"]
    # TODO: Figure out a way to do lowercase in string template substitution
    # directly.
    py_subs = {
        "dag_full_name": dag_full_name,
        "lower_module_name": module_name.lower(),
        "query_file": sql_file_name,
        "load_frequency": load_frequency,
        "year": today.year,
        "month": today.month,
        "day": today.day
    }

    generate_file_from_template(python_dag_template_file, output_py_file,
                                **py_subs)

    logging.info("Generated dag python file: %s", output_py_file)


def _create_view(bq_client: bigquery.Client, view_name: str,
                 core_sql: str) -> None:
    """Creates BQ Reporting view."""
    create_view_sql = ("CREATE OR REPLACE VIEW " + view_name + " AS (\n" +
                       textwrap.indent(core_sql, "    ") + "\n)")
    create_view_job = bq_client.query(create_view_sql)
    _ = create_view_job.result()
    logging.info("Created view '%s'", view_name)


def _create_table(bq_client: bigquery.Client, full_table_name: str,
                  sql_str: str, table_setting: dict) -> None:
    """Creates empty BQ Reporting table."""

    # Steps to create table:
    # a. Use core_sql to create a "temporary" table, and use it to get schema.
    # b. Add partition and cluster clauses.
    # c. Create final table using above two.

    # NOTE: The other option is to create the table directly using
    # "CREATE TABLE" DDL, but applying partition and cluster clauses is
    # more complex.

    # Create a temp table using table create query to get schema.
    # -------------------------------------------------------------
    # NOTE: We can't create BQ temp table using `create_table` API call.
    # Hence, creating a regular table as temp table.
    temp_table_name = full_table_name + "_temp"
    bq_client.delete_table(temp_table_name, not_found_ok=True)
    logging.debug("Creating temporary table '%s'", temp_table_name)
    temp_table_sql = ("CREATE TABLE " + temp_table_name + " AS (\n" +
                      "    SELECT * FROM (\n" +
                      textwrap.indent(sql_str, "        ") + ")\n" +
                      "    WHERE FALSE\n)")
    logging.debug("Create temporary table sql = '%s'", temp_table_sql)
    create_temp_table_job = bq_client.query(temp_table_sql)
    _ = create_temp_table_job.result()
    logging.debug("Temporary table created with sql = \n'%s'", temp_table_sql)
    table_schema = bq_client.get_table(temp_table_name).schema
    logging.debug("Table schema = \n'%s'", table_schema)

    # Create real table.
    # ------------------
    logging.debug("Creating real table '%s'", full_table_name)
    table = bigquery.Table(full_table_name, schema=table_schema)
    # Add partition and cluster details.
    partition_details = table_setting.get("partition_details")
    if partition_details:
        table = bq_materializer.add_partition_to_table_def(
            table, partition_details)
    cluster_details = table_setting.get("cluster_details")
    if cluster_details:
        table = bq_materializer.add_cluster_to_table_def(table, cluster_details)

    try:
        _ = bq_client.create_table(table)
        logging.info("Created table '%s'", full_table_name)
    except Exception as e:
        raise e
    finally:
        # Cleanup - remote temporary table
        bq_client.delete_table(temp_table_name, not_found_ok=True)


def _generate_table_refresh_sql(bq_client: bigquery.Client,
                                full_table_name: str, sql_str: str) -> str:
    """Returns sql for refreshing a table with results from a sql query."""
    table_schema = bq_client.get_table(full_table_name).schema
    table_columns = [("`" + field.name + "`") for field in table_schema]

    table_refresh_sql = ("TRUNCATE TABLE " + full_table_name + ";\n" +
                         "INSERT INTO " + full_table_name + " (\n" +
                         textwrap.indent(",\n".join(table_columns), "  ") +
                         "\n)\n" + sql_str + ";")

    logging.debug("Table Refresh SQL = \n%s", table_refresh_sql)

    return table_refresh_sql


def _validate_sql(bq_client: bigquery.Client, sql: str) -> None:
    """Runs a given sql in BQ to verify if the syntax is correct."""
    logging.info("Validating SQL file....")
    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    try:
        _ = bq_client.query(query=sql, job_config=job_config)
    except Exception as e:
        raise SystemExit("ERROR: Failed to parse sql.\n"
                         "----\n"
                         f"SQL = \n{sql}"
                         "\n----\n"
                         f"Error = {e}")

    logging.info("SQL file is valid.")


def main():

    # Parse and validate arguments.
    (module_name, jinja_data_file, target_dataset, bq_object_setting,
     load_test_data) = _parse_args()

    sql_file = bq_object_setting["sql_file"]
    if not Path(sql_file).is_file():
        raise ValueError(f"sql_file '{sql_file}' does not exist.")

    # Directories to store generated files.
    Path(_GENERATED_DAG_PY_DIR).mkdir(exist_ok=True)
    Path(_GENERATED_DAG_SQL_DIR).mkdir(exist_ok=True)

    bq_client = bigquery.Client()

    # Render core sql text from reporting ddl sql file after applying Jinja
    # parameters.
    rendered_sql = jinja.apply_jinja_params_to_file(sql_file, jinja_data_file)
    # Validate core sql.
    _validate_sql(bq_client, rendered_sql)

    object_type = bq_object_setting["type"]

    if object_type in ["table", "view"]:
        object_name = Path(sql_file).stem
        logging.info("Generating Reporting %s '%s'...", object_type,
                     object_name)
        object_name_full = target_dataset + "." + object_name

        # Create view or table in reporting dataset, if not already present.
        if bq_helper.table_exists(bq_client, object_name_full):
            bq_client.get_table(object_name_full)
            logging.info(
                "Reporting %s '%s' already exists. Not creating it again.",
                object_type, object_name)
            sys.exit(0)

        if object_type == "view":
            try:
                _create_view(bq_client, object_name_full, rendered_sql)
            except Exception as e:
                raise SystemExit(f"ERROR: Failed to create view. Error = {e}.")
        else:
            table_setting = bq_object_setting["table_setting"]
            bq_materializer.validate_materializer_setting(table_setting)
            try:
                _create_table(bq_client, object_name_full, rendered_sql,
                              table_setting)
            except Exception as e:
                raise SystemExit(f"ERROR: Failed to create table. Error = {e}.")

            table_refresh_sql = _generate_table_refresh_sql(
                bq_client, object_name_full, rendered_sql)

            _generate_dag_files(module_name, target_dataset, object_name,
                                table_setting, table_refresh_sql)

            if load_test_data:
                try:
                    logging.info("Populating table '%s' with test data...",
                                 object_name_full)
                    query_job = bq_client.query(table_refresh_sql)
                    # Wait for query to finish.
                    _ = query_job.result()
                except Exception as e:
                    raise SystemExit(
                        f"ERROR: Failed to load test data. Error = {e}.")

        logging.info("Generated Reporting %s '%s' successfully.", object_type,
                     object_name)

    if object_type == "script":
        logging.info("Executing script '%s'...", sql_file)
        try:
            _ = bq_client.query(query=rendered_sql)
        except Exception as e:
            raise SystemExit("ERROR: Failed to run sql.\n"
                             "----\n"
                             f"SQL = \n{rendered_sql}\n"
                             "----\n"
                             f"Error = {e}")
        logging.info("Executed script '%s' successfully.", sql_file)


if __name__ == "__main__":
    main()
