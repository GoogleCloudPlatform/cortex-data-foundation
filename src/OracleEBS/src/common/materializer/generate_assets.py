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
"""Defines utility functions to generate assets e.g. BQ tables and DAG files.

Note that functions specific to task dependent dags are defined separately in
dependent_dags.py
"""

import datetime
import json
import logging
from pathlib import Path
import string
import textwrap
import typing
from typing import Optional
import yaml

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from common.py_libs import bq_materializer
from common.py_libs import constants
from common.py_libs import dag_generator

# TODO: create unit tests.

GENERATED_BUILD_DIR_NAME = Path().cwd() / "generated_materializer_build_files"
GENERATED_DAG_DIR_NAME = Path().cwd() / "generated_materializer_dag_files"
JINJA_DATA_FILE_NAME = "bq_sql_jinja_data.json"


def get_task_dep_materializer_settings(settings_file: Path) -> Path:
    """Returns the path to the task dependent version of the settings file."""
    ext = settings_file.suffix
    td_file_name = Path(f"{settings_file.stem}_task_dep{ext}")
    return settings_file.parent / td_file_name


def get_enabled_task_dep_settings_file(base_settings_file: Path,
                                  config_dict: dict) -> Optional[Path]:
    """Returns the path to the task dependent settings file.

    This only returns a path if it exists and task dependencies are enabled
    in the config.
    """
    if config_dict.get("enableTaskDependencies"):
        td_settings_file = get_task_dep_materializer_settings(
            base_settings_file)
        if td_settings_file.exists():
            return td_settings_file


def generate_dag_files(module_name: str, target_dataset_type: str,
                       target_dataset: str, table_name: str,
                       table_setting: dict, table_refresh_sql: str,
                       allow_telemetry: bool, location: str, template_dir: Path,
                       generated_dag_dir: Path) -> None:
    """Generates necessary DAG files to refresh a given table.

    There are two files to be generated:
    1. Python file - this is the main DAG file, and is generated using a
       template.
    2. BigQuery SQL file that the DAG needs to execute to refresh a table.

    Naming schema:
       Dag Name :
         <project>_<dataset>_refresh_<table>
       Dag Full Name (shown in Airflow UI):
         <module>_<dataset_type>_<dag_name>
       Output Directory:
         <dag_dir>/<module>/<dataset_type>/<dag_name>.py
       Python file:
         <output_directory>/<dag_name>.py
       SQL file:
         <output_directory>/sql_scripts/<dag_name>.sql

       e.g.
          dag_dir/cm360/reporting/
              project1_dataset1_refresh_clicks.py
              project1_dataset1_refresh_impressions.py
              sql_scripts/
                  project1_dataset1_refresh_clicks.sql
                  project1_dataset1_refresh_impressions.sql

    Args:
        module_name: Name of module (e.g. "cm360", "sap")
        target_dataset_type: Type of dataset - e.g. "reporting" or "cdc".
        target_dataset: Bigquery dataset including GCP project id.
            e.g. "my_project.my_dataset".
        table_name: Table name to refresh. (e.g. "CustomerMD")
        table_setting: Table Settings as defined in the settings file.
        table_refresh_sql: SQL with logic to populate data in the table.
        allow_telemetry: Bool from Cortex config file to specify if
            telemetry is allowed.
        location: Location to pass to BigQueryInsertJob operators in DAGs.
        template_dir: directory where python dag template is stored.
        generated_dag_dir: directory where generated dag will be materialized.
    """
    dag_name = "_".join(
        [target_dataset.replace(".", "_"), "refresh", table_name])
    dag_full_name = "_".join(
        [module_name.lower(), target_dataset_type, dag_name])

    # Directory to store generated files - e.g. "dag_dir/cm360/reporting/"
    output_dir = Path(generated_dag_dir, module_name.lower(),
                      target_dataset_type)

    # Generate sql file.
    sql_file = Path("sql_scripts", dag_name).with_suffix(".sql")
    output_sql_file = Path(output_dir, sql_file)
    output_sql_file.parent.mkdir(exist_ok=True, parents=True)
    with output_sql_file.open(mode="w+", encoding="utf-8") as sqlf:
        sqlf.write(table_refresh_sql)
    logging.info("Generated DAG SQL file : %s", output_sql_file)

    # Generate python DAG file.
    python_dag_template_file = Path(template_dir,
                                    "airflow_dag_template_reporting.py")
    output_py_file = Path(output_dir, dag_name).with_suffix(".py")

    today = datetime.datetime.now()
    load_frequency = table_setting["load_frequency"]
    # TODO: Figure out a way to do lowercase in string template substitution
    # directly.
    py_subs = {
        "dag_full_name": dag_full_name,
        "lower_module_name": module_name.lower(),
        "lower_tgt_dataset_type": target_dataset_type,
        "query_file": str(sql_file),
        "load_frequency": load_frequency,
        "year": today.year,
        "month": today.month,
        "day": today.day,
        "runtime_labels_dict": "",  # A place holder for label dict string,
        "bq_location": location
    }

    # Add bq_labels to py_subs dict if telemetry allowed
    # Converts CORTEX_JOB_LABEL to str for substitution purposes
    if allow_telemetry:
        py_subs["runtime_labels_dict"] = str(constants.CORTEX_JOB_LABEL)

    if target_dataset_type == "reporting":
        py_subs["tags"] = [module_name.lower(), "reporting"]

    dag_generator.generate_file_from_template(python_dag_template_file,
                                              output_py_file, **py_subs)

    logging.info("Generated dag python file: %s", output_py_file)


def create_view(bq_client: bigquery.Client, view_name: str,
                description: typing.Optional[str], core_sql: str) -> None:
    """Creates BQ Reporting view."""
    create_view_sql = ("CREATE OR REPLACE VIEW `" + view_name + "` " +
                       f"OPTIONS(description=\"{description or ''}\") AS (\n" +
                       textwrap.indent(core_sql, "    ") + "\n)")
    create_view_job = bq_client.query(create_view_sql)
    _ = create_view_job.result()
    logging.info("Created view '%s'", view_name)


def create_table(bq_client: bigquery.Client, full_table_name: str,
                 description: typing.Optional[str], sql_str: str,
                 table_setting: dict) -> None:
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
    logging.info("Creating temporary table '%s'", temp_table_name)
    temp_table_sql = ("CREATE TABLE `" + temp_table_name + "` " +
                      "OPTIONS(expiration_timestamp=TIMESTAMP_ADD(" +
                      "CURRENT_TIMESTAMP(), INTERVAL 12 HOUR))" +
                      " AS (\n   SELECT * FROM (\n" +
                      textwrap.indent(sql_str, "        ") + "\n)\n" +
                      "    WHERE FALSE\n)")
    logging.debug("temporary table sql = '%s'", temp_table_sql)
    create_temp_table_job = bq_client.query(temp_table_sql)
    _ = create_temp_table_job.result()
    logging.info("Temporary table created.")
    table_schema = bq_client.get_table(temp_table_name).schema
    logging.debug("Table schema = \n'%s'", table_schema)

    # Create final actual table.
    # -------------------------
    logging.info("Creating actual table '%s'", full_table_name)
    table = bigquery.Table(full_table_name, schema=table_schema)
    # Add partition and cluster details.
    partition_details = table_setting.get("partition_details")
    if partition_details:
        table = bq_materializer.add_partition_to_table_def(
            table, partition_details)
    cluster_details = table_setting.get("cluster_details")
    if cluster_details:
        table = bq_materializer.add_cluster_to_table_def(table, cluster_details)
    # Add optional description
    if description:
        table.description = description

    try:
        _ = bq_client.create_table(table)
        logging.info("Created table '%s'", full_table_name)
    except Exception as e:
        raise e
    finally:
        # Cleanup - remove temporary table
        bq_client.delete_table(temp_table_name, not_found_ok=True)
        try:
            bq_client.get_table(temp_table_name)
            logging.warning(
                "⚠️ Couldn't delete temporary table `%s`."
                "Please delete it manually. ⚠️", temp_table_name)
        except NotFound:
            logging.info("Deleted temp table = %s'", temp_table_name)


def generate_table_refresh_sql(bq_client: bigquery.Client, full_table_name: str,
                               sql_str: str) -> str:
    """Returns sql for refreshing a table with results from a sql query."""
    table_schema = bq_client.get_table(full_table_name).schema
    table_columns = [f"`{field.name}`" for field in table_schema]

    # We want to make table refresh atomic in nature. Wrapping TRUNCATE and
    # INSERT within a transaction achieves that purpose. Without this, it leads
    # to suboptimal customers experience when some tables miss data (albeit
    # momentarily) during the table refresh dag execution.

    # TODO: Indent the string below for readability. Handle output using dedent.
    table_refresh_sql_text = """
BEGIN
    BEGIN TRANSACTION;

    TRUNCATE TABLE `${full_table_name}`;

    INSERT INTO `${full_table_name}`
    (
        ${table_columns}
    )
    ${select_statement}
    ;

    COMMIT TRANSACTION;

    EXCEPTION WHEN ERROR THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = @@error.message;

END;
    """

    table_refresh_sql = string.Template(table_refresh_sql_text).substitute(
        full_table_name=full_table_name,
        table_columns=textwrap.indent(",\n".join(table_columns), " " * 8),
        select_statement=textwrap.indent(sql_str, " " * 4))

    logging.debug("Table Refresh SQL = \n%s", table_refresh_sql)

    return table_refresh_sql


def validate_sql(bq_client: bigquery.Client, sql: str) -> None:
    """Runs a given sql in BQ to verify if the syntax is correct."""
    logging.info("Validating SQL file....")
    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    try:
        _ = bq_client.query(query=sql, job_config=job_config)
    except Exception as e:
        raise SystemExit("🛑 ERROR: Failed to parse sql.\n"
                         "----\n"
                         f"SQL = \n{sql}"
                         "\n----\n") from e

    logging.info("SQL file is valid.")


def get_materializer_settings(materializer_settings_file: str) -> dict:
    """Parses settings file and returns settings dict after validations."""
    logging.info("Loading Materializer settings file '%s'...",
                 materializer_settings_file)

    with open(materializer_settings_file,
              encoding="utf-8") as materializer_settings_fp:
        materializer_settings = yaml.safe_load(materializer_settings_fp)

    if materializer_settings is None:
        raise ValueError(f"🛑 '{materializer_settings_file}' file is empty.")

    logging.debug("Materializer settings for this module : \n%s",
                  json.dumps(materializer_settings, indent=4))

    # Validate bq object settings.
    # Since this setting file contains two separate bq table setting sections,
    # we validate both of them.
    independent_tables_settings = materializer_settings.get(
        "bq_independent_objects")
    dependent_tables_settings = materializer_settings.get(
        "bq_dependent_objects")

    # At least one of the two sections needs to be present.
    if (independent_tables_settings is None and
            dependent_tables_settings is None):
        raise ValueError(
            "🛑 'bq_independent_objects' and 'bq_dependent_setting' both "
            "can not be empty.")

    for settings in [independent_tables_settings, dependent_tables_settings]:
        if settings:
            bq_materializer.validate_bq_materializer_settings(settings)

    return materializer_settings
