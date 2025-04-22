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
"""Defines utility functions to help construct task dependent DAGs."""

from collections import defaultdict
import dataclasses
import datetime
import graphlib
import logging
from pathlib import Path
import textwrap
from typing import Optional

from common.materializer import dag_types
from common.materializer import generate_assets
from common.py_libs import constants
from common.py_libs import cortex_bq_client
from common.py_libs import dag_generator
from common.py_libs import jinja


def _get_parent_task_ids(parent_files: list[str]) -> list[str]:
    task_ids = []
    for p in parent_files:
        if p == "start_task":
            task_ids.append(p)
        else:
            task_ids.append(f"refresh_{Path(p).stem}")
    return task_ids


def _get_dag_setting(obj: dag_types.BqObject) -> Optional[dag_types.Dag]:
    if not (obj.table_setting and obj.table_setting.dag_setting):
        return None
    return obj.table_setting.dag_setting


def get_validated_load_freq(
        top_level_objs: dict[str, dag_types.BqObject]) -> str:
    """Return validated load_frequency defined by top level nodes.

    Top level nodes must have a consistent schedule defined. Multiple nodes
    can define the same load_frequence, or nodes can leave it unset as long as
    on of the top level nodes has it set.

    Args:
        top_level_objs: map of top level BQ objects that are direct children of
            the DAG root keyed by sql file name.

    Returns: Validated load_frequency string defining DAG schedule.

    Raises:
        ValueError: if the top level nodes don't have any defined load_frequency
            or if they conflict.
    """
    schedule = None
    for sql_file, obj in top_level_objs.items():
        assert obj.table_setting is not None

        if obj.table_setting.load_frequency is None:
            continue

        load_frequency = obj.table_setting.load_frequency
        if schedule is None:
            schedule = load_frequency
            continue

        if load_frequency != schedule:
            raise ValueError(
                "Top level node deteced with conflicting load_frequency. "
                f"{sql_file} set to '{load_frequency}' but expected "
                f"'{schedule}'. Please resolve any load_frequency conflicts "
                f"among top level nodes: {list(top_level_objs.keys())}")

    if schedule is None:
        raise ValueError(
            "At least one top level node must define load_frequency. Top level "
            f"nodes include: {list(top_level_objs.keys())}")
    return schedule


def get_task_deps(bq_object_settings: dict) -> dict[str, dag_types.BqObject]:
    """Get a dictionary of task dependent objects from reporting settings.

    Any object that has dag_setting defined is considered task dependent, even
    if there are no defined edges.

    Args:
        bq_object_settings: A dict representing BQ [in]dependent objects
            defined in a reportings settings yaml file.

    Returns:
        A dict of task dependent BQ objects keyed by their SQL file.
    """
    reporting_objects = dag_types.ReportingObjects.from_dict(bq_object_settings)

    # Populate map of BQ objects and set of objects that are parent or children
    # deps.
    obj_map = {}
    dep_files = set()
    all_objs = []
    if reporting_objects.bq_independent_objects:
        all_objs.extend(reporting_objects.bq_independent_objects)
    if reporting_objects.bq_dependent_objects:
        all_objs.extend(reporting_objects.bq_dependent_objects)

    for obj in all_objs:
        obj_map[obj.sql_file] = obj

        # Objects without dag_setting are ignored and considered independent.
        dag_setting = _get_dag_setting(obj)
        if not dag_setting:
            continue

        dep_files.add(obj.sql_file)
        if dag_setting.parents:
            dep_files = dep_files.union(dag_setting.parents)

    return {k: v for k, v in obj_map.items() if k in dep_files}


@dataclasses.dataclass
class DependentDagGenerator:
    """Generates DAG files with task dependencies.

    Attributes:
        module_name: Name of module (e.g. "cm360", "sap")
        target_dataset_name: Name of BQ dataset - e.g. "my_project.my_dataset"
        target_dataset_type: Type of dataset - e.g. "reporting" or "cdc".
        allow_telemetry: Cortex config file option to enable telemetry.
        location: Location used for BigQueryInsertJob operators.
        output_dir: Directory to write the generated files.
        jinja_data_file: File containing jinja substitutions used to create
            generated SQL.
    """
    module_name: str
    target_dataset_name: str
    target_dataset_type: str
    allow_telemetry: bool
    location: str
    output_dir: Path
    jinja_data_file: Path

    def _generate_sql_file(self, sql_file: str, dag_name: str) -> Path:
        """Returns the relative path to the generated table refresh sql file."""

        table_name = Path(sql_file).stem
        full_table_name = f"{self.target_dataset_name}.{table_name}"
        bq_client = cortex_bq_client.CortexBQClient()

        # Generate core sql text from sql file after applying Jinja parameters.
        core_sql = jinja.apply_jinja_params_to_file(sql_file,
                                                    str(self.jinja_data_file))
        generate_assets.validate_sql(bq_client, core_sql)
        refresh_query = generate_assets.generate_table_refresh_sql(
            bq_client, full_table_name, core_sql)

        relative_sql_file = Path(table_name).with_suffix(".sql")
        full_sql_file = self.output_dir / dag_name / relative_sql_file
        full_sql_file.parent.mkdir(exist_ok=True, parents=True)
        full_sql_file.write_text(refresh_query, encoding="utf-8")
        logging.info("Generated DAG SQL file : %s", full_sql_file)

        return relative_sql_file

    def _generate_dag_file(self, dag_name: str, ordered_nodes: list[str],
                           task_dep_objs: dict[str, dag_types.BqObject],
                           load_frequency: str) -> Path:
        logging.info("Generating DAG file for: %s", dag_name)

        parent_dir = Path(__file__).resolve().parent
        template_dir = parent_dir / "templates"
        header_template = (template_dir /
                           "airflow_task_dep_dag_template_reporting.py")
        bq_op_template = template_dir / "bq_insert_job_template.txt"
        dag_full_name = "_".join(
            [self.target_dataset_name.replace(".", "_"), dag_name])
        today = datetime.datetime.now()

        # General template substitutions.
        subs = {
            "dag_full_name": dag_full_name,
            "module_name": self.module_name,
            "tgt_dataset_type": self.target_dataset_type,
            "load_frequency": load_frequency,
            "year": today.year,
            "month": today.month,
            "day": today.day,
            "runtime_labels_dict": "",  # A place holder for label dict string,
            "bq_location": self.location
        }
        if self.allow_telemetry:
            subs["runtime_labels_dict"] = str(constants.CORTEX_JOB_LABEL)
        if self.target_dataset_type == "reporting":
            subs["tags"] = [self.module_name, self.target_dataset_type]

        # Create DAG header.
        dag_header = dag_generator.generate_str_from_template(
            header_template, **subs)

        # Create BQ ops and edges.
        bq_op_strs = []
        edge_strs = []
        for sql_file in ordered_nodes:
            # Start tasks don't have parent edges or BQ operators.
            if sql_file == "start_task":
                continue

            table_name = Path(sql_file).stem
            if sql_file == "stop_task":
                task_id = sql_file
            else:
                task_id = f"refresh_{table_name}"

            # Generate edges.
            obj = task_dep_objs[sql_file]
            parents = _get_parent_task_ids(
                obj.table_setting.dag_setting.parents)  # type: ignore
            assert parents is not None
            if len(parents) > 1:
                parents_str = ", ".join(sorted(parents))
                edge_str = f"[{parents_str}] >> {task_id}"
            else:
                edge_str = f"{parents[0]} >> {task_id}"

            edge_str = textwrap.indent(edge_str, " " * 4)
            edge_strs.append(edge_str)

            # Don't generate BQ operator for stop tasks .
            if sql_file == "stop_task":
                continue

            generated_sql_file = self._generate_sql_file(sql_file, dag_name)

            # SQL File specific substitutions.
            subs["table_name"] = table_name
            subs["query_file"] = generated_sql_file

            # Generate BQ operators.
            bq_op = dag_generator.generate_str_from_template(
                bq_op_template, **subs)
            bq_op = textwrap.indent(bq_op, " " * 4)
            bq_op_strs.append(bq_op)

        generated_dag = "\n\n".join([dag_header, *bq_op_strs, *edge_strs])
        generated_dag_file = (self.output_dir / dag_name /
                              dag_full_name).with_suffix(".py")
        generated_dag_file.parent.mkdir(parents=True, exist_ok=True)
        generated_dag_file.write_text(generated_dag)

        logging.info("Generated DAG py file : %s", generated_dag_file)
        return generated_dag_file

    def create_dep_dag(self, dag_name: str,
                       task_dep_objs: dict[str, dag_types.BqObject]) -> Path:
        """Creates a single DAG from a map of task dependent objects.

        Args:
            dag_name: Name of the DAG to generate.
            task_dep_objs: map of sql file names to their BQ Object settings.
                This argument is mutated to add a "start_task" parent to all top
                level nodes and a "stop_task" key with leaf nodes as parents.

        Returns: Path to generated DAG file.

        Raises:
            ValueError: If a cycle is detected in the DAG.
            RuntimeError: If a task dependent object is provided without
                dag_setting defined.
        """
        topo_sorter = graphlib.TopologicalSorter()

        # Top level nodes only have the root as a parent dependency
        top_level_nodes = {}
        leaf_nodes = set(task_dep_objs.keys())

        for sql_file, obj in task_dep_objs.items():
            dag_setting = _get_dag_setting(obj)
            if not dag_setting:
                raise RuntimeError(
                    "Task dependent object must have dag_setting defined: "
                    f"{obj}")

            # Add an inferred root parent for top level nodes.
            if not dag_setting.parents:
                top_level_nodes[sql_file] = obj
                dag_setting.parents = ["start_task"]

            leaf_nodes = leaf_nodes.difference(dag_setting.parents)
            topo_sorter.add(sql_file, *dag_setting.parents)

        # Add an inferred stop node
        topo_sorter.add("stop_task", *leaf_nodes)
        task_dep_objs["stop_task"] = dag_types.BqObject(
            type=dag_types.BqObjectType.BQ_OBJECT_TYPE_UNSPECIFIED,
            table_setting=dag_types.Table(dag_setting=dag_types.Dag(
                name=dag_name, parents=list(leaf_nodes))))

        load_freq = get_validated_load_freq(top_level_nodes)

        try:
            ordered_nodes = [*topo_sorter.static_order()]
        except graphlib.CycleError as e:
            raise ValueError(f"Cyclic dependency detected in DAG: {dag_name}. "
                             "DAG must not have cycles.") from e

        return self._generate_dag_file(dag_name, ordered_nodes, task_dep_objs,
                                       load_freq)

    def create_dep_dags(
            self, task_dep_objs: dict[str, dag_types.BqObject]) -> list[Path]:
        """Creates a series of DAGs from a dict of objs keyed by sql file.

        Returns: List of paths to generated DAG files.
        """
        dags = defaultdict(dict)
        output_files = []

        # Group objects into DAGs by name.
        for sql_file, obj in task_dep_objs.items():
            if not (obj.table_setting and obj.table_setting.dag_setting and
                    obj.table_setting.dag_setting.name):
                raise RuntimeError("Task dependent object was detected but "
                                   f"dag_setting.name is undefined: {obj}")

            dag_name = obj.table_setting.dag_setting.name
            dag_name = dag_name.lower().replace(" ", "_")
            dags[dag_name][sql_file] = obj

        for dag_name, obj in dags.items():
            output_files.append(self.create_dep_dag(dag_name, obj))

        return output_files
