"""A factory that generates Airflow DAGs from a dbt manifest."""
from __future__ import annotations

import json
import typing
from enum import Enum
from os import PathLike
from pathlib import Path

import yaml
from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow_dbt_python.operators.dbt import (
    DbtBaseOperator,
    DbtRunOperator,
    DbtSeedOperator,
    DbtTestOperator,
)

from dbt_dag_factory.graph import ManifestGraph

OPERATORS = {
    "model": (DbtRunOperator, "dbt_run"),
    "seed": (DbtSeedOperator, "dbt_seed"),
    "test": (DbtTestOperator, "dbt_test"),
}


class FilterBy(Enum):
    """Enumeration of potential keys to models by."""

    PATHS = "paths"
    TAGS = "tags"
    MODELS = "models"

    def filter_func(self, node, value) -> bool:
        """Decide whether a model passes this filter."""
        if self.value == "paths":
            return node["original_file_path"] in value
        elif self.value == "tags":
            return all((tag in node.get("tags", []) for tag in value))
        elif self.value == "models":
            return node["name"] in value
        else:
            return False


class DbtDagFactory:
    """A factory that uses a dbt manifest to produce DAGs."""

    def __init__(
        self,
        manifest_path: typing.Union[str, PathLike[str]],
        config_path: typing.Union[str, PathLike[str]],
    ):
        """Initialize a DbtDagFactory from a dbt manifest and a configuration."""
        self.manifest_path = Path(manifest_path)
        self.config_path = Path(config_path)
        self._manifest: typing.Optional[dict[str, typing.Any]] = None
        self._config: typing.Optional[dict[str, typing.Any]] = None
        self._graph: typing.Optional[ManifestGraph] = None

    @property
    def manifest(self) -> dict[str, typing.Any]:
        """Return a manifest dictionary, loading it first if it's None."""
        if self._manifest is None:
            with open(self.manifest_path, "r") as f:
                self._manifest = json.loads(f.read(), strict=False)
        return self._manifest

    @property
    def config(self) -> dict[str, typing.Any]:
        """Return a configuration dictionary, loading it first if it's None."""
        if self._config is None:
            self._config = self.load_config()
        return self._config

    def load_config(self) -> dict[str, typing.Any]:
        """Load a configuration from a JSON or YAML file."""
        with open(self.config_path, "r") as f:
            content = f.read()

        if self.config_path.suffix == ".yaml" or self.config_path.suffix == ".yml":
            return yaml.safe_load(content)
        elif self.config_path.suffix == ".json":
            return json.loads(content)
        else:
            raise ValueError("Configuration file must be YAML or JSON.")

    @property
    def graph(self) -> ManifestGraph:
        """Return a ManifestGraph."""
        if self._graph is None:
            self._graph = ManifestGraph(self.manifest)
        return self._graph

    def generate_dags(
        self,
        globals: dict[str, typing.Any],
    ):
        """Generate all DAGs from a dbt manifest."""
        for dag_id, dag in self.build_dags().items():
            globals[dag_id] = dag

    def build_dags(self):
        """Build all DAGs from a dbt manifest."""
        dags: dict[str, DAG] = {}
        tasks: dict[str, tuple[DbtBaseOperator, str]] = {}

        for model in self.graph.root_models():
            for parent, child in self.graph.traverse(model):
                parent_dag_id = self.get_node_dag_id(parent)
                child_dag_id = self.get_node_dag_id(child)

                if parent not in tasks:
                    if parent_dag_id not in dags:
                        dags[parent_dag_id] = DAG(
                            parent_dag_id,
                            **self.get_dag_arguments(parent_dag_id),
                        )

                    tasks[parent] = self.build_task(
                        parent,
                        dags[parent_dag_id],
                    )

                if child not in tasks:
                    if child_dag_id not in dags:
                        dags[child_dag_id] = DAG(
                            child_dag_id,
                            **self.get_dag_arguments(child_dag_id),
                        )

                    tasks[child] = self.build_task(
                        child,
                        dags[child_dag_id],
                    )

                if parent_dag_id != child_dag_id:
                    parent_task = tasks[parent].task_id
                    sensor_id = f"wait_on_{parent_dag_id}_{parent_task}"

                    if sensor_id not in tasks:
                        sensor = ExternalTaskSensor(
                            task_id=sensor_id,
                            dag_id=child_dag_id,
                            external_dag_id=parent_dag_id,
                            external_task_id=parent_task,
                            mode="reschedule",
                        )

                        tasks[sensor_id] = sensor

                    parent = sensor_id

                tasks[parent].set_downstream(tasks[child])

        return dags

    def build_task(self, node_id: str, dag: DAG) -> DbtBaseOperator:
        """Build a DbtBaseOperator task from a dbt node_id."""
        node = self.get_node(node_id)

        resource_type = node["resource_type"]
        task_id, select = self.get_node_task_id_select(node_id, dag)
        operator = OPERATORS[resource_type][0](
            task_id=task_id,
            select=select,
            dag=dag,
        )
        return operator

    def get_dag_arguments(self, dag_id: str):
        """Return the arguments for a given DAG ID."""
        return self.config[dag_id]["dag_arguments"]

    def get_node_dag_id(self, node_id: str):
        """Return the DAG ID of a given node ID."""
        node = self.get_node(node_id)

        for dag_id, dag_config in self.config.items():
            for _filter in FilterBy:
                value = dag_config.get(_filter.value, None)
                if value is None:
                    continue

                result = _filter.filter_func(node, value)
                if result is False:
                    break

            else:
                return dag_id

        return node["project_name"]

    def get_node_task_id_select(self, node_id: str, dag: DAG):
        """Return the Task ID and the select argument of a given node ID."""
        node = self.get_node(node_id)

        if dag.dag_id not in self.config:
            return node_id, None

        resource_type = node["resource_type"]
        dag_config = self.config[dag.dag_id]

        task_id = OPERATORS[resource_type][1]
        select = []

        for _filter in FilterBy:
            value = dag_config.get(_filter.value, None)
            if value is None:
                continue

            task_id += value.join("_")
            select.extend(value)

        if len(select) == 0:
            return node_id, [node["path"]]

        return task_id, select

    def get_node(self, node_id: str):
        """Get a node from the graph."""
        node = self.graph.get_node(node_id)
        if node is None:
            raise ValueError(f"The node ID {node_id} is not found in the manifest")
        return node
