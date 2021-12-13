"""A factory that generates Airflow DAGs from a dbt manifest."""
from __future__ import annotations

import json
import typing
from os import PathLike
from pathlib import Path

import yaml
from airflow import DAG
from airflow_dbt_python.operators.dbt import (
    DbtBaseOperator,
    DbtRunOperator,
    DbtSeedOperator,
    DbtTestOperator,
)

from .graph import ManifestGraph

OPERATORS = {
    "model": DbtRunOperator,
    "seed": DbtSeedOperator,
    "test": DbtTestOperator,
}


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

    def build_task(self, node_id: str, dag: DAG) -> DbtBaseOperator:
        """Build a DbtBaseOperator task from a dbt node_id."""
        node = self.graph.get_node(node_id)

        if node is None:
            raise ValueError(f"The node ID {node_id} is not found in the manifest")

        resource_type = node["resource_type"]
        path = node["path"]
        operator = OPERATORS[resource_type](
            task_id=node_id,
            select=[path],
            dag=dag,
        )
        return operator

    def build_dag(
        self,
    ) -> DAG:
        """Build a DAG from a dbt manifest."""
        dag = DAG(**self.config)
        tasks: dict[str, DbtBaseOperator] = {}

        for model in self.graph.root_models():
            for parent, child in self.graph.traverse(model):
                if tasks.get(parent) is None:
                    tasks[parent] = self.build_task(parent, dag)
                if tasks.get(child) is None:
                    tasks[child] = self.build_task(child, dag)

                tasks[parent].set_downstream(tasks[child])

        return dag

    def generate_dags(
        self,
        globals: dict[str, typing.Any],
    ):
        """Generate all DAGs from a dbt manifest."""
        dag = self.build_dag()
        globals[self.config["dag_id"]] = dag
