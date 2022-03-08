"""Test the DbtDagFactory class."""
import pytest

from dbt_dag_factory import DbtDagFactory

CONFIG = """
test_dag:
  dag_arguments:
    schedule_interval: '* * * * *'
    max_active_tasks: 1
    max_active_runs: 1
    description: 'Sample dbt DAG'
    start_date: 2021-01-01 00:00:00

    default_args:
      owner: example_owner
      retries: 1
      retry_delay_sec: 300
"""


@pytest.fixture
def config_file(tmp_path_factory):
    """Create a config.yml file for testing."""
    p = tmp_path_factory.mktemp(".dbt") / "config.yml"
    p.write_text(CONFIG)
    return p


@pytest.fixture
def factory(config_file):
    """Return a DbtDagFactory with a test manifest and configuration."""
    factory = DbtDagFactory("tests/manifest.json", config_file)
    return factory


def test_factory_dag_building(factory):
    """Test building a DAG with a test config."""
    dags = factory.build_dags()
    assert "test_dag" in dags
    assert "test_dag" == dags["test_dag"].dag_id

    dag = dags["test_dag"]

    tasks = dag.tasks
    seen = set()

    for task in tasks:
        node_id = task.task_id
        assert node_id not in seen
        seen.add(node_id)

        node = factory.graph.manifest["nodes"][node_id]
        assert task.select == [node["path"]]

        children = factory.graph.manifest["child_map"].get(node_id, [])
        assert set(children) == task.downstream_task_ids

        parents = factory.graph.manifest["parent_map"].get(node_id, [])
        assert set(parents) == task.upstream_task_ids
