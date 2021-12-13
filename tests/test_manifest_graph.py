"""Test the ManifestGraph class."""
import json

import pytest

from dbt_dag_factory.graph import ManifestGraph


@pytest.fixture
def manifest():
    """Read a test manifest.json."""
    with open("tests/manifest.json") as f:
        manifest = json.load(f)
    return manifest


def test_manifest_graph_root_models(manifest):
    """Assert test ManifestGraph generates correct root models."""
    mg = ManifestGraph(manifest)
    root_models = list(mg.root_models())

    assert root_models == [
        "seed.jaffle_shop.raw_customers",
        "seed.jaffle_shop.raw_orders",
        "seed.jaffle_shop.raw_payments",
    ]


def test_manifest_first_children(manifest):
    """Assert ManifestGraph generates correct first children."""
    mg = ManifestGraph(manifest)

    assert mg.get_first_children("seed.jaffle_shop.raw_customers") == [
        "model.jaffle_shop.stg_customers"
    ]
    assert mg.get_first_children("seed.jaffle_shop.raw_orders") == [
        "model.jaffle_shop.stg_orders"
    ]
    assert mg.get_first_children("model.jaffle_shop.stg_customers") == [
        "model.jaffle_shop.customers",
        "test.jaffle_shop.not_null_stg_customers_customer_id.e2cfb1f9aa",
        "test.jaffle_shop.unique_stg_customers_customer_id.c7614daada",
    ]
