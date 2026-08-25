"""DAG integrity tests - the cheapest bug-catcher in an Airflow repo.

Runs in CI on every push and catches the failures that would otherwise surface
as a red DAG in the UI at 3am: import errors, cycles, and DAGs shipped without
an owner, retries, or tags. No scheduler, no running Airflow, no database.

--------------------------------------------------------------------------
Airflow 3 gotcha that every Airflow 2 tutorial still gets wrong
--------------------------------------------------------------------------
`from airflow.models import DagBag` still "works", but it is a deprecation
shim that hands you `DBDagBag` - a database-backed class whose signature is
`(load_op_links, cache_size, cache_ttl)`. Passing it `dag_folder=` or
`include_examples=` raises:

    TypeError: DagBag.__init__() got an unexpected keyword argument
               'include_examples'

The file-parsing DagBag now lives at `airflow.dag_processing.dagbag`, and its
signature is:

    DagBag(dag_folder=None, safe_mode=NOTSET, load_op_links=True,
           collect_dags=True, known_pools=None, bundle_path=None,
           bundle_name=None)

Note there is no `include_examples` any more - example DAGs are controlled by
the `AIRFLOW__CORE__LOAD_EXAMPLES` setting instead (set below and in CI).
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
DAG_FOLDER = REPO_ROOT / "dags"

# Must be set before importing airflow so config picks them up.
os.environ.setdefault("AIRFLOW_HOME", str(REPO_ROOT / ".airflow"))
os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"

from airflow.dag_processing.dagbag import DagBag  # noqa: E402

# Absolute paths keep this independent of pytest's working directory.
# bundle_path is new in Airflow 3 - it is the base that import_errors keys and
# other bundle-relative paths are resolved against.
DAG_BAG = DagBag(dag_folder=str(DAG_FOLDER), bundle_path=REPO_ROOT)


def test_no_import_errors() -> None:
    """Every file in dags/ must import cleanly."""
    assert not DAG_BAG.import_errors, f"DAG import failures: {DAG_BAG.import_errors}"


def test_dags_were_found() -> None:
    """Guard against an empty DagBag making the whole suite vacuously pass."""
    assert DAG_BAG.dag_ids, f"no DAGs discovered in {DAG_FOLDER}"


@pytest.mark.parametrize("dag_id", DAG_BAG.dag_ids)
def test_dag_has_tags(dag_id: str) -> None:
    """Tags are how you find a DAG in a UI showing 500 of them."""
    assert DAG_BAG.get_dag(dag_id).tags, f"{dag_id} has no tags"


@pytest.mark.parametrize("dag_id", DAG_BAG.dag_ids)
def test_tasks_have_retries(dag_id: str) -> None:
    """Zero retries turns a transient network blip into a pager alert."""
    for task in DAG_BAG.get_dag(dag_id).tasks:
        assert task.retries >= 1, f"{dag_id}.{task.task_id} has retries={task.retries}"


@pytest.mark.parametrize("dag_id", DAG_BAG.dag_ids)
def test_tasks_have_real_owner(dag_id: str) -> None:
    """'airflow' is nobody. On-call needs a name."""
    for task in DAG_BAG.get_dag(dag_id).tasks:
        assert task.owner not in (None, "", "airflow"), (
            f"{dag_id}.{task.task_id} has owner={task.owner!r}"
        )
