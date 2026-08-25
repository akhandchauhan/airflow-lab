"""DAG integrity tests - the cheapest bug-catcher in an Airflow repo.

Runs in CI on every push and catches the failures that would otherwise surface
as a red DAG in the UI at 3am: import errors, cycles, and DAGs shipped without
an owner, retries, or tags.

Needs no scheduler, no running Airflow, no database.

Airflow 3 note: assertions read *resolved task attributes* (`task.retries`,
`task.owner`) rather than `dag.default_args`. `default_args` is a
construction-time convenience that is not reliably retained on the DAG object,
especially for @dag-decorated DAGs. What actually governs runtime behaviour is
what landed on each task - so that is what we assert on.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
DAG_FOLDER = REPO_ROOT / "dags"

# Give Airflow a scratch home so it does not try to write into the repo root.
os.environ.setdefault("AIRFLOW_HOME", str(REPO_ROOT / ".airflow-test-home"))
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")

from airflow.models import DagBag  # noqa: E402  (must follow the env setup)

# Absolute path keeps this independent of pytest's working directory.
DAG_BAG = DagBag(dag_folder=str(DAG_FOLDER), include_examples=False)


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
