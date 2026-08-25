"""DAG integrity tests - the cheapest bug-catcher in an Airflow repo.

These run in CI on every push and catch the failures that would otherwise
surface as a red DAG in the UI at 3am:

  - import / syntax errors
  - cycles in the graph
  - DAGs shipped without an owner, retries, or tags

Runs in seconds and needs no scheduler, no database, no running Airflow.
"""

from __future__ import annotations

import pytest
from airflow.models import DagBag

DAG_BAG = DagBag(dag_folder="dags/", include_examples=False)


def test_no_import_errors() -> None:
    """Every file in dags/ must import cleanly."""
    assert not DAG_BAG.import_errors, f"DAG import failures: {DAG_BAG.import_errors}"


def test_dags_were_found() -> None:
    """Guard against a silently-empty DagBag making the suite vacuously pass."""
    assert DAG_BAG.dag_ids, "no DAGs discovered in dags/"


@pytest.mark.parametrize("dag_id", DAG_BAG.dag_ids)
def test_dag_has_tags(dag_id: str) -> None:
    """Tags are how you find a DAG in a UI with 500 of them."""
    assert DAG_BAG.get_dag(dag_id).tags, f"{dag_id} has no tags"


@pytest.mark.parametrize("dag_id", DAG_BAG.dag_ids)
def test_dag_has_retries(dag_id: str) -> None:
    """A task with zero retries turns a transient blip into a pager alert."""
    retries = DAG_BAG.get_dag(dag_id).default_args.get("retries", 0)
    assert retries >= 1, f"{dag_id} sets retries={retries}"


@pytest.mark.parametrize("dag_id", DAG_BAG.dag_ids)
def test_dag_has_real_owner(dag_id: str) -> None:
    """'airflow' is nobody. On-call needs a name."""
    owner = DAG_BAG.get_dag(dag_id).default_args.get("owner")
    assert owner not in (None, "", "airflow"), f"{dag_id} has owner={owner!r}"
