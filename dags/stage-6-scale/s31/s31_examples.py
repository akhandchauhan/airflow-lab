"""Session 31 · examples — multi-tenancy (per-team queue / pool / ownership).

Boilerplate is ready. Build the note's reference here (§7): two DAGs standing in for
two teams, each pinned to its own queue and pool with a distinct owner, so the access
and resource boundaries are visible. Plain DAGs, no BigQuery. Run:
    airflow dags test s31_examples 2026-01-01
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s31_examples",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-31"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():
    # TODO: model two teams with distinct owner + queue + pool to show tenant boundaries
    @task
    def todo() -> None:
        print("replace me")

    todo()


pipeline()
