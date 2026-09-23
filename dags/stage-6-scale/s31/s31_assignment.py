"""Session 31 · assignment — carve a tenant boundary for one team.

Boilerplate is ready. Build what the note (§8) asks:
  - a team-owned DAG with its own owner, tag, queue and pool
  - tasks that only ever claim that team's pool (never default_pool)
  - a note in the docstring on which RBAC role / bundle would own it
Then run:
    airflow dags test s31_assignment 2026-01-01
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s31_assignment",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-31"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():
    # TODO: build a single team's isolated DAG (own owner/queue/pool), no shared default_pool
    @task
    def todo() -> None:
        print("replace me")

    todo()


pipeline()
