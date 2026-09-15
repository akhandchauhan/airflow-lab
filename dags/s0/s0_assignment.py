"""Session 0 · assignment — your first DAG that passes every gate.

Boilerplate is ready. Build what the note (§6) asks:
  - one task returns your name; one task prints a greeting using it
  - wire them by passing the value (no >>)
Then run:
   airflow dags test s0_assignment 2026-01-01
    python -m pytest tests/ -v 
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s0_assignment",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-0"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():
    # TODO: get_name() -> greet(name); wire greet(get_name())
    @task
    def get_name() -> str:
        return "Heisenberg"

    @task
    def greet(name: str) -> None:
        print("All Hail", name)

    greet(get_name())


pipeline()
