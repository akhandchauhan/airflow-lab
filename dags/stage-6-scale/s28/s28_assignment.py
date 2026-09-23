"""Session 28 · assignment — isolate a conflicting dependency.

Boilerplate is ready. Build what the note (§8) asks:
  - a @task.virtualenv (or @task.external_python) that pins a dep conflicting with core
  - it computes a small scalar and returns it (imports live INSIDE the callable)
  - a plain @task downstream that consumes the scalar and asserts it
Then run:
    airflow dags test s28_assignment 2026-01-01
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s28_assignment",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-28"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():
    # TODO: isolate a conflicting dep in a venv task, return a scalar, consume it downstream
    @task
    def todo() -> None:
        print("replace me")

    todo()


pipeline()
