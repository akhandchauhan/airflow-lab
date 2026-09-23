"""Session 19 · examples — sensors & deferrable operators.

Boilerplate is ready. Build the note's reference here (§8): a @task.sensor returning a
PokeReturnValue plus a deferrable FileSensor, then a process task. Run:
    airflow dags test s19_examples 2026-01-01
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s19_examples",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-19"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():
    # TODO: add a @task.sensor (PokeReturnValue) + a deferrable FileSensor, then a process task
    @task
    def todo() -> None:
        print("replace me")

    todo()


pipeline()
