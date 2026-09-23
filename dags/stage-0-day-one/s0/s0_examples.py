"""Session 00 · examples — first DAG & testing.

Boilerplate is ready. Build the note's reference (§5): say_hello -> show, wired by
passing the value. Then prove it three ways:
    python dags/stage-0-day-one/s0/s0_examples.py
    airflow dags test s0_examples 2026-01-01
    python -m pytest tests/ -v
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s0_examples",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-0"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():
    # TODO: say_hello() returns a string; show(msg) prints it; wire show(say_hello())
    @task
    def say_hello() -> str:
        return "Hola Amigos, Salamanca Brothers"

    @task
    def show(msg: str) -> None:
        print(msg)

    show(say_hello())


pipeline()
