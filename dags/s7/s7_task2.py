"""Session 7 · Byte 6.2 — pass a value at trigger time.

Boilerplate is ready. Give @dag a param with a default, read it in the task, then
override it (see the note, §2):
    airflow dags test s7_task2 2026-01-01 --conf '{"name": "Panda"}'
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s7_task2",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-7"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():
    # TODO byte 6.2: one param with a default; print it; then override with --conf.
    @task
    def todo() -> None:
        print("replace me")

    todo()


pipeline()
