"""Session 5 · Byte 5.1 — retries.

Boilerplate is ready. Replace the stub task inside pipeline() with your retries
task (see the note, §1), then run:
    airflow dags test s5_task1 2026-01-01
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s5_task1",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-5"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():
    # TODO byte 5.1: replace this stub with your retries task.
    @task
    def todo() -> None:
        print("replace me")

    todo()


pipeline()
