"""Session 5 · Byte 5.2 — retry_delay.

Boilerplate is ready. Replace the stub task inside pipeline() with a task that
has both retries and a retry_delay (see the note, §2), then run:
    airflow dags test s5_task2 2026-01-01
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s5_task2",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-5"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():
    # TODO byte 5.2: replace this stub. Add retry_delay=timedelta(seconds=10)
    #   (remember: from datetime import timedelta).
    @task
    def todo() -> None:
        print("replace me")

    todo()


pipeline()
