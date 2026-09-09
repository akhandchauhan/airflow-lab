"""Session 5 · Byte 5.3 — a plain DAG that recovers from a flaky task.

fetch fails ~half the time; with retries=3 + a 10s retry_delay it almost always
recovers before the run goes red. report runs after it.
    airflow dags test s5_task3 2026-01-01
"""
from __future__ import annotations

from datetime import timedelta
import random

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s5_task3",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-5", "retries"],
    default_args={"owner": "akhand", "retries": 3, "retry_delay": timedelta(seconds=10)},
)
def pipeline():

    @task
    def fetch() -> str:
        if random.random() < 0.5:          # fails ~half the time
            raise RuntimeError("network blip — will retry")
        return "ok"

    @task
    def report(status: str) -> None:
        print(f"done — fetch returned {status}")

    report(fetch())


pipeline()
