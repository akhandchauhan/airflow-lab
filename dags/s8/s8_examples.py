"""Session 8 · examples — dynamic task mapping.

Boilerplate is ready. Build the note's reference here (§2–§7): a task that returns a
list, a mapped task via .partial(...).expand(...), and a reduce task. Then run:
    airflow dags test s8_examples 2026-01-01
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s8_examples",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-8"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():
    # TODO: list_files -> process.partial(...).expand(...) -> summarize
    @task
    def todo() -> None:
        print("replace me")

    todo()


pipeline()
