"""Session 12 · examples — connections & hooks.

Boilerplate is ready. Build the note's reference here (§5): three tasks, all via hooks
on the Stack Overflow tables — BaseHook.get_connection to inspect the Connection,
BigQueryHook.get_first for one scalar, BigQueryHook.get_records for a few rows.
Instantiate every hook INSIDE the task, never at module top level. Then run:
    airflow dags test s12_examples 2026-01-01
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s12_examples",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-12"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():
    # TODO: inspect Connection via BaseHook.get_connection; read a scalar with
    # BigQueryHook.get_first; read rows with BigQueryHook.get_records — hooks built
    # inside the task body.
    @task
    def todo() -> None:
        print("replace me")

    todo()


pipeline()
