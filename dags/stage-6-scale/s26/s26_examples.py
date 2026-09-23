"""Session 26 · examples — setup & teardown tasks.

Boilerplate is ready. Build the note's reference here (§7): a @setup that creates a scratch
BigQuery dataset, a work @task that queries Stack Overflow, and a @teardown (.as_teardown)
that always drops the dataset — even if the work fails. Run:
    airflow dags test s26_examples 2026-01-01
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s26_examples",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-26"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():
    # TODO: add @setup create_scratch -> work query -> @teardown drop_scratch (.as_teardown)
    @task
    def todo() -> None:
        print("replace me")

    todo()


pipeline()
