"""Session 27 · examples — reliability knobs & deadline alerts.

Boilerplate is ready. Build the note's reference here (§8): a cost-capped BigQuery task with
execution_timeout + retry_exponential_backoff + max_retry_delay, a DAG-level dagrun_timeout,
and a DeadlineAlert. Run:
    airflow dags test s27_examples 2026-01-01
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s27_examples",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-27"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():
    # TODO: task with execution_timeout + exp backoff + max_retry_delay; add dagrun_timeout + DeadlineAlert
    @task
    def todo() -> None:
        print("replace me")

    todo()


pipeline()
