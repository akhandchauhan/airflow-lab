"""Session 25 · examples — cross-DAG dependencies.

Boilerplate is ready. Build the note's reference here (§5): an ingest DAG that fires a
report DAG via TriggerDagRunOperator, plus an ExternalTaskSensor on the pull side. Run:
    airflow dags test s25_examples 2026-01-01
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s25_examples",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-25"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():
    # TODO: TriggerDagRunOperator (push) + ExternalTaskSensor (pull), mind the logical date
    @task
    def todo() -> None:
        print("replace me")

    todo()


pipeline()
