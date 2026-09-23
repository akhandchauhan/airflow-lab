"""Session 14 · examples — event-driven scheduling (AssetWatcher + queue).

Boilerplate is ready. Build the note's reference (§6): wire a MessageQueueTrigger into
an AssetWatcher on an Asset, schedule a DAG on it, and read the payload from extra. Run:
    airflow dags test s14_examples 2026-01-01
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s14_examples",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-14"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():
    # TODO: define the watched Asset + a consumer that reads the TriggerEvent payload
    @task
    def todo() -> None:
        print("replace me")

    todo()


pipeline()
