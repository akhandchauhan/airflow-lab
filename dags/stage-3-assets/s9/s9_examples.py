"""Session 09 · examples — assets (producer -> asset -> consumer).

Boilerplate is ready. Build the note's reference (§7): put outlets=[questions] on the
producer task, and read triggering_asset_events in the consumer. Then run:
    airflow dags test s9_producer 2026-01-01
"""
from __future__ import annotations

import pendulum
from airflow.sdk import Asset, dag, task

# the shared data dependency, imported by both DAGs
questions = Asset(uri="file:///data/questions.csv", name="questions")


@dag(
    dag_id="s9_producer",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    tags=["session-9"],
    default_args={"owner": "akhand", "retries": 1},
)
def producer():
    # TODO: add outlets=[questions] to the task that loads the data
    @task
    def todo() -> None:
        print("replace me")

    todo()


@dag(
    dag_id="s9_consumer",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=[questions],
    catchup=False,
    tags=["session-9"],
    default_args={"owner": "akhand", "retries": 1},
)
def consumer():
    # TODO: read context["triggering_asset_events"]; build the report
    @task
    def todo() -> None:
        print("replace me")

    todo()


producer()
consumer()
