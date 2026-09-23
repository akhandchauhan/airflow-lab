"""Session 10 · examples — asset logic (AssetAll / AssetAny / AssetAlias).

Boilerplate is ready. Build the note's reference (§6): nest (a & b) | c in schedule=,
then add an AssetAlias producer that resolves a runtime Asset via outlet_events. Run:
    airflow dags test s10_examples 2026-01-01
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s10_examples",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-10"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():
    # TODO: build the nested-condition consumer + the AssetAlias runtime producer
    @task
    def todo() -> None:
        print("replace me")

    todo()


pipeline()
