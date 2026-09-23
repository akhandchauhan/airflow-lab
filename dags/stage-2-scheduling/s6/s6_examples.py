"""Session 06 · examples — schedules & data intervals.

Boilerplate is ready. Build the note's reference here (§6): a @daily DAG whose task
prints logical_date, data_interval_start/end, and ds from get_current_context().
Then run:
    airflow dags test s6_examples 2026-01-01
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s6_examples",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    tags=["session-6"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():
    # TODO: print logical_date + data_interval_start/end + ds from get_current_context()
    @task
    def todo() -> None:
        print("replace me")

    todo()


pipeline()
