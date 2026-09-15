"""Session 9 · assignment — report the processing window.

Boilerplate is ready. Build what the note (§7) asks:
  - schedule @hourly, catchup=False
  - one task prints "processing window <start> -> <end>" from the data interval
  - one task prints {{ ds }} (the short date label)
Then run:
    airflow dags test s9_assignment 2026-01-01
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s9_assignment",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="@hourly",
    catchup=False,
    tags=["session-9"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():
    # TODO: print the data_interval window, and ds
    @task
    def todo() -> None:
        print("replace me")

    todo()


pipeline()
