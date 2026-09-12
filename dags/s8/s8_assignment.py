"""Session 8 · assignment — fan-out / fan-in over regions.

Boilerplate is ready. Build what the note (§8) asks:
  - a task that returns a list of regions (produced at run time)
  - a mapped task: process each region + one constant via .partial()
  - a reduce task that totals the per-region numbers
Then run:
    airflow dags test s8_assignment 2026-01-01
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s8_assignment",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-8"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():
    # TODO: list_regions -> process.partial(year=...).expand(region=...) -> summarize
    @task
    def todo() -> None:
        print("replace me")

    todo()


pipeline()
