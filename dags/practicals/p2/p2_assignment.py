"""Practical 2 · assignment — parametrized, dynamic incremental load.

Boilerplate is ready. Build the spec from docs/course/P2-parametrized-dynamic-load.md:
a Param declares which dates to load; a task turns that into a list of dates;
a mapped task incrementally loads each date from posts_questions with a capped,
date-filtered BigQuery job; a reduce task reports the totals. Every query capped;
no SELECT *. Then run:
    airflow dags test p2_assignment 2026-01-01
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="p2_assignment",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["practical-2"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():
    # TODO: Param -> list of dates -> mapped capped per-date load -> reduce.
    @task
    def todo() -> None:
        print("replace me")

    todo()


pipeline()
