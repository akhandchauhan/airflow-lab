"""Session 16 · assignment — build a data-quality gate.

Boilerplate is ready. Build what the note (§8) asks:
  - a declarative check (SQLColumnCheckOperator / SQLTableCheckOperator or BQ subclass)
  - a value/threshold check (BigQueryCheckOperator / BigQueryValueCheckOperator) that FAILS on bad data
  - a @task.short_circuit that SKIPS publish quietly on a legitimately empty day
  - a final @task that publishes only when all gates pass; cap every query, no SELECT *
Then run:
    airflow dags test s16_assignment 2026-01-01
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s16_assignment",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-16"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():
    # TODO: gate a Product Health metric behind checks before it publishes
    @task
    def todo() -> None:
        print("replace me")

    todo()


pipeline()
