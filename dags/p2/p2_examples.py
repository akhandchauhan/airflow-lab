"""Practical 2 · examples — params + dynamic mapping on Stack Overflow.

Boilerplate is ready. Prototype the note's reference here: a Param supplies a date
(or date list); a task expands that into per-date mapped loads with .partial()/.expand();
each mapped task runs a capped BigQuery job scoped to its date; a reduce task sums.
Keep every query capped and filtered on creation_date. Then run:
    airflow dags test p2_examples 2026-01-01
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="p2_examples",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["practical-2"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():
    # TODO: read a date Param, expand a capped per-date BigQuery load with
    # .partial()/.expand(), then reduce the per-date counts.
    @task
    def todo() -> None:
        print("replace me")

    todo()


pipeline()
