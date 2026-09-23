"""Session 16 · examples — data quality as a circuit breaker.

Boilerplate is ready. Build the note's reference here (§7): a quality gate on the
Stack Overflow pipeline — a BigQueryCheckOperator (non-empty), a
BigQueryValueCheckOperator (answer rate in band), and a @task.short_circuit that
skips publish quietly on an empty day. Then run:
    airflow dags test s16_examples 2026-01-01
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s16_examples",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-16"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():
    # TODO: add BigQueryCheckOperator + BigQueryValueCheckOperator + @task.short_circuit gate
    @task
    def todo() -> None:
        print("replace me")

    todo()


pipeline()
