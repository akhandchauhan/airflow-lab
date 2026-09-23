"""Capstone · examples — the StackPulse platform, piece by piece.

Boilerplate is ready. Use this file to prototype each layer of the capstone in
isolation before wiring them into capstone_assignment.py: asset trigger, capped
BigQuery ELT, branch on a scalar, dynamic-mapped tag loads, quality gate, Slack
alert. Keep every query capped and read-only on bigquery-public-data.stackoverflow.
Then run:
    airflow dags test capstone_examples 2026-01-01
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="capstone_examples",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["capstone"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():
    # TODO: prototype one capstone layer at a time (asset, ELT, branch, mapping,
    # quality gate, alert) before assembling the full pipeline.
    @task
    def todo() -> None:
        print("replace me")

    todo()


pipeline()
