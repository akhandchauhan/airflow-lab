"""Capstone · assignment — the whole StackPulse Product Health pipeline.

Boilerplate is ready. Build the full spec from docs/course/capstone.md: an
asset-triggered, cost-capped BigQuery ELT that branches on data freshness,
dynamic-maps the per-tag rollups, gates on data quality, and alerts Slack on
failure — tested and CI-green. Every query capped; hooks built inside tasks. Run:
    airflow dags test capstone_assignment 2026-01-01
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="capstone_assignment",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["capstone"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():
    # TODO: assemble every layer into one pipeline per docs/course/capstone.md.
    @task
    def todo() -> None:
        print("replace me")

    todo()


pipeline()
