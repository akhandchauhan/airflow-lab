"""Session 6 · Byte 6.5 — run the BigQuery reference DAG.

Boilerplate is ready. Paste the reference DAG from the note (§5) here — a capped
BigQueryInsertJobOperator, a BigQueryCheckOperator gate, and a BigQueryHook read —
then run (needs the google_cloud_default connection):
    airflow dags test s6_task1 2026-01-01
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s6_task1",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-6"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():
    # TODO byte 6.5: replace this stub with the §5 reference (InsertJob -> Check -> Hook).
    @task
    def todo() -> None:
        print("replace me")

    todo()


pipeline()
