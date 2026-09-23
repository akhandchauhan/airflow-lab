"""Session 18 · examples — observability (plain DAG, no BigQuery).

Boilerplate is ready. Build the note's reference here (§7): a plain @task that emits
INFO/WARNING/ERROR via the stdlib logging logger so the lines land in the task log
(and, with remote_logging on, in object storage). Then run:
    airflow dags test s18_examples 2026-01-01
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s18_examples",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-18"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():
    # TODO: emit log.info/log.warning/log.error via logging.getLogger(__name__)
    @task
    def todo() -> None:
        print("replace me")

    todo()


pipeline()
