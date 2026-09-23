"""Session 13 · assignment — a tiered mart on a nested asset condition.

Boilerplate is ready. Build what the note (§7) asks:
  - schedule a mart on (questions & answers) | tags_reload using the & and | operators
  - add one AssetAlias producer whose real Asset URI is decided at run time
Then run:
    airflow dags test s13_assignment 2026-01-01
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s13_assignment",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-13"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():
    # TODO: nested schedule condition + AssetAlias runtime resolution
    @task
    def todo() -> None:
        print("replace me")

    todo()


pipeline()
