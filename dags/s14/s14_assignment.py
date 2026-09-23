"""Session 14 · assignment — react to a queue, with a daily time floor.

Boilerplate is ready. Build what the note (§7) asks:
  - an Asset watched by an AssetWatcher on a MessageQueueTrigger (event from OUTSIDE)
  - a consumer on AssetOrTimeSchedule so a silent queue still runs once a day
Then run:
    airflow dags test s14_assignment 2026-01-01
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s14_assignment",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-14"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():
    # TODO: watched Asset + AssetOrTimeSchedule consumer (asset-or-time floor)
    @task
    def todo() -> None:
        print("replace me")

    todo()


pipeline()
