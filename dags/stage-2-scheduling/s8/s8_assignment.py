"""Session 08 · assignment — a custom business-days-only Timetable."""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s8_assignment",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-8"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():
    # TODO: build what the note (§7) asks — a custom Timetable (weekdays only),
    # registered via an AirflowPlugin, applied with schedule=YourTimetable().
    @task
    def todo() -> None:
        print("replace me")

    todo()


pipeline()
