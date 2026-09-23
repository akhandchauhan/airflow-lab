"""Session 08 · examples — timetables (trigger vs data-interval)."""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s8_examples",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-8"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():
    # TODO: build the note's reference (§6) — schedule with CronTriggerTimetable and
    # print data_interval_start/end to show start == end (a point in time).
    @task
    def todo() -> None:
        print("replace me")

    todo()


pipeline()
