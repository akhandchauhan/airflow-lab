"""Session 19 · assignment — cross-DAG wait without a held slot.

Boilerplate is ready. Build what the note (§9) asks:
  - ExternalTaskSensor on s12_producer.load_questions (reschedule or deferrable)
  - a @task.sensor returning a PokeReturnValue with an xcom_value
  - a final task that runs after both waits and prints the xcom_value
Then run:
    airflow dags test s19_assignment 2026-01-01
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s19_assignment",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-19"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():
    # TODO: ExternalTaskSensor (no plain poke) + @task.sensor + final task printing the xcom
    @task
    def todo() -> None:
        print("replace me")

    todo()


pipeline()
