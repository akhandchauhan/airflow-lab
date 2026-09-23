"""Session 25 · assignment — two-DAG chain with different schedules.

Boilerplate is ready. Build what the note (§7) asks:
  - s25_upstream @daily (midnight) with a prepare_data task
  - s25_downstream at 06:00, waiting on prepare_data via ExternalTaskSensor + execution_delta
  - upstream also fires an ad-hoc downstream run via TriggerDagRunOperator with conf
Then run:
    airflow dags test s25_assignment 2026-01-01
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s25_assignment",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-25"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():
    # TODO: ExternalTaskSensor with the right execution_delta + a TriggerDagRunOperator
    @task
    def todo() -> None:
        print("replace me")

    todo()


pipeline()
