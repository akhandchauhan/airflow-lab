"""Tier 1 - Exercise 1: the XCom lecture DAG, rewritten in pure TaskFlow.

What changed vs the classic version:
  - No ti.xcom_push / ti.xcom_pull. A task's return value IS the XCom.
  - No explicit `>>` wiring. Passing one task's output into another
    creates the dependency (functional DAG building).

Airflow 3 syntax notes:
  - `from airflow.sdk import ...`   (not `from airflow import DAG`)
  - `schedule=`                     (`schedule_interval` was REMOVED in 3.0)
  - `data_interval_start`           (`execution_date` was REMOVED in 3.0)
"""

from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="taskflow_etl",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    tags=["tier-1", "taskflow"],
    default_args={
        "owner": "akhand",
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=5),
    },
    doc_md=__doc__,
)
def taskflow_etl():
    @task
    def create_customer(**context) -> str:
        """Write the customer slice and hand back only its location."""
        ds = context["data_interval_start"].to_date_string()
        path = f"/raw/customer/{ds}/"
        print(f"customer data written to {path}")
        return path

    @task
    def create_user(**context) -> str:
        ds = context["data_interval_start"].to_date_string()
        path = f"/raw/user/{ds}/"
        print(f"user data written to {path}")
        return path

    @task
    def create_product(**context) -> str:
        ds = context["data_interval_start"].to_date_string()
        path = f"/raw/product/{ds}/"
        print(f"product data written to {path}")
        return path

    @task
    def read_raw(customer: str, user: str, product: str) -> int:
        """Downstream receives the upstream return values as plain arguments."""
        for path in (customer, user, product):
            print(f"reading {path}")
        return 3

    # This single expression builds: [customer, user, product] >> read_raw
    read_raw(create_customer(), create_user(), create_product())


taskflow_etl()
