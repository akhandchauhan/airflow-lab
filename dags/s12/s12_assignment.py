"""Session 12 · assignment — a two-input mart (orders & refunds).

Boilerplate is ready. Build what the note (§8) asks:
  - prod_a outlets [orders]; prod_b outlets [refunds]
  - mart is scheduled on (orders & refunds) and logs its triggering_asset_events
Then run:
    airflow dags test s12_prod_a 2026-01-01
"""
from __future__ import annotations

import pendulum
from airflow.sdk import Asset, dag, task

orders = Asset(uri="file:///data/orders.csv", name="orders")
refunds = Asset(uri="file:///data/refunds.csv", name="refunds")

COMMON = {
    "start_date": pendulum.datetime(2026, 1, 1, tz="UTC"),
    "catchup": False,
    "tags": ["session-12"],
    "default_args": {"owner": "akhand", "retries": 1},
}


@dag(dag_id="s12_prod_a", schedule="@daily", **COMMON)
def prod_a():
    # TODO: outlets=[orders]
    @task
    def todo() -> None:
        print("replace me")

    todo()


@dag(dag_id="s12_prod_b", schedule="@daily", **COMMON)
def prod_b():
    # TODO: outlets=[refunds]
    @task
    def todo() -> None:
        print("replace me")

    todo()


@dag(dag_id="s12_mart", schedule=(orders & refunds), **COMMON)
def mart():
    # TODO: log context["triggering_asset_events"]
    @task
    def todo() -> None:
        print("replace me")

    todo()


prod_a()
prod_b()
mart()
