"""Tier 1 - Exercise 3: fan out with dynamic task mapping, not hardcoded tasks.

The lecture notes fan out by writing N task objects by hand:

    start >> [product_api, checkout_api, login_api]

That only works when N is known at parse time. `.expand()` decides N at
*runtime*, from upstream data - one task definition becomes N independently
retryable task instances. This is the single most-asked "have you actually
shipped Airflow" question.

  .partial(...)  -> arguments that are the SAME for every mapped instance
  .expand(...)   -> the argument that VARIES, one instance per element
"""

from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="dynamic_multi_source_ingest",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    max_active_tasks=4,  # cap the fan-out so you don't hammer the source API
    tags=["tier-1", "dynamic-task-mapping"],
    default_args={
        "owner": "akhand",
        "retries": 3,
        "retry_delay": pendulum.duration(minutes=2),
    },
    doc_md=__doc__,
)
def dynamic_multi_source_ingest():
    @task
    def list_sources() -> list[dict]:
        """In production this reads a config table, not a literal."""
        return [
            {"name": "orders", "endpoint": "/v1/orders"},
            {"name": "users", "endpoint": "/v1/users"},
            {"name": "products", "endpoint": "/v1/products"},
            {"name": "checkout", "endpoint": "/v1/checkout"},
        ]

    @task
    def fetch(source: dict, bucket: str, **context) -> str:
        """One mapped instance per source. Idempotent: writes a dated partition.

        Using data_interval_start (not datetime.now()) is what makes a retry
        produce the same result as the original attempt.
        """
        start = context["data_interval_start"]
        end = context["data_interval_end"]
        partition = start.to_date_string()

        print(f"GET {source['endpoint']}?from={start}&to={end}")
        path = f"gs://{bucket}/raw/{source['name']}/dt={partition}/"
        print(f"wrote {path}")
        return path

    @task
    def register(paths: list[str]) -> None:
        """Fan-in: receives the collected list of every mapped return value."""
        for path in paths:
            print(f"registering partition {path}")
        print(f"{len(paths)} partitions loaded")

    register(fetch.partial(bucket="my-data-lake").expand(source=list_sources()))


dynamic_multi_source_ingest()
