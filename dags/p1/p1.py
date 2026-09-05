from __future__ import annotations

import pendulum
from airflow.sdk import dag, task, task_group
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryCheckOperator,
)
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

SRC = "bigquery-public-data.austin_bikeshare.bikeshare_trips"
CAP = "100000000"   # 100 MB max bytes billed per query — safety cap


@dag(
    dag_id="p1_bigquery_hello",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["practical", "p1", "bigquery"],
    default_args={"owner": "akhand", "retries": 2},
)
def pipeline():

    @task_group(group_id="metrics")
    def compute_metrics():
        # data-quality gate: COUNT(*) = 0 is falsy -> this task fails -> run stops.
        # COUNT(*) scans 0 bytes, so no maximumBytesBilled needed here.
        not_empty = BigQueryCheckOperator(
            task_id="not_empty",
            gcp_conn_id="google_cloud_default",
            location="US",
            sql=f"SELECT COUNT(*) FROM `{SRC}`",
            use_legacy_sql=False,
        )

        # metric 2: top-5 start stations by trip count (scans one column, capped)
        top_stations = BigQueryInsertJobOperator(
            task_id="top_start_stations",
            gcp_conn_id="google_cloud_default",
            location="US",
            configuration={
                "query": {
                    "query": (
                        f"SELECT start_station_name, COUNT(*) AS trips "
                        f"FROM `{SRC}` "
                        f"GROUP BY start_station_name "
                        f"ORDER BY trips DESC "
                        f"LIMIT 5"
                    ),
                    "useLegacySql": False,
                    "maximumBytesBilled": CAP,
                }
            },
        )

        # metric 1: total trips — returned value is auto-pushed to XCom
        @task
        def total_trips() -> int:
            hook = BigQueryHook(gcp_conn_id="google_cloud_default",
                                location="US", use_legacy_sql=False)
            row = hook.get_first(
                f"SELECT COUNT(*) AS trips FROM `{SRC}`")  # COUNT = 0 bytes
            return int(row[0])

        trip_count = total_trips()
        # gate first, then metrics; total_trips runs last so the group output is the count
        not_empty >> top_stations >> trip_count
        return trip_count

    @task
    def summarize(trips: int) -> None:
        print(
            f"[P1] austin bikeshare — total trips = {trips:,}; top-5 start stations computed")

    trip_count = compute_metrics()
    summarize(trip_count)


pipeline()
