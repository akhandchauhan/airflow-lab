from __future__ import annotations

import pendulum
from airflow.sdk import dag, task
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

SRC = "bigquery-public-data.austin_bikeshare.bikeshare_trips"
CAP = "100000000"   # 100 MB max bytes billed per query — safety cap


@dag(
    dag_id="p1_bq_hello_demo",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["practical", "p1", "bigquery"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():

    count_trips = BigQueryInsertJobOperator(
        task_id="count_trips",
        gcp_conn_id="google_cloud_default",
        location="US",
        configuration={
            "query": {
                "query": f"SELECT COUNT(*) AS trips FROM `{SRC}`",
                "useLegacySql": False,
                "maximumBytesBilled": CAP,   # rejects the query if it would exceed the cap
            }
        },
    )

    @task
    def done() -> None:
        print("BigQuery count job finished")

    count_trips >> done()


pipeline()