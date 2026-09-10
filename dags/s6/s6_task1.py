from __future__ import annotations
import pendulum
from airflow.sdk import dag, task
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryCheckOperator,
)
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook


CONN = "google_cloud_default"
QUESTIONS = "bigquery-public-data.stackoverflow.posts_questions"
CAP = "2000000000"


@dag(
    dag_id="s6_task1",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-6", "bigquery"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():

    run_count = BigQueryInsertJobOperator(
        task_id="run_count",
        gcp_conn_id=CONN,
        location='US',
        configuration={"query": {
            "query": f"SELECT COUNT(*) FROM `{QUESTIONS}` WHERE answer_count = 0",
            "useLegacySql": False,
            "maximumBytesBilled": CAP,
        }},
    )
    check_has_rows = BigQueryCheckOperator(
        task_id='check_has_rows',
        gcp_conn_id=CONN,
        use_legacy_sql=False,
        sql=f"SELECT COUNT(*) FROM `{QUESTIONS}` WHERE answer_count = 0",
    )

    @task
    def log_scaler() -> None:
        hook = BigQueryHook(
            gcp_conn_id=CONN, use_legacy_sql=False, location='US')
        n = hook.get_first(
            f"SELECT COUNT(*) FROM `{QUESTIONS}` WHERE answer_count = 0")[0]
        print(f"answered questions = {int(n):,}")

    run_count >> check_has_rows >> log_scaler()


pipeline()
