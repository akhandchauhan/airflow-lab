from __future__ import annotations

import pendulum
from airflow.sdk import dag
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator, BigQueryCheckOperator

PROJECT = "dogwood-abbey-490606-e8"
CAP = 50_000_000_000  # 2 GB cost cap on every job


@dag(
    dag_id="p3_medallion",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    tags=["practical-3", "medallion"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():

    # raw layer
    raw_load = BigQueryInsertJobOperator(
        task_id="raw_load",
        gcp_conn_id="google_cloud_default",
        configuration={
            "query": {
                "query": f"CALL `{PROJECT}.raw.raw_load`();",
                "useLegacySql": False,
                "maximumBytesBilled": CAP,
            }
        },
    )

    # bronze layer
    bronze_clean = BigQueryInsertJobOperator(
        task_id="bronze_clean",
        gcp_conn_id="google_cloud_default",
        configuration={
            "query": {
                "query": f"CALL `{PROJECT}.bronze.questions_load`();",
                "useLegacySql": False,
                "maximumBytesBilled": CAP,
            }
        },
    )
    # ── data-quality gate ─────────────────────────────────────────────
    dq_gate = BigQueryCheckOperator(
        task_id="dq_gate",
        gcp_conn_id="google_cloud_default",
        use_legacy_sql=False,
        sql=f"SELECT COUNT(*) > 0 FROM `{PROJECT}.bronze.questions_clean`",
    )

    # ── gold layer ───────────────────────────────────────────────────
    gold_marts = BigQueryInsertJobOperator(
        task_id="gold_marts",
        gcp_conn_id="google_cloud_default",
        configuration={
            "query": {
                "query": f"CALL `{PROJECT}.gold.questions_load`();",
                "useLegacySql": False,
                "maximumBytesBilled": CAP,
            }
        },
    )

    raw_load >> bronze_clean >> dq_gate >> gold_marts


pipeline()
