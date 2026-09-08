from __future__ import annotations
import pendulum

from airflow.sdk import dag, task, TriggerRule
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


QUESTIONS = "bigquery-public-data.stackoverflow.posts_questions"
CAP = "2000000000"          # 2 GB max bytes billed per query — safety cap
BIG_BACKLOG = 5_000_000  # above this many unanswered -> the deep path


@dag(
    dag_id="s4_product_health_demo",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-4", "branching", "stackoverflow"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():
    @task
    def count_unanswered() -> int:
        hook = BigQueryHook(gcp_conn_id="google_cloud_default",
                            location="US", use_legacy_sql=False)
        row = hook.get_first(
            f"SELECT COUNT(*) FROM `{QUESTIONS}` WHERE answer_count = 0")
        return int(row[0])

    @task.short_circuit
    def has_backlog(backlog: int) -> bool:
        print(f"unanswered questions = {backlog:,}")
        return backlog > 0

    @task.branch
    def triage_by_size(backlog: int) -> str:
        return "deep_triage" if backlog > BIG_BACKLOG else "light_triage"   # returns a TASK_ID

    # heavy path: which tags are drowning? (unnests the pipe-delimited tags column)
    deep_path = BigQueryInsertJobOperator(
        task_id="deep_triage",
        gcp_conn_id="google_cloud_default",
        location="US",
        configuration={"query": {
            "query": f"""
                SELECT tag, COUNT(*) AS unanswered
                FROM `{QUESTIONS}`, UNNEST(SPLIT(tags, '|')) AS tag
                WHERE answer_count = 0
                GROUP BY tag ORDER BY unanswered DESC LIMIT 10
            """,
            "useLegacySql": False, "maximumBytesBilled": CAP,
        }},
    )
    # light path: just the headline number
    light_path = BigQueryInsertJobOperator(
        task_id="light_triage",
        gcp_conn_id="google_cloud_default",
        location="US",
        configuration={"query": {
            "query": f"SELECT COUNT(*) AS unanswered FROM `{QUESTIONS}` WHERE answer_count = 0",
            "useLegacySql": False, "maximumBytesBilled": CAP,
        }},
    )

    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def publish() -> None:               # join after the branch — survives the skipped path
        print("product-health summary published")

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def notify() -> None:                # always runs — success, failure, or skip
        print("daily run finished — status sent")

    backlog = count_unanswered()
    guard = has_backlog(backlog)
    decision = triage_by_size(backlog)

    guard >> decision >> [deep_path, light_path] >> publish() >> notify()


pipeline()
