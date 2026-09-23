# Practical P2 · Parametrized, dynamic incremental load

**Applies:** Session 05 (Params — validated input at trigger time) + Session 15 (Dynamic task mapping — fan out over a run-time list), against the **real** Stack Overflow spine. You take the two Stage-1/Stage-4 mechanics and combine them into the pattern every incremental warehouse load uses in production: *the run tells the DAG which dates to load, and the DAG spawns one capped BigQuery job per date.*

**Dataset:** `bigquery-public-data.stackoverflow.posts_questions` (the spine, R24) in multi-region **US**. Workhorse columns: `creation_date` (the date you slice on) and `answer_count`. `posts_questions.tags` is pipe-delimited. The table is historical — pick dates that exist in the data (2022 range), not today.

**What you'll do:** declare a **date** input as a Param, turn it into a list at run time, and **dynamic-map** a cost-capped BigQuery job over that list — one task instance per date, each loading/aggregating exactly its own day. Change the dates at trigger time with no code edit; add a date and one more mapped instance appears.

---

## 1. Why this pattern exists

An incremental load processes **one window at a time** — today's data, or a named backfill range — not the whole table every run. Two questions have to be answered per run, and they are exactly the two sessions you just did. *Which windows?* is **input** — it changes per run, so it is a **Param** (Session 05), validated at trigger, not hardcoded. *How many parallel loads?* is **data** — it is however many windows the input named, so it is **dynamic task mapping** (Session 15), where N is decided at run time and each window becomes its own independent, retryable task instance. Combine them and you have a single DAG that loads one day on a normal run and ten days on a backfill, driven entirely by `--conf`, with each day isolated so one bad date retries alone.

The mechanism is the same parse-time / run-time split both sessions hammered: at **parse time** the DAG knows only that a mapped load *exists* and that a `dates` param is *declared*; at **run time** the run's `conf` supplies the actual dates, and Airflow creates one mapped instance per date.

```
PARSE TIME                                 RUN TIME (a triggered run)
----------                                 --------------------------
params={"dates": Param([...])}             conf {"dates": ["d1","d2","d3"]} merged + validated
load.expand(configuration=build_jobs())    build_jobs() -> 3 job configs -> 3 mapped instances
  → "load is mapped", N unknown              load[0]=d1  load[1]=d2  load[2]=d3  (parallel, capped)
```

---

## 2. Cost safety (read before running — R16/R17)

Every query bills **your** project for **bytes scanned**. Each mapped instance is a real query, so the cap goes on **every** one:

- **`maximumBytesBilled` on every job** — a string in bytes; BigQuery **rejects** a query that would exceed it instead of billing you. Use `"2000000000"` (2 GB) here and tighten from Job history.
- **Filter on `creation_date`** so each instance scans one day's slice of the column, not all history — this, not the cap, is what actually keeps it cheap.
- **`"useLegacySql": False`** on every query; **never `SELECT *`**; prefer `COUNT`/aggregates.
- **Orchestrate, don't compute (R20):** `BigQueryInsertJobOperator`'s XCom is the **job id**, not rows — the reduce step counts jobs, or a `@task` pulls back only a small scalar via `BigQueryHook.get_first`.
- **Pin the provider** (`apache-airflow-providers-google`); creds come from the `google_cloud_default` Connection (P1/Session 12) — no key in the file (R18/R19).

---

## 3. The two mechanics, together

- **Param → list.** Declare `dates` as an array Param with a small default; read it in a task with `get_current_context()["params"]`; return the per-date **job configurations** built from it. Because it is validated at trigger, a malformed `dates` is rejected before any job runs (Session 05).
- **Map the operator.** `BigQueryInsertJobOperator.partial(...)` freezes the constants (`gcp_conn_id`, `location`), and `.expand(configuration=...)` fans out one instance per config (Session 15). Mapping the operator's `configuration` argument is the classic-operator equivalent of the `@task.expand` you saw over a list.

---

## 4. Complete runnable reference DAG

A whole file: a `dates` Param → a task that builds one capped per-date job config from it → a mapped `BigQueryInsertJobOperator` (one instance per date) → a reduce `@task`. Read-only aggregate per date, every query capped and `creation_date`-filtered. Names kept distinct (R12): `build_jobs` (verb), `count_by_date` (the mapped task_id, a noun-ish role), `summarize` (verb), the mapped variable `job_ids` is its role.

```python
# dags/practicals/p2/p2_examples.py
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task, Param, get_current_context
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

CONN = "google_cloud_default"
QUESTIONS = "bigquery-public-data.stackoverflow.posts_questions"
CAP = "2000000000"          # 2 GB max bytes billed per query — reject anything bigger


@dag(
    dag_id="p2_examples",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["practical-2"],
    params={
        # dates that exist in the historical data; override at trigger with --conf
        "dates": Param(
            ["2022-08-01", "2022-08-02", "2022-08-03"],
            type="array",
            items={"type": "string"},
            minItems=1,
        ),
    },
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():

    @task
    def build_jobs() -> list[dict]:
        # read the validated Param and turn each date into one capped job config
        inputs = get_current_context()["params"]
        return [
            {
                "query": {
                    "query": (
                        f"SELECT COUNT(*) AS n FROM `{QUESTIONS}` "
                        f"WHERE DATE(creation_date) = '{day}'"     # scans one day of one column
                    ),
                    "useLegacySql": False,
                    "maximumBytesBilled": CAP,
                }
            }
            for day in inputs["dates"]
        ]

    # one mapped instance per date; constants frozen with .partial, configs fanned with .expand
    count_by_date = BigQueryInsertJobOperator.partial(
        task_id="count_by_date",
        gcp_conn_id=CONN,
        location="US",
    ).expand(configuration=build_jobs())

    @task
    def summarize(job_ids: list) -> None:
        # XCom is the job id, not rows (R20) — the reduce just proves the fan-out
        print(f"launched {len(job_ids)} capped per-date load jobs")

    summarize(count_by_date.output)


pipeline()
```

Run it three ways:

```bash
python dags/practicals/p2/p2_examples.py                                       # parses
airflow dags test p2_examples 2026-01-01                            # 3 dates (the default) -> [3] mapped
airflow dags test p2_examples 2026-01-01 --conf '{"dates": ["2022-08-01", "2022-08-02"]}'
```

In the Graph, `count_by_date` shows as a mapped node with one square per date; `summarize` runs once over all of them. Add a date to `--conf` and rerun — one more instance appears, no change to the mapped task. Check **BigQuery Job history**: each job scans only the `creation_date` slice, well under the 2 GB cap.

---

## 5. Build spec — your challenge (no solution)

**File:** `dags/practicals/p2/p2_assignment.py` · **dag_id:** `p2_assignment`

Build a parametrized, dynamic **incremental load** that actually lands data — one capped BigQuery job per date, driven by a Param.

**The problem:**

- Declare a Param that names **which dates to load** — an array of date strings (`minItems=1`), plus an optional scalar like `dataset` or `min_answer_count`. Read them via `get_current_context()["params"]`.
- A task turns the Param into the per-date work list at **run time** (not a literal in `.expand()`).
- A **mapped** `BigQueryInsertJobOperator` runs **one capped job per date** — each **incrementally loads** its day (e.g. `CREATE OR REPLACE TABLE`/`WRITE_TRUNCATE` into that date's partition of a mart in your own dataset, or a capped per-date aggregate if you are read-only). Each query is `creation_date`-filtered and carries `maximumBytesBilled`.
- A **reduce** task runs once over all mapped outputs and logs a one-line summary (dates loaded / total).

**Constraints:**

- Params (Session 05) supply the dates; dynamic mapping (Session 15) supplies the fan-out — the mapped list comes from the **upstream task's output**.
- Every query capped, `"useLegacySql": False`, no `SELECT *`, filtered on `creation_date` (R16/R17).
- The load is **idempotent** — rerunning a date lands identical rows (`WRITE_TRUNCATE` on the partition / `MERGE`, Session 13), so a mapped instance can retry alone safely (Session 15 §9).
- Credentials from the `google_cloud_default` Connection; nothing grep-able in the file.
- Names distinct (R12); passes the integrity gates: `tags`, real `owner`, `retries >= 1` (R15).

**Acceptance criteria:**

- `python dags/practicals/p2/p2_assignment.py` parses (prints nothing).
- `airflow dags test p2_assignment 2026-01-01` runs green on the default dates; the Graph shows the load as a mapped **[N]** node, one instance per date, and the reduce runs once.
- `airflow dags test p2_assignment 2026-01-01 --conf '{"dates": ["2022-08-01"]}'` runs a **single** instance; adding a date adds an instance with no code change.
- `--conf '{"dates": []}'` is **rejected** at trigger (`minItems=1`).
- Rerunning the same date lands **identical** rows (idempotent) — not doubled.
- **BigQuery Job history** shows every query within the cap; `python -m pytest tests/ -v` stays green.

**Nudge (only if stuck):** map the operator's `configuration` argument — `BigQueryInsertJobOperator.partial(gcp_conn_id=..., location=...).expand(configuration=build_jobs())` — where `build_jobs()` returns a **list of config dicts** built from the `dates` param. You write no loop over dates yourself; `.expand` is the loop Airflow runs.

---

## 6. Production tip — the incremental load's two classic corruptions

- **A non-idempotent per-date load turns a retry into double-counted data.** Each mapped instance can retry alone (Session 15 §9); if it *appends* rather than overwriting its date's partition, a single retried square silently doubles that day. Key every write to its date — `WRITE_TRUNCATE` the target partition or `MERGE` — so the same date always lands the same rows, however many times it runs.
- **An unbounded `dates` param is an uncapped fan-out.** A backfill `--conf` with 3,000 dates spawns 3,000 real task instances *and* 3,000 queries — a scheduler and a billing incident at once. Bound the input (`maxItems`, or `max_map_length` on the mapped task, default 1024) and batch large ranges into chunks, exactly the guardrail Session 15 called out. The Param is the dial; put a sane limit on it before someone spins it to eleven at 2am.

---

## 7. Verify + commit

```bash
python dags/practicals/p2/p2_assignment.py
airflow dags test p2_assignment 2026-01-01
airflow dags test p2_assignment 2026-01-01 --conf '{"dates": ["2022-08-01", "2022-08-02"]}'
python -m pytest tests/ -v
ruff check dags/ include/ tests/ --select E,F,AIR3
```

Done when: the DAG loads the default dates green, the mapped node shows `[N]` and tracks the `dates` param, a rerun is idempotent, and Job history shows every query within the cap. Tick **P2** in `docs/course/README.md`.

Sources:
[Params — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/params.html),
[Dynamic Task Mapping — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html),
[Airflow BigQuery operators](https://airflow.apache.org/docs/apache-airflow-providers-google/stable/operators/cloud/bigquery.html),
[BigQuery cost best practices](https://docs.cloud.google.com/bigquery/docs/best-practices-costs)
