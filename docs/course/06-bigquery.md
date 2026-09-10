# Session 06 · Inside the Warehouse

**BigQuery, ground-up → advanced** — what actually happens when you run a query, so
you never blow a budget or wait on a scan you didn't need.

> ## 📟 Cold open
> You wired BigQuery back in P1 and ran a few queries. Then a teammate ran
> `SELECT * FROM a_partitioned_table` with no filter, scanned 4 TB, and turned a
> $0 dashboard into a real bill. The difference between the two was **knowing what
> BigQuery charges for**. This session is that knowledge.

This is a **reference** session — mostly reading, one runnable DAG, one small build.
Back to BigQuery (you'll need the `google_cloud_default` connection from P1).

---

## 1. What BigQuery actually is

A **serverless, columnar data warehouse**. You never manage a server — you hand it
SQL, it finds machines to run it, and hands back rows.

```
   your SQL ─▶ Dremel (compute)  ⇄  Jupiter network  ⇄  Colossus (storage)
                 └─ slots do the work         └─ Capacitor columnar files
```

The one idea that explains everything else — **storage and compute are separate**:

- **Storage** = **Colossus**, Google's distributed filesystem. Your table is kept as
  compressed, encrypted, replicated **columnar** files (format: **Capacitor**).
- **Compute** = **Dremel**, which shreds a query into a tree of **slots** (units of
  CPU+RAM). Slots read from storage over the **Jupiter** petabit network.
- They scale **independently**. You're not paying for a running cluster — you pay for
  the storage you keep and the compute a query uses, separately.

Hierarchy you organize data in:

| Level | What it is |
|---|---|
| **Project** | billing + IAM boundary (e.g. `dogwood-abbey-490606-e8`) |
| **Dataset** | a named container of tables, pinned to a **location** (`US`, `EU`, a region) |
| **Table** | the data — native, external, view, or materialized view |

A query and the datasets it touches must be in the **same location**. `US` is a
multi-region; public data lives there, which is why P1 used `location="US"`.

---

## 2. How you pay (and why columnar matters)

Two things cost money: **storage** and **compute**. They're billed separately.

**Compute — two pricing models:**

| Model | You pay for | When to use |
|---|---|---|
| **On-demand** | **bytes scanned** by each query (per TiB) | default; spiky/low volume |
| **Capacity (Editions)** | **slots** you reserve (per slot-hour) | steady heavy workloads |

On on-demand (what you're on), **the query's cost = how many bytes it reads**. This
is where columnar storage decides your bill:

- BigQuery stores each **column** separately. A query reads **only the columns it
  names** — `SELECT id, answer_count` never touches `body`.
- `SELECT *` reads **every** column → the most expensive thing you can write.
- `SELECT COUNT(*)` reads **0 bytes** (metadata only) — free.
- **`LIMIT` does NOT reduce cost** on a non-clustered table — BigQuery scans the
  column, *then* limits. `LIMIT 10` on a huge table still scans the whole column.

**Storage:**

- **Active** vs **long-term**: a table not modified for **90 days** drops to **50%
  off** storage price automatically. No action needed.
- **Free tier:** **1 TiB of query bytes/month** and **10 GiB of storage/month** free.

---

## 3. Making queries cheap and fast (partitioning + clustering)

The lever that beats every other trick: **read less data**. Two table features do that.

**Partitioning** — split a table into segments by a column (usually a date). A query
that filters on that column skips whole segments — **partition pruning**.

```sql
-- table partitioned by DATE(creation_date):
SELECT ... FROM t WHERE DATE(creation_date) = '2022-08-01'   -- scans 1 day, not all history
```

- Types: **time-unit** (date/timestamp), **ingestion-time** (`_PARTITIONTIME`), **integer-range**.
- Rule of thumb: partition tables **> ~100 GB**. A filter on the partition column is
  what makes it pay off — no filter, no pruning.

**Clustering** — sort the data *within* each partition by up to 4 columns. A filter on
a clustered column scans only the relevant blocks — **block pruning**.

```sql
-- clustered by tag: a WHERE tag = 'python' reads only python's blocks
```

- Rule of thumb: cluster tables **> ~10 GB**; great for high-cardinality filter columns.
- ⚠️ With clustering, `maximumBytesBilled` uses an **upper-bound estimate** — a query
  can be *rejected* even if the real scan would've been under the cap. Know this before
  you set a tight cap on a clustered table.

**Always, regardless of table:**

- **`maximumBytesBilled`** — a hard cap; the query is **rejected before billing** if it
  would exceed it.
- **Dry run first** — `bq query --dry_run` estimates bytes with no charge.
- **Never `SELECT *`**; name only the columns you need; filter on the partition column.

---

## 4. Table types and idempotent loads

**Table types:**

| Type | What it is | Cost note |
|---|---|---|
| **Native** | data stored in Colossus (Capacitor) | fastest; normal storage cost |
| **External** | data stays in GCS/Sheets/etc., queried in place | no storage cost; slower, no clustering |
| **View** | a saved query, re-run every time | scans the underlying tables each call |
| **Materialized view** | precomputed, auto-refreshed result | storage cost, but cheap+fast reads |

**Idempotency — the rule that makes Airflow reruns safe.** A DAG can re-run the same
day's task (retry, backfill). If the SQL just `INSERT`s, a rerun **double-loads**.
Make loads **idempotent** so rerunning is harmless:

- **`WRITE_TRUNCATE` a partition** — overwrite exactly the day you're loading:
  ```sql
  -- overwrite only 2022-08-01's partition; rerun = same result
  ```
- **`MERGE`** — upsert: update matching rows, insert new ones, in one atomic statement.
  The warehouse equivalent of "insert or update."

Never write a load that only appends with `datetime.now()` — reruns duplicate and the
result depends on *when* it ran. Same idea as retries in Session 5: a task must be safe
to run twice.

---

## 5. BigQuery from Airflow (the operators) + runnable reference

**Golden rule: orchestrate, don't compute.** The heavy SQL runs *in BigQuery*; Airflow
just launches jobs and reads back small signals. `BigQueryInsertJobOperator`'s XCom is
the **job id**, not the rows.

The operators you'll actually reach for (all from
`airflow.providers.google.cloud.operators.bigquery`):

| Operator | Use |
|---|---|
| `BigQueryInsertJobOperator` | run any query/DDL/DML job — the workhorse. Pass `job_id` for **idempotency** (reattaches to an existing job instead of double-running); supports **deferrable** mode |
| `BigQueryCheckOperator` | run a 1-row SQL; **fail** the task if any value is falsy — a data-quality gate |
| `BigQueryValueCheckOperator` | assert a query's value equals a `pass_value` |
| `BigQueryIntervalCheckOperator` | compare a metric today vs N days ago (drift) |
| `BigQueryColumnCheckOperator` / `BigQueryTableCheckOperator` | declarative column/table quality tests |
| `BigQueryGetDataOperator` | pull rows from a table into XCom |
| `BigQueryCreateEmptyDatasetOperator` / `BigQueryCreateTableOperator` / `...DeleteTableOperator` | manage datasets/tables |

> `BigQueryExecuteQueryOperator` is **deprecated** (legacy-SQL era) — use
> `BigQueryInsertJobOperator`. Legacy SQL itself is being **restricted after
> 2026-06-01**; always set `"useLegacySql": False`.

For a small scalar back in Python, use `BigQueryHook.get_first` in a `@task` (Session 15).

**Runnable reference DAG** — launch a capped job, gate on a check, read a scalar. All
read-only on the Stack Overflow spine, cost-capped.

```python
# dags/s6/s6_task1.py
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
CAP = "2000000000"          # 2 GB max bytes billed — reject anything bigger


@dag(
    dag_id="s6_task1",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-6", "bigquery"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():

    # 1) run a capped aggregate job (XCom = the job id, not the number)
    run_count = BigQueryInsertJobOperator(
        task_id="run_count",
        gcp_conn_id=CONN,
        location="US",
        configuration={"query": {
            "query": f"SELECT COUNT(*) AS n FROM `{QUESTIONS}` WHERE answer_count = 0",
            "useLegacySql": False,
            "maximumBytesBilled": CAP,
        }},
    )

    # 2) quality gate: fail the run if the table has zero unanswered rows
    check_has_rows = BigQueryCheckOperator(
        task_id="check_has_rows",
        gcp_conn_id=CONN,
        use_legacy_sql=False,
        sql=f"SELECT COUNT(*) FROM `{QUESTIONS}` WHERE answer_count = 0",
    )

    # 3) pull the small scalar back into Python and log it
    @task
    def log_scalar() -> None:
        hook = BigQueryHook(gcp_conn_id=CONN, use_legacy_sql=False, location="US")
        n = hook.get_first(f"SELECT COUNT(*) FROM `{QUESTIONS}` WHERE answer_count = 0")[0]
        print(f"unanswered questions = {int(n):,}")

    run_count >> check_has_rows >> log_scalar()


pipeline()
```

```bash
python dags/s6/s6_task1.py
airflow dags test s6_task1 2026-01-01
```

Check **BigQuery Job history**: each query scans only `answer_count` (a few hundred
MB), well under the 2 GB cap.

---

## 6. Your build (no solution)

**File:** `dags/s6/s6_task2.py` (scaffold ready) · **dag_id:** `s6_task2`

Build a small, cost-capped BigQuery DAG on the Stack Overflow data.

**The job:**

- A `BigQueryInsertJobOperator` runs a **capped** aggregate (your choice — e.g. count
  of questions with `view_count > 1000`).
- A `BigQueryCheckOperator` **fails** the run if a table/metric is empty.
- A `@task` uses `BigQueryHook.get_first` to log one scalar metric.

**Rules of engagement:**

- Every operator query carries `maximumBytesBilled`; **no `SELECT *`**;
  `"useLegacySql": False`.
- Orchestrate, don't compute — pull back only a small scalar.
- Passes the integrity gates: `tags`, real `owner`, `retries >= 1`.

**Done when:**

- `python dags/s6/s6_task2.py` parses (prints nothing).
- `airflow dags test s6_task2 2026-01-01` runs green.
- **Job history** shows every query within the cap.
- `python -m pytest tests/ -v` stays green.

---

## 7. Production tip — the two bugs that cost real money

- **An uncapped `SELECT *` on a partitioned table is a budget incident, not a style
  nit.** One unfiltered scan of a big table can bill terabytes. Cap every query, filter
  on the partition column, and name your columns — that trio, not luck, is what keeps a
  warehouse cheap.
- **A non-idempotent load turns a retry into corrupted data.** If a task appends rows
  and then Airflow retries it (Session 5), you've now double-counted. Use
  `WRITE_TRUNCATE` on the target partition or `MERGE` so a rerun lands the *same* data,
  every time.

---

## 8. Verify + commit

```bash
python dags/s6/s6_task2.py
airflow dags test s6_task2 2026-01-01
python -m pytest tests/ -v
git add -A && git commit -m "session 06: bigquery build" && git push
```

Done when the DAG runs green and Job history shows small, capped scans. Then tick the
bytes in `README.md`.

Sources:
[BigQuery overview & architecture](https://docs.cloud.google.com/bigquery/docs/storage_overview),
[BigQuery under the hood — Dremel/Colossus/Capacitor](https://cloud.google.com/blog/products/bigquery/bigquery-under-the-hood),
[Cost best practices](https://docs.cloud.google.com/bigquery/docs/best-practices-costs),
[Partitioned tables](https://docs.cloud.google.com/bigquery/docs/partitioned-tables),
[Clustered tables](https://docs.cloud.google.com/bigquery/docs/clustered-tables),
[Airflow BigQuery operators](https://airflow.apache.org/docs/apache-airflow-providers-google/stable/operators/cloud/bigquery.html)
