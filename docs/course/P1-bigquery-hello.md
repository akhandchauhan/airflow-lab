# Practical P1 · BigQuery hello

**Applies:** Session 01 (TaskFlow `@task`), 02 (operators + `>>`), 03 (TaskGroups)
against a **real** public dataset.

**Dataset:** `bigquery-public-data.austin_bikeshare.bikeshare_trips` — every
Austin bike-share trip (~2M rows: start/end station, duration, timestamps). It
lives in the BigQuery multi-region **US**, readable by anyone.

**What you'll do:** run real queries against it from Airflow using the Google
provider operators, organized with TaskGroups, cost-capped so it stays free.

---

## 1. GCP setup (one-time)

You have GCP creds. Wire Airflow → BigQuery once.

### 1a. Project + service account

1. In the GCP console pick (or create) a **project** — note its **project id**. This is the project that gets **billed** for bytes scanned (public data is the *source*; the *job* runs in your project).
2. **IAM & Admin → Service Accounts → Create**. Grant it two roles:
   - **BigQuery Job User** (`roles/bigquery.jobUser`) — lets it run query jobs.
   - **BigQuery Data Viewer** (`roles/bigquery.dataViewer`) — lets it read tables (public data is already viewable, but you'll need this for your own tables later).
3. On that service account → **Keys → Add key → JSON**. Download the key file.

### 1b. Install the provider

Add to `requirements.txt`, then install in the Codespace:

```bash
# add the pinned provider to requirements.txt, then:
pip install -r requirements.txt
```

Pin it (unpinned installs rot): `apache-airflow-providers-google==21.2.0`.

### 1c. Give Airflow the credentials (define the connection)

The Google operators look up a **connection** named `google_cloud_default` — it
must exist. Setting `GOOGLE_APPLICATION_CREDENTIALS` alone is **not** enough: in
Airflow 3 the operator calls `get_connection("google_cloud_default")` and raises
`AirflowNotFoundException` if it isn't defined, *before* ADC is ever read.

Two ways to define it. For a single-node standalone Codespace the **metadata-DB**
connection is the most reliable (read by the API server at task time, no restart,
no shell-env surprises):

```bash
# key lives OUTSIDE the repo
mkdir -p ~/.gcp
mv /path/to/downloaded-key.json ~/.gcp/key.json

airflow connections add google_cloud_default \
  --conn-type google_cloud_platform \
  --conn-extra '{"key_path": "/home/vscode/.gcp/key.json", "project": "your-project-id"}'

airflow connections get google_cloud_default   # verify
```

Alternative — an **environment-variable connection** (no DB row). Works only if
the var is present in the **API server's** process (i.e. exported *before* you
launch `standalone`, in that same shell), which is easy to get wrong:

```bash
export AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT='{"conn_type": "google_cloud_platform", "extra": {"key_path": "/home/vscode/.gcp/key.json", "project": "your-project-id"}}'
```

Accepted extra keys: `key_path`, `key_dict`, `project`, `scope`, `num_retries`.
Use an **absolute** `key_path` — `~` is not expanded inside the JSON string.

> In Airflow 3 tasks resolve connections through the **API server**, a separate
> process from your shell — that is why a plain `export` in one terminal often
> doesn't reach the task runner, and the DB connection is the safer default.

### 1d. Security — never commit the key

The repo `.gitignore` already blocks `.gcp/`, `*-key.json`, `*service_account*.json`,
and your key's exact filename. Still, confirm before every commit:

```bash
git status                            # confirm no *.json key is listed
```

A leaked service-account key = someone billing queries to your project. Keep it
out of git and out of `dags/`.

---

## 2. Cost safety (read before running anything)

Queries bill **your** project for **bytes scanned** (free tier: 1 TiB/month).
Rules:

- **`SELECT COUNT(*)`** scans **0 bytes** in BigQuery — free. Great for a hello.
- Selecting/grouping a column scans only **that column**, not the whole table.
- **Never `SELECT *`** on a big table — it scans every column.
- Put a hard cap on every query with **`maximumBytesBilled`** (a string, in
  bytes). If a query would exceed it, BigQuery **rejects** it instead of billing
  you. Use ~100 MB (`"100000000"`) here.

---

## 3. The BigQuery provider operators

All from `airflow.providers.google.cloud.operators.bigquery`:

| Operator | Use |
|---|---|
| `BigQueryInsertJobOperator` | run any query/DDL job; the general workhorse. XCom = the **job id**, not the query rows |
| `BigQueryValueCheckOperator` | run a single-row query and **assert** its value (data quality); passes/fails, doesn't hand you the value |
| `BigQueryGetDataOperator` | read rows from a **table** into XCom |

These are **classic operators** (you instantiate them, wire with `>>`), not
TaskFlow. Common args: `gcp_conn_id="google_cloud_default"`, `location="US"`, and
for `InsertJob` a `configuration={"query": {...}}` block.

```python
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
```

> **Orchestrate, don't compute.** `InsertJob` is for side-effecting SQL
> (`CREATE TABLE AS`, `INSERT`, `MERGE`, transforms) — the result stays *in
> BigQuery*, and downstream tasks read it there. XCom is a tiny control-signal
> store (metadata DB), never a data pipe; only pull a **small scalar** back when
> orchestration must decide on it (a branch, a quality gate). To surface a value
> in XCom, use a `@task` with `BigQueryHook.get_first(sql)` and `return` it.

---

## 4. Mixing TaskFlow + TaskGroups + BQ operators

A real DAG combines all three:

- **classic BQ operators** do the warehouse work (they have no TaskFlow form),
- **TaskGroups** organize them into logical stages,
- **`@task`** functions handle Python glue (logging, branching on a result).

You wire classic operators and `@task` results together with `>>`.

---

## 5. Complete runnable reference DAG

A whole file, cost-capped, runs one free `COUNT(*)` and logs. Copy the shape.

```python
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
```

Run it:

```bash
python dags/p1/p1_bq_hello_demo.py
airflow dags test p1_bq_hello_demo 2026-01-01
```

Then open the **BigQuery console → Job history** — you'll see the job, and
**Bytes billed = 0 B** for the `COUNT(*)`. That confirms the wiring and the cost
cap both work. (The `count_trips` XCom holds the **job id**, not the number — to
read the count, open the job in Job history, run the query in the BQ SQL editor,
or use the `BigQueryHook.get_first` `@task` pattern above.)

---

## 6. Build spec — your challenge (no solution)

**File:** `dags/p1/p1_bigquery_hello.py`  ·  **dag_id:** `p1_bigquery_hello`

Build a DAG that reports a few metrics on the Austin bike-share trips, using the
BigQuery provider operators, organized with TaskGroups, and cost-capped.

**The problem:**

- Compute **three** metrics on `bigquery-public-data.austin_bikeshare.bikeshare_trips`:
  1. total number of trips,
  2. the **top 5 start stations** by trip count,
  3. a **data-quality check** that the table is not empty (fail the run if it is).
- Organize the metric queries under **TaskGroups** so the Graph stays readable
  (your call how to group them).
- Every query must carry a **`maximumBytesBilled`** cap.
- Include at least one **`@task`** (TaskFlow) that runs after the metrics and
  logs a short summary line.

**Constraints:**

- Use the Google provider operators for the BigQuery work (not raw SQL in a shell).
- Every query cost-capped; no `SELECT *` on the table.
- Passes the integrity gates: `tags`, real `owner`, `retries >= 1`.

**Acceptance criteria:**

- `python dags/p1/p1_bigquery_hello.py` parses (prints nothing).
- `airflow dags test p1_bigquery_hello 2026-01-01` runs everything green.
- BigQuery **Job history** shows your queries, each with **Bytes billed within
  the cap** (COUNT is 0 B; the top-5 query scans only the station column).
- The empty-table check would **fail** the run if the count were 0.
- `python -m pytest tests/ -v` stays green.

**Nudge (only if stuck):** `BigQueryValueCheckOperator` is built exactly for the
"fail if a query's value isn't what I expect" case; `BigQueryInsertJobOperator`
runs the counting/top-N queries. TaskGroups from Session 03 organize them.

---

## Production tip — cost governance is the SLA of warehouse orchestration

In a real org, an uncapped query is a bug, not a style nit: one `SELECT *` on a
partitioned table can scan terabytes and bill real money. Four practices that
separate a toy DAG from a production one:

- **Cap every query** with `maximumBytesBilled` (you already do). Make it a
  non-negotiable rule — ideally enforced in review/CI, not left to memory.
- **Label jobs for cost attribution.** The operator already auto-stamps
  `airflow-dag` / `airflow-task` labels (you saw them in the run log). Add your
  own so BigQuery billing is sliceable by team/env:
  ```python
  configuration={"query": {..., "labels": {"team": "data-eng", "env": "dev"}}}
  ```
  Then attribute spend with `INFORMATION_SCHEMA.JOBS` (filter on the labels) —
  answers "what did this pipeline cost last month?".
- **Partition pruning is the real lever.** "Bytes billed" is driven by how much
  data a query touches. Filter on the partition/clustering column so BigQuery
  scans one partition, not the whole table — that, not the cap, is what keeps
  cost low day to day.
- **Least-privilege, per-environment service account.** Job User + Data Viewer
  only, a separate SA per env; never an Owner/Editor key sitting in a DAG.

---

## 7. Verify + commit

```bash
python dags/p1/p1_bigquery_hello.py
airflow dags test p1_bigquery_hello 2026-01-01
python -m pytest tests/ -v
git add -A && git commit -m "practical: P1 bigquery hello" && git push
```

Done when: the DAG runs green **and** BigQuery Job history shows the queries ran
within the byte cap. Tick **P1** in `README.md`.

**Cost check after running:** BigQuery console → your project → **Job history** →
confirm each job's *Bytes billed* is tiny and under your cap. If anything shows
a large scan, you selected too much — add/lower `maximumBytesBilled`.
