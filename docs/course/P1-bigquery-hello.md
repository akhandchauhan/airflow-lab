# Practical P1 · BigQuery hello

**Applies:** Session 01 (TaskFlow `@task`), 02 (operators + `>>`), 03 (TaskGroups)
against a **real** public dataset.

**Dataset:** `bigquery-public-data.austin_bikeshare.bikeshare_trips` - every
Austin bike-share trip (~2M rows: start/end station, duration, timestamps). It
lives in the BigQuery multi-region **US**, readable by anyone.

**What you'll do:** run real queries against it from Airflow using the Google
provider operators, organized with TaskGroups, cost-capped so it stays free.

---

## 1. GCP setup (one-time)

You have GCP creds. Wire Airflow -> BigQuery once.

### 1a. Project + service account

1. In the GCP console pick (or create) a **project** - note its **project id**. This is the project that gets **billed** for bytes scanned (public data is the *source*; the *job* runs in your project).
2. **IAM & Admin -> Service Accounts -> Create**. Grant it two roles:
   - **BigQuery Job User** (`roles/bigquery.jobUser`) - lets it run query jobs.
   - **BigQuery Data Viewer** (`roles/bigquery.dataViewer`) - lets it read tables (public data is already viewable, but you'll need this for your own tables later).
3. On that service account -> **Keys -> Add key -> JSON**. Download the key file.

### 1b. Install the provider

Add to `requirements.txt`, then install in the Codespace:

```bash
echo "apache-airflow-providers-google" >> requirements.txt
pip install apache-airflow-providers-google
```

### 1c. Give Airflow the credentials (ADC - simplest)

Put the key **outside the repo** (never commit it) and point Airflow at it via
Application Default Credentials:

```bash
mkdir -p ~/.gcp
mv /path/to/downloaded-key.json ~/.gcp/key.json          # outside the repo
export GOOGLE_APPLICATION_CREDENTIALS=~/.gcp/key.json
export GOOGLE_CLOUD_PROJECT=your-project-id               # your billing project
```

With `GOOGLE_APPLICATION_CREDENTIALS` set, the default `google_cloud_default`
connection uses that key automatically - no connection object to create. Restart
Airflow (`Ctrl+C`, then `airflow standalone`) so it picks up the env vars.

> To make these persist across Codespace restarts, add the two `export` lines to
> `~/.bashrc`, or store the key as a **Codespaces secret** and write it to
> `~/.gcp/key.json` on start.

### 1d. Security - never commit the key

```bash
echo "*.json" >> .gitignore          # or specifically the key path
git status                            # confirm key.json is NOT listed
```

A leaked service-account key = someone billing queries to your project. Keep it
out of git and out of `dags/`.

---

## 2. Cost safety (read before running anything)

Queries bill **your** project for **bytes scanned** (free tier: 1 TiB/month).
Rules:

- **`SELECT COUNT(*)`** scans **0 bytes** in BigQuery - free. Great for a hello.
- Selecting/grouping a column scans only **that column**, not the whole table.
- **Never `SELECT *`** on a big table - it scans every column.
- Put a hard cap on every query with **`maximumBytesBilled`** (a string, in
  bytes). If a query would exceed it, BigQuery **rejects** it instead of billing
  you. Use ~100 MB (`"100000000"`) here.

---

## 3. The BigQuery provider operators

All from `airflow.providers.google.cloud.operators.bigquery`:

| Operator | Use |
|---|---|
| `BigQueryInsertJobOperator` | run any query/DDL job; the general workhorse |
| `BigQueryValueCheckOperator` | run a single-row query and assert its value (data quality) |
| `BigQueryGetDataOperator` | read rows from a **table** into XCom |

These are **classic operators** (you instantiate them, wire with `>>`), not
TaskFlow. Common args: `gcp_conn_id="google_cloud_default"`, `location="US"`, and
for `InsertJob` a `configuration={"query": {...}}` block.

```python
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
```

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
CAP = "100000000"   # 100 MB max bytes billed per query - safety cap


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

Then open the **BigQuery console -> Job history** - you'll see the job, and
**Bytes billed = 0 B** for the `COUNT(*)`. That confirms the wiring and the cost
cap both work.

---

## 6. Build spec - your challenge (no solution)

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

## 7. Verify + commit

```bash
python dags/p1/p1_bigquery_hello.py
airflow dags test p1_bigquery_hello 2026-01-01
python -m pytest tests/ -v
git add -A && git commit -m "practical: P1 bigquery hello" && git push
```

Done when: the DAG runs green **and** BigQuery Job history shows the queries ran
within the byte cap. Tick **P1** in `README.md`.

**Cost check after running:** BigQuery console -> your project -> **Job history**
-> confirm each job's *Bytes billed* is tiny and under your cap. If anything shows
a large scan, you selected too much - add/lower `maximumBytesBilled`.
