# ETL in GCP — where the code lives and where it actually runs

**Goal:** answer the exact question that confuses everyone starting out — *"I write a DAG in the Airflow folder, but where does the join/clean actually run? On the Airflow machine, or in GCP?"* The one-sentence answer: **Airflow orchestrates, BigQuery computes.** Your DAG file lives in the Airflow `dags/` folder and only *submits* work; the SQL that joins and cleans your data runs inside **BigQuery's serverless engine** on Google's infrastructure, not on the Airflow box. Everything below is that idea, made concrete for a raw → bronze → gold pipeline on a public dataset.

---

## 1. The one mental model (read this first)

Two separate systems. Don't merge them in your head:

```
   AIRFLOW  (the orchestrator — a timer + a to-do list)        GCP  (storage + compute)
   ────────────────────────────────────────────────           ─────────────────────────────
   dags/etl_pipeline.py   ──"run this SQL now"──▶              BigQuery  (RUNS the SQL,
   include/sql/*.sql          (submits a job,                             HOLDS the tables)
                               waits for job id)                 raw → bronze → gold datasets
                          ◀──"job 12345 done"────              GCS      (optional raw file landing)
                                                               Looker Studio / Superset (reads gold)
```

- **Airflow** = a smart alarm clock with a checklist. It decides *when* and *in what order*, calls the service that does the work, and waits for the result. It moves **no data** and does **no joins**.
- **BigQuery** = the warehouse. It stores the tables *and* runs the SQL on Google's servers. When your DAG says "run `bronze_clean.sql`," BigQuery executes it on its own compute — the Airflow machine just watches.

**Running analogy — a restaurant.** Airflow is the **head waiter**: takes the order, tells the kitchen, checks it's done, sends the next course. BigQuery is the **kitchen**: it has the ingredients (tables) and the stoves (compute). The waiter never cooks. If your join is slow, it's the kitchen working hard — the waiter is just standing there holding a ticket.

> This is the golden rule of GCP data pipelines: **orchestrate, don't compute.** The Airflow operator submits a BigQuery job and gets back a *job id*, not the rows. The rows never travel through Airflow.

---

## 2. Where every piece of code lives (the file layout)

| What | Where it lives | What it is | Where it EXECUTES |
|---|---|---|---|
| `dags/etl_pipeline.py` | Airflow `dags/` folder (your git repo) | the DAG — order, schedule, which SQL to run | Airflow scheduler/worker |
| `include/sql/raw_load.sql`, `bronze_clean.sql`, `gold_marts.sql` | your git repo (next to the DAG) | the transformations | **BigQuery** |
| `raw`, `bronze`, `gold` datasets + their tables | your **GCP project** (BigQuery) | the actual data | BigQuery storage |
| the dashboard | Looker Studio / Superset | the "show the data" layer | reads the gold BigQuery table |

The important row is #2: **your SQL files live in the repo, but they run in BigQuery.** The DAG just reads the `.sql` text and hands it to BigQuery. So "is the code in Airflow or GCP?" — the *files* are in the Airflow repo; the *execution* is in GCP.

---

## 3. The raw → bronze → gold flow, mapped to GCP services

Your three layers (this is the "medallion" pattern; note the conventional names in the last column):

| Your layer | What happens | GCP service that does it | Conventional name |
|---|---|---|---|
| **raw** | land the public data exactly as-is, no changes | BigQuery `raw` dataset (or a GCS bucket for files) | Bronze |
| **bronze** | join + clean (types, dedupe, filter, standardize) | BigQuery SQL → `bronze` tables | Silver |
| **gold** | aggregate into what the dashboard shows | BigQuery SQL → `gold` tables | Gold |

Each arrow between layers is **one Airflow task** that submits **one BigQuery SQL job**:

```
[public dataset] --load--> raw.questions   (task 1: BigQueryInsertJobOperator)
raw.questions   --join+clean--> bronze.questions_clean   (task 2)
bronze.*        --aggregate--> gold.daily_health         (task 3)
gold.daily_health --read--> Looker Studio dashboard      (no Airflow task — the BI tool reads it live)
```

The "show the data" step is **not** an Airflow task — a BI tool (Looker Studio, Superset) points at the `gold` table and queries it whenever someone opens the dashboard.

---

## 4. Concrete example (public dataset: Stack Overflow)

Uses `bigquery-public-data.stackoverflow` — a free public dataset already in BigQuery.

**The DAG — `dags/etl_pipeline.py`** (lives in Airflow, submits 3 jobs):

```python
from __future__ import annotations

import pendulum
from airflow.sdk import dag
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

PROJECT = "your-gcp-project"
CAP = 2_000_000_000  # 2 GB maximumBytesBilled — cost guard on every query


def q(sql: str) -> dict:
    return {"query": {"query": sql, "useLegacySql": False, "maximumBytesBilled": CAP}}


@dag(
    dag_id="etl_pipeline",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    tags=["etl", "gcp"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():
    # RAW: land public data into your own raw dataset (idempotent overwrite)
    raw = BigQueryInsertJobOperator(
        task_id="raw_load",
        configuration=q(f"""
            CREATE OR REPLACE TABLE `{PROJECT}.raw.questions` AS
            SELECT id, title, tags, creation_date, answer_count, view_count
            FROM `bigquery-public-data.stackoverflow.posts_questions`
            WHERE creation_date >= '2024-01-01'
        """),
    )

    # BRONZE: join + clean (dedupe, cast, standardize)
    bronze = BigQueryInsertJobOperator(
        task_id="bronze_clean",
        configuration=q(f"""
            CREATE OR REPLACE TABLE `{PROJECT}.bronze.questions_clean` AS
            SELECT DISTINCT
                id,
                LOWER(TRIM(title)) AS title,
                SPLIT(tags, '|') AS tag_list,
                DATE(creation_date) AS created_date,
                COALESCE(answer_count, 0) AS answer_count
            FROM `{PROJECT}.raw.questions`
            WHERE title IS NOT NULL
        """),
    )

    # GOLD: aggregate into what the dashboard shows
    gold = BigQueryInsertJobOperator(
        task_id="gold_marts",
        configuration=q(f"""
            CREATE OR REPLACE TABLE `{PROJECT}.gold.daily_health` AS
            SELECT
                created_date,
                COUNT(*) AS questions,
                COUNTIF(answer_count = 0) AS unanswered,
                ROUND(COUNTIF(answer_count = 0) / COUNT(*), 3) AS unanswered_rate
            FROM `{PROJECT}.bronze.questions_clean`
            GROUP BY created_date
        """),
    )

    raw >> bronze >> gold


pipeline()
```

**Read what's happening:**
- The DAG file is in `dags/` (Airflow). It contains **SQL strings**, but it does not run them — `BigQueryInsertJobOperator` **submits** each to BigQuery and waits for the job id (this is `orchestrate, don't compute`).
- The `raw` → `bronze` → `gold` tables are created **inside BigQuery**, in your GCP project. Nothing but a job id comes back to Airflow.
- `CREATE OR REPLACE TABLE` makes each step **idempotent** — re-running the DAG rebuilds the table cleanly, never doubles the data.
- `maximumBytesBilled` caps the cost of every query so a runaway scan can't surprise you.

You could keep the SQL in separate files (`include/sql/bronze_clean.sql`) and load them instead of inlining — cleaner for real projects, same execution model.

---

## 5. Where does this run in PRODUCTION? (dev vs prod)

This is the part your question is really about. **The transform always runs in BigQuery.** What changes between dev and prod is **where Airflow itself lives.**

| | Airflow runs where? | Your `dags/` folder is… | The SQL runs where? |
|---|---|---|---|
| **Dev / learning (you now)** | in your **Codespace** (local Airflow) | the repo you edit | BigQuery (GCP) |
| **Prod option A — Cloud Composer** | Google-managed Airflow on a **GKE cluster** in your GCP project | synced to a special **GCS bucket** (`gs://<composer-bucket>/dags/`) that Composer reads every ~30s | BigQuery (GCP) |
| **Prod option B — Astronomer / self-hosted** | Airflow on Astronomer's cloud or your own VM/K8s | deployed via `astro deploy` or a Docker image | BigQuery (GCP) |

**So "if written in GCP, where exactly does it run?"**
- **Cloud Composer** *is* Airflow running inside GCP. Your DAG Python files get uploaded to a **GCS bucket**; Composer's cluster reads that bucket, parses the DAGs, and its **workers execute the DAG logic** (the `>>`, the scheduling, the "submit job" call).
- But even in Composer, the **join/clean SQL still executes in BigQuery**, not on the Composer workers. Composer workers are tiny — they just send the job and wait.
- ⚠️ For your setup specifically: **never create a Cloud Composer environment** — it bills ~$300+/month even when idle. Your Codespace Airflow pointing at BigQuery is the right dev runtime. Composer is listed here only so you understand the prod picture.

```
DEV                                          PROD (Cloud Composer)
Codespace                                    GCP project
  └─ airflow (scheduler+worker)                └─ Composer (GKE): scheduler+workers
       └─ dags/etl_pipeline.py                      └─ reads gs://composer-bucket/dags/
              │                                             │
              └── submits SQL ──▶ BigQuery ◀── submits SQL ─┘   ← SAME place either way
```

---

## 6. The full production setup (the checklist)

What a real version of this pipeline needs beyond the DAG:

1. **Source → raw:** for a public BigQuery dataset, "load" is just a `CREATE TABLE AS SELECT` (no file movement). For external files (CSV/JSON from an API), land them in a **GCS bucket** first, then load into BigQuery — GCS is the raw file zone.
2. **Datasets:** create three BigQuery datasets — `raw`, `bronze`, `gold` — ideally with a **table expiration** on `raw` (auto-delete old scratch data).
3. **Credentials:** a **service-account** with BigQuery Data Editor + Job User, wired to Airflow as the `google_cloud_default` **connection**. Keep the key file **out of git** (`~/.gcp/`), and in prod use a **secrets backend** (Secret Manager), never a param.
4. **Scheduling:** `@daily` for a simple clock, or **assets** (Session 09) so `gold` rebuilds the moment `bronze` finishes instead of on a hopeful timer.
5. **Idempotency:** every step is `CREATE OR REPLACE` or a `MERGE` keyed on the date — re-running never duplicates. Never filter on `datetime.now()`; filter on the run's data interval.
6. **Cost control:** `maximumBytesBilled` on every query; no `SELECT *` on big tables; partition `raw`/`bronze` by date so downstream queries prune. (BigQuery free tier: 1 TiB scanned/month.)
7. **Data quality gate:** before publishing `gold`, a `BigQueryCheckOperator` that fails if `bronze` has 0 rows or a null rate too high (Session 16) — stops a bad number reaching the dashboard.
8. **Alerting:** `on_failure_callback` → Slack, so a broken run pages a human (Session 17).
9. **CI/CD:** DAGs in git; on push, run `ruff --select AIR3` + the DAG-integrity test; deploy only if green (Session 22).
10. **Show the data:** point **Looker Studio** (free) or **Superset** at `gold.daily_health`. The BI tool queries the gold table directly — no Airflow involved in serving.

---

## 7. Two ways to write the transforms (same execution model)

| Approach | What you write | Runs where | When to use |
|---|---|---|---|
| **Raw SQL operators** (above) | `.sql` + `BigQueryInsertJobOperator` | BigQuery | small pipelines, full control |
| **dbt (via Cosmos)** | dbt models (SQL + `ref()`) | BigQuery | many models, tests, lineage, docs — the industry default (Session 23) |
| **Dataform** | SQLX in GCP's native tool | BigQuery | you want the transform layer *inside* GCP, no Airflow for T |

All three **compute in BigQuery**. dbt/Dataform just give you dependency graphs, testing, and lineage on top. Since you already know dbt, the natural production shape is: **Airflow orchestrates + triggers → dbt does the bronze/gold transforms → BigQuery runs it all.**

---

## 8. The answer in one table

| Your question | Answer |
|---|---|
| Where is the DAG code written? | In the Airflow `dags/` folder (your git repo) |
| Where is the transform SQL written? | Also in the repo (inline or `include/sql/`) |
| Where does the join/clean actually run? | **In BigQuery**, on Google's servers — never on the Airflow machine |
| What does Airflow do, then? | Schedules, orders the steps, submits jobs, waits, retries, alerts |
| If Airflow runs "in GCP" (Composer), where's that? | On a GKE cluster; DAGs read from a GCS bucket — but SQL still runs in BigQuery |
| Where does the data live? | BigQuery datasets `raw` / `bronze` / `gold` in your GCP project |
| Where is the data shown? | Looker Studio / Superset reading the `gold` table directly |

**The sentence to remember:** *the code lives in Airflow, the work happens in BigQuery.*

Sources:
[BigQuery operators — Airflow Google provider](https://airflow.apache.org/docs/apache-airflow-providers-google/stable/operators/cloud/bigquery.html),
[Cloud Composer overview (managed Airflow on GCP)](https://cloud.google.com/composer/docs/composer-3/composer-overview),
[BigQuery introduction (serverless architecture)](https://cloud.google.com/bigquery/docs/introduction),
[Medallion architecture (raw/bronze→silver→gold)](https://www.databricks.com/glossary/medallion-architecture)
