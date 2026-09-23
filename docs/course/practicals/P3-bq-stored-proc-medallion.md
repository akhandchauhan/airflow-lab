# Practical P3 · Medallion with BigQuery stored procedures

**Spec only — you design and write everything.** No code here, on purpose.

**Goal:** move the pipeline's SQL *into BigQuery* as **stored procedures** (raw → bronze → gold), and let Airflow orchestrate — one procedure call per layer, `@daily`, with a **data-quality gate** that stops the run before bad data reaches gold. Airflow owns the timing and the order; BigQuery owns and runs the logic.

---

## The scenario

StackPulse's daily health report started as a short query in a task. It's now hundreds of lines and unmaintainable inline. The decision: **the SQL lives in BigQuery** as stored procedures you own, and **Airflow just calls them** on a schedule. Build that end to end on `bigquery-public-data.stackoverflow`.

---

## Dataset reference (source)

You read from **`bigquery-public-data.stackoverflow.posts_questions`**. Grain: **one row per question**. Columns you'll need:

| Column | Type | Notes |
|---|---|---|
| `id` | INT64 | unique question id — your dedupe key |
| `title` | STRING | question title (can be null) |
| `tags` | STRING | **pipe-delimited** (`python\|airflow\|gcp`) — split into an array in bronze |
| `creation_date` | TIMESTAMP | when asked — derive the report date from this |
| `answer_count` | INT64 | number of answers (can be null → treat as 0) |
| `view_count` | INT64 | views |

Large table — filter by `creation_date`, select only these columns, never scan the whole thing.

---

## What you build

### Part A — three stored procedures (in BigQuery), one per layer

Each must be **idempotent** — re-running produces the same result, never duplicates.

1. **Raw** — land the source into your own `raw.questions` table: only the columns above, filtered to a recent window. No transformation yet — just a clean copy you control.

2. **Bronze** — from `raw.questions`, produce `bronze.questions_clean`:
   - dedupe by `id` (one row per question),
   - derive a `created_date` (DATE) from `creation_date`, lowercase + trim `title`, treat a null `answer_count` as 0,
   - split the pipe-delimited `tags` into an array,
   - drop rows with a null `title`.

3. **Gold** — from `bronze.questions_clean`, produce `gold.daily_health`. Grain: **one row per `created_date`**, with: `questions` (count), `unanswered` (count where `answer_count = 0`), and `unanswered_rate` (unanswered ÷ questions).

### Part B — the Airflow DAG

- `@daily`, `catchup=False`, real `owner`, `retries >= 1`, `tags`; connection `google_cloud_default`.
- The DAG holds **no business SQL** — each task only *calls* the matching procedure in BigQuery.
- Order: **raw → bronze → data-quality gate → gold.**
- **The gate** (between bronze and gold): fails if `bronze.questions_clean` is empty (or a null rate you deem unacceptable). On failure, **gold must not run**.
- Every job is **cost-capped**; no full-table scans.

---

## Constraints

- **SQL lives in BigQuery** (the stored procedures), not in the DAG. Tasks only call them.
- **Idempotent:** running the DAG twice for the same day leaves every layer's row counts unchanged.
- **Cost-safe:** a byte cap on every query, no `SELECT *` on the public table, aggregates where possible.
- Passes the repo's integrity gates (owner / retries ≥ 1 / tags) and `pytest`.

---

## Acceptance criteria

- Three stored procedures exist in BigQuery, each idempotent.
- The DAG runs **raw → bronze → gate → gold green** for a test date.
- Re-running for the same date yields **identical row counts** in every layer (idempotency proven).
- Forcing `bronze.questions_clean` empty makes the **gate fail and gold skip** (circuit breaker proven).
- `gold.daily_health` has one row per day with `questions`, `unanswered`, `unanswered_rate`.
- `pytest` stays green.

---

## Pointers (names only — look these up, don't copy code)

- BigQuery **stored procedure** — created once, then invoked by name from a query (Airflow issues that invocation).
- The Airflow operator that **submits a BigQuery job** (it returns a job id, not rows).
- The Airflow **check operator** that fails when its query's first value is falsy — your one-line gate.
- The SQL function that **splits** a delimited string into an array.

Done when the call-chain runs green, the gate blocks gold on empty bronze, and a re-run is idempotent. Tick P3 in `docs/course/README.md`.
