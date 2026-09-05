# GCP project & BigQuery hooks — reference

Standing reference for the course's GCP wiring and how to talk to BigQuery from a
task. Not a session — a lookup page.

## This project

| Thing | Value |
|---|---|
| Billing project id | `dogwood-abbey-490606-e8` |
| Service account | `akhand-chauhan@dogwood-abbey-490606-e8.iam.gserviceaccount.com` |
| Roles | BigQuery Job User + BigQuery Data Viewer |
| Key location | `~/.gcp/key.json` (Codespace) — **outside the repo, never committed** |
| Airflow connection | `google_cloud_default` (type `google_cloud_platform`) |

Connection is stored in Airflow's metadata DB:

```bash
airflow connections add google_cloud_default \
  --conn-type google_cloud_platform \
  --conn-extra '{"key_path": "/home/vscode/.gcp/key.json", "project": "dogwood-abbey-490606-e8"}'
airflow connections get google_cloud_default   # verify
```

The billing project is charged for **bytes scanned**; public data is only the
*source*. `COUNT(*)` scans **0 bytes** (free). Never `SELECT *` a big table; cap
operator queries with `maximumBytesBilled`.

---

## What a Hook is

A **Hook** is Airflow's reusable, authenticated client wrapper around an external
system (BigQuery, GCS, Postgres, …). It:

- reads the **connection** (`gcp_conn_id`) so you never handle raw credentials,
- exposes high-level methods (`get_first`, `insert_job`, …),
- is what **operators use internally** — `BigQueryInsertJobOperator` calls
  `BigQueryHook` under the hood.

**Operator vs Hook — when to use which:**

| Use an **operator** | Use a **hook** (inside a `@task`) |
|---|---|
| Run a query/DDL as a graph node, wire with `>>` | You need the query's **value back** in Python |
| Side-effecting SQL (`CREATE TABLE AS`, `MERGE`) | Branch/log/decide on a returned scalar |
| XCom = job id, not rows | You want the actual rows in XCom |

Import:

```python
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
```

---

## BigQueryHook — the methods you'll use

```python
hook = BigQueryHook(
    gcp_conn_id="google_cloud_default",   # which connection → auth + project
    location="US",                        # must match the dataset's region
    use_legacy_sql=False,                 # standard GoogleSQL (backtick tables)
)
```

| Method | Returns | Use |
|---|---|---|
| `hook.get_first(sql)` | first row (tuple-like), or `None` | a single scalar (`COUNT`, `MAX`) |
| `hook.get_records(sql)` | list of rows | a small result set |
| `hook.get_pandas_df(sql)` | `pandas.DataFrame` | tabular work in Python (needs pandas) |
| `hook.insert_job(configuration=...)` | job object | run a job like the operator does |

`location` mismatch → "dataset not found". Hook reads (`get_first`, etc.) don't
take `maximumBytesBilled` easily — keep them to `COUNT`/aggregates (≈0 bytes); for
capped scans use `BigQueryInsertJobOperator`.

---

## Worked example — pull a count into XCom

```python
@task
def total_trips() -> int:
    hook = BigQueryHook(gcp_conn_id="google_cloud_default",
                        location="US", use_legacy_sql=False)
    row = hook.get_first(f"SELECT COUNT(*) AS trips FROM `{SRC}`")  # COUNT = 0 bytes
    return int(row[0])
```

Line by line:

- **`@task ... -> int`** — a TaskFlow task; the returned int is auto-pushed to XCom
  under key `return_value`.
- **`BigQueryHook(...)`** — authenticated BigQuery client using the
  `google_cloud_default` connection, jobs run in `US`, standard SQL.
- **`hook.get_first(sql)`** — runs the query, returns only the **first row** as a
  tuple, e.g. `(2318447,)`. For `COUNT(*)` there is exactly one row.
- **`return int(row[0])`** — `row[0]` is the single column (the count); cast to int
  and return → lands in XCom for a downstream task to consume.

In one line: *open a BigQuery client via the connection, run a one-row `COUNT(*)`,
take the value out of the row, return it so downstream tasks read it from XCom.*

---

## Security (non-negotiable)

- The service-account **JSON key never enters git** (`.gitignore` blocks it; keep
  it in `~/.gcp/`). A leaked key = someone billing your project.
- If a key is ever exposed, **rotate it**: GCP → IAM → Service Accounts → delete
  the key → create a fresh one → update the connection.
- Never create a **Cloud Composer** environment — the Codespace is the runtime.

See also: [`P1-bigquery-hello.md`](P1-bigquery-hello.md) (full GCP setup), repo
`CLAUDE.md` (authoring rules).
