# Airflow Learning Notes

Personal notes for Apache Airflow 3.3. Concepts explained for a data engineer
(strong SQL/Python background).

- [1. The `**context` / `print_context` pattern](#1-the-context--print_context-pattern)
- [2. XCom — cross-task communication](#2-xcom--cross-task-communication)

---

## 1. The `**context` / `print_context` pattern

### The code

```python
def print_context(**kwargs):
    print(kwargs)
    print("Job Completed")

task2 = PythonOperator(
    task_id="task2",
    python_callable=print_context,   # a REFERENCE, no ()
    dag=dag,
)
```

### The logic

You never call `print_context()` yourself. You hand the *reference* to the
operator. When the task runs, **Airflow calls the function for you** and injects
the task context as keyword arguments:

```python
# conceptually, inside the worker:
print_context(
    ti=<TaskInstance>,
    dag=<DAG>,
    run_id="manual__2026-01-01...",
    logical_date=DateTime(2026, 1, 1),
    data_interval_start=...,
    data_interval_end=...,
    params={...},
    # ...~30 more keys
)
```

`**kwargs` is a **catch-all**: it scoops every injected keyword argument into one
dict named `kwargs`. `print(kwargs)` then dumps the entire context.

### Step by step

1. Airflow builds a `context` dict for the task instance (all run metadata).
2. `PythonOperator` calls `print_context(**context)` — `**` **unpacks** the dict
   into keyword arguments.
3. Your `**kwargs` **re-packs** those keyword arguments back into a dict.
4. The full context is now available inside the function.

`**` does opposite jobs on each side: **unpack** at the call site, **collect** in
the signature.

### What this approach is called

**Dependency injection via context** — the framework supplies the arguments
instead of you fetching them (same idea as a pytest fixture or a FastAPI
dependency). The enabling Python feature is `**kwargs` (variadic keyword
arguments).

### The better way to write it

Dumping the whole dict is a debugging move. In real DAGs, pull out only what you
use:

```python
def print_context(**context):
    ti = context["ti"]
    print(ti.task_id, context["logical_date"])
```

Cleaner in Airflow 3 — name the exact keys and Airflow injects only those, no
`**kwargs` needed:

```python
def print_context(ti, logical_date, data_interval_start):
    print(ti.task_id, logical_date, data_interval_start)
```

`ti` (TaskInstance) is the one you reach for most — it is the gateway to XCom.

### Common context keys

| Key | What it is |
|---|---|
| `ti` / `task_instance` | TaskInstance — gateway to `xcom_push` / `xcom_pull` |
| `dag`, `task`, `dag_run`, `run_id` | The objects around this execution |
| `logical_date` | Datetime identifying the run (== `run_after` in Airflow 3) |
| `data_interval_start` / `data_interval_end` | The window this run covers |
| `ds`, `ds_nodash`, `ts`, `ts_nodash` | Formatted strings derived from `logical_date` |
| `params` | Runtime parameters |
| `var.value` / `var.json`, `conn` | Airflow Variables and Connections |

> **Incremental-load trap:** `ds` derives from `logical_date`, which in Airflow 3
> equals `run_after` — **not** `data_interval_start` as in Airflow 2. For
> incremental loads use `data_interval_start` / `data_interval_end` explicitly.
> Never use `datetime.now()` — it breaks idempotency on retries.

---

## 2. XCom — cross-task communication

### What it is

**XCom = Cross-Communication.** Airflow's built-in key-value store for passing
**small metadata** between tasks in the same DAG run. Tasks are isolated — they
may run in different processes, containers, or machines — so they cannot share
Python variables. XCom is the channel.

```
task A ──push──▶ [ XCom table in metadata DB ] ──pull──▶ task B
```

### Why it exists

Tasks do not share memory. This does NOT work:

```python
def extract():
    path = "/raw/2026-01-01/"   # local variable

def load():
    read(path)   # NameError — different process, path doesn't exist here
```

`path` dies when `extract`'s process ends. XCom persists it in the database so
`load` can read it back.

### What gets stored

Each XCom row is uniquely identified by:

| Field | Meaning |
|---|---|
| `dag_id` | which DAG |
| `run_id` | which DAG **run** (scopes it to one execution) |
| `task_id` | which task pushed it |
| `map_index` | for dynamically-mapped tasks (`-1` if not mapped) |
| `key` | label; default is `return_value` |
| `value` | the serialized payload |

The composite key is why XCom is scoped to **one DAG + one run** — task B in
today's run cannot read task A's value from yesterday's run.

### Push and pull — classic way

```python
def push(**context):
    context["ti"].xcom_push(key="folder_path", value="/raw/2026-01-01/")

def pull(**context):
    p = context["ti"].xcom_pull(task_ids="push", key="folder_path")
    print(p)   # /raw/2026-01-01/
```

`xcom_push` writes a row; `xcom_pull` reads it back by `task_ids` + `key`.

### Push and pull — TaskFlow way (preferred)

With `@task`, the **return value is auto-pushed** (key `return_value`) and
passing it as an argument **auto-pulls** it:

```python
@task
def extract() -> str:
    return "/raw/2026-01-01/"      # auto-pushed to XCom

@task
def load(path: str) -> None:       # auto-pulled from XCom
    print(path)

load(extract())                    # wiring + XCom, both handled for you
```

No `ti`, no `xcom_push`, no `xcom_pull` — same mechanism, none of the
boilerplate.

### The one rule interviews always ask

**XCom is for metadata, NOT data.**

| ✅ Push this | ❌ Never push this |
|---|---|
| File paths (`/raw/2026-01-01/`) | DataFrames |
| Row counts (`4213`) | CSV / file contents |
| IDs, flags, timestamps | API response bodies |
| GCS / S3 URIs | Millions of records |

**Why:** the default backend is the **metadata database**. Every value is a row.
Push a 500 MB DataFrame and you bloat the DB, slow every scheduler query, and
eventually crash it.

**The pattern:** write the data to object storage, push only its location.

```python
@task
def extract() -> str:
    df = fetch_from_api()
    path = "gs://lake/raw/2026-01-01/orders.parquet"
    df.to_parquet(path)            # data → object store
    return path                    # location → XCom

@task
def transform(path: str):
    df = pd.read_parquet(path)     # read data back from the location
```

### Size limits

Value size is capped by the DB column: roughly **48 KB (Postgres)**,
**64 KB (MySQL)**, **1 GB (SQLite — never rely on this)**. For genuinely large
payloads, configure a **custom XCom backend** (S3/GCS) so the value lives in
object storage and only a reference sits in the DB. Standard advice remains
"pass the path".

### Where it lives

- Table: `xcom` in the metadata database
- Auto-serialized on push (JSON by default), auto-deserialized on pull
- Cleared when you clear the task instance or DAG run

### Interview one-liner

> "XCom passes small metadata like file paths or IDs between tasks in the same
> DAG run — not actual datasets. Store data in S3/GCS, pass only the location."
