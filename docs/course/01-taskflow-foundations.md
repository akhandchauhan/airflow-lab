# Session 01 - W1 Sat - TaskFlow Foundations

**Goal:** author a DAG entirely with `@dag` / `@task`, where XCom is implicit -
return values flow as function arguments, and `multiple_outputs` splits a dict
into separately-addressable XComs.

---

## 1. Concept

TaskFlow is the decorator-based authoring style. You write plain Python
functions; Airflow turns each into a task and wires the data flow for you.

- A `@task` function's **return value is auto-pushed to XCom** (key `return_value`).
- **Passing that return as an argument to another `@task` auto-pulls it** and creates the dependency. You never call `xcom_push` / `xcom_pull`.
- `multiple_outputs=True` on a task that returns a `dict` stores **each key as its own XCom**, so downstream tasks can take just the keys they need.

Reach for TaskFlow whenever the work is Python and you're moving small values
between steps. Drop to classic operators only for non-Python work (Bash, SQL,
containers) or when an operator has no decorator form.

---

## 2. API + example

```python
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="01_taskflow_foundations",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["course", "w1", "taskflow"],
    default_args={"owner": "akhand", "retries": 2},
)
def pipeline():
    @task(multiple_outputs=True)
    def extract() -> dict:
        # each key becomes its own XCom
        return {"path": "gs://lake/raw/2026-01-01/", "rows": 4213}

    @task
    def transform(path: str, rows: int) -> int:
        print(f"transforming {rows} rows from {path}")
        return rows

    @task
    def load(row_count: int) -> None:
        print(f"loaded {row_count} rows")

    data = extract()
    load(transform(path=data["path"], rows=data["rows"]))


pipeline()
```

`data["path"]` and `data["rows"]` work **because** of `multiple_outputs=True` -
without it, `extract` returns one XCom (the whole dict) and you'd subscript it
downstream instead.

---

## 3. Build spec - you write this (no solution)

Create `dags/01_taskflow_foundations.py`.

**Requirements:**
1. A `@dag` named `01_taskflow_foundations`, `schedule=None`, `catchup=False`, tags include `course`, `default_args` with a real `owner` and `retries>=1`.
2. `extract` - `@task(multiple_outputs=True)`, returns a dict with at least `source_path: str` and `record_count: int`.
3. `transform` - takes `source_path` and `record_count` as **named arguments**, prints a message, returns the (possibly adjusted) count as an `int`.
4. `load` - takes the transformed count, prints `"loaded N rows"`, returns `None`.
5. Wire them by **function calls only** - no `>>`, no `xcom_push`, no `xcom_pull`.

**Acceptance criteria:**
- `grep -r "xcom_p" dags/01_taskflow_foundations.py` -> no matches.
- Graph view shows `extract -> transform -> load`.
- Grid view shows separate XComs `source_path` and `record_count` on the `extract` task.

---

## 4. Production tip - top-level code

Everything at **module scope re-runs on every DAG parse** (~every 30s per file).
Keep module scope to imports, the `@dag` definition, and the final `pipeline()`
call. Put heavy imports (pandas, requests, cloud SDKs) **inside** the task body:

```python
@task
def transform(path: str):
    import pandas as pd   # loaded only when the task runs, not on every parse
    ...
```

A top-level `import pandas` or, worse, an API call at module scope steals
scheduler CPU on every parse cycle forever.

---

## 5. Verify + commit

```bash
python dags/01_taskflow_foundations.py                       # parses, no error
airflow tasks test 01_taskflow_foundations transform 2026-01-01
python -m pytest tests/ -v                                   # integrity gates pass
git add dags/01_taskflow_foundations.py && git commit -m "course: 01 taskflow foundations" && git push
```

Done when CI is green. Tick session 01 in `docs/course/README.md`.
