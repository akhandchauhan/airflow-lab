# Session 06 · Give the DAG a Dial

**Params** — let someone pass a value in when they run the DAG, with no code change.

> ## 📟 Cold open
> The target country was hardcoded in your DAG: `country = "IN"`. Every time
> marketing wanted a different one, you edited the file, committed, and waited for a
> redeploy — just to change two letters. There's a dial for exactly this.
>
> **Today:** turn a hardcoded value into a **param** anyone can set at trigger time.

No BigQuery. Plain tasks. A param is a **dial on the outside of the machine** — you
change the setting without opening it up.

---

## 1. Declare a param

Add a `params` dict to `@dag`. Each entry is a name and a default (wrap it in
`Param(...)` when you want a type or validation).

```python
from airflow.sdk import dag, Param

@dag(
    dag_id="s6_task1",
    params={"name": Param("world", type="string")},   # ← THE MECHANIC: one dial, default "world"
    # ... start_date, schedule, etc.
)
def pipeline():
    ...
```

- Bare default: `params={"name": "world"}`. Typed/validated: `Param("world", type="string")`.
- The default means the DAG **always runs** even if nobody passes a value.

---

## 2. Read it, and pass a value

Inside a task, the params live in the run context.

```python
from airflow.sdk import task, get_current_context

@task
def greet() -> None:
    params = get_current_context()["params"]     # ← THE MECHANIC: read the params dict
    print(f"hello {params['name']}")
```

Run it with the default, then override the dial:

```bash
airflow dags test s6_task1 2026-01-01                          # uses default → "hello world"
airflow dags test s6_task1 2026-01-01 --conf '{"name": "Panda"}'   # → "hello Panda"
```

`--conf` is a JSON string; its values override the defaults for that run.

---

## 3. Complete runnable reference DAG (plain)

Two dials — a string and a validated integer. Build this in your `s6_task1.py`
scaffold and run it.

```python
# dags/s6/s6_task1.py
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task, Param, get_current_context


@dag(
    dag_id="s6_task1",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-6", "params"],
    params={
        "name": Param("world", type="string"),
        "times": Param(1, type="integer", minimum=1),   # must be >= 1
    },
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():

    @task
    def greet() -> None:
        params = get_current_context()["params"]
        for _ in range(params["times"]):
            print(f"hello {params['name']}")

    greet()


pipeline()
```

```bash
python dags/s6/s6_task1.py
airflow dags test s6_task1 2026-01-01
airflow dags test s6_task1 2026-01-01 --conf '{"name": "Panda", "times": 3}'
```

The second run prints the greeting three times to "Panda". Try `--conf '{"times":
0}'` and watch it **reject the run** — `minimum=1` validation fired before any task
ran. That early rejection is the real value of typed params.

---

## 4. Your build (no solution)

**File:** `dags/s6/s6_task3.py` (scaffold ready) · **dag_id:** `s6_task3`

Build a tiny plain DAG driven by params.

**The job:**

- Declare two params: `country` (string, default `"IN"`) and `limit` (integer,
  `minimum=1`, default `10`).
- One `@task` reads both from the context and prints a line like
  `report for IN, top 10`.
- No BigQuery, no connection.

**Done when:**

- `python dags/s6/s6_task3.py` parses (prints nothing).
- `airflow dags test s6_task3 2026-01-01` runs green with the defaults.
- `airflow dags test s6_task3 2026-01-01 --conf '{"country": "US", "limit": 5}'`
  prints `report for US, top 5`.
- `--conf '{"limit": 0}'` is **rejected** (validation).
- `python -m pytest tests/ -v` stays green.

---

## 5. Production tip — validate the dial, and never put secrets on it

- **Type + bounds catch a bad trigger before it wastes a run.** A `Param` with
  `type="integer", minimum=1` rejects `limit=0` at parse of the conf, not three
  tasks deep when a query divides by zero. Cheap guardrail, huge time saver.
- **Params are visible — never pass a secret through one.** Param values show up in
  the UI trigger form and the run conf. Secrets belong in a Connection or a secrets
  backend (that's Session 15), never in a param.

---

## 6. Verify + commit

```bash
python dags/s6/s6_task3.py
airflow dags test s6_task3 2026-01-01
python -m pytest tests/ -v
git add -A && git commit -m "session 06: params" && git push
```

Done when the DAG runs on its defaults and changes behaviour from `--conf`. Then
tick the bytes in `README.md`.

Sources:
[Params — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/params.html),
[Create and use params — Astronomer](https://www.astronomer.io/docs/learn/airflow-params)
