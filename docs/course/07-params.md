# Session 07 · Params

**Goal:** make a DAG take **input at trigger time** instead of hardcoding values —
understand that a Param is a *declared, validated input with a default*, where the
declaration is read at **parse time** and the actual value is bound at **run time**
from the DAG run's `conf`. Plain DAGs, no BigQuery.

---

## 1. What a Param is (and what it is NOT)

A **Param is a named input to a DAG**: a default value, an optional type, and
optional validation rules. You declare params on the DAG; whoever triggers a run can
override them.

The mechanism is the same parse-time / run-time split as XCom (Session 01):

```
PARSE TIME                          RUN TIME (a triggered run)
----------                          --------------------------
@dag(params={"name": Param(...)})   run conf {"name": "Panda"} merged over defaults
  → the DECLARATION is read           → validated against the Param rules
    when the file is parsed           → exposed in the task context as params["name"]
```

- At **parse time** Airflow reads the `params={...}` dict — the *shape* of the input.
- At **run time** the concrete values come from the run's **`conf`**, merged over the
  defaults, validated, and handed to tasks as `context["params"]`.

What it is **NOT**:

| Not this | Because |
|---|---|
| a **Variable** | Variables are global, stored in the metadata DB, shared across *all* DAGs; a Param is scoped to **one DAG run**, set at trigger |
| an **XCom** | XCom passes data **task → task** during a run; a Param is input **into** the run, from outside |
| an env var / config | Params are per-run and validated; config is static and global |

Reach for a Param when the same DAG should run with different inputs — a date range, a
country, a batch size, a dry-run flag — without editing code.

---

## 2. Declaring params

Add a `params` dict to `@dag`. Each entry maps a name to a **default** — a bare value,
or a `Param(...)` when you want a type and validation.

```python
from airflow.sdk import dag, Param

@dag(
    dag_id="...",
    params={
        "name": "world",                                  # bare default (untyped)
        "times": Param(1, type="integer", minimum=1),     # typed + validated
    },
)
def pipeline():
    ...
```

- `Param(default, ...)` — the **first argument is the default**.
- Validation is **JSON-Schema** under the hood. The ones you'll actually use:

| Rule | Example | Meaning |
|---|---|---|
| `type` | `type="integer"` | `"string"`, `"integer"`, `"number"`, `"boolean"`, `"array"`, `"object"` |
| `minimum` / `maximum` | `minimum=1, maximum=10` | numeric bounds |
| `enum` | `enum=["dev", "prod"]` | value must be one of these |
| `minLength` / `maxLength` | `minLength=2` | string length bounds |

- **Allowing null:** a typed Param rejects `None`. To allow it, use a list type:
  `Param(None, type=["null", "string"])`.
- **A Param with no default** and a type that forbids null becomes **required** — the
  DAG can't complete a run until a value is supplied at trigger.

---

## 3. Reading params in a task

The values live in the run context. Two ways in TaskFlow:

```python
from airflow.sdk import task, get_current_context

@task
def greet() -> None:
    params = get_current_context()["params"]     # the whole params dict
    print(f"hello {params['name']}")
```

```python
@task
def greet(**context) -> None:                    # or grab context via **kwargs
    print(f"hello {context['params']['name']}")
```

And in any **templated** field, params are available as Jinja:

```python
BashOperator(task_id="echo", bash_command="echo {{ params.name }}")
```

`get_current_context()` is the TaskFlow-native way and reads clearly — prefer it.

---

## 4. Passing values at trigger time

Four ways to set params on a run; all override the defaults:

| How | Command / place |
|---|---|
| **CLI test** (what you'll use) | `airflow dags test <dag> <date> --conf '{"name": "Panda"}'` |
| **CLI trigger** (scheduler) | `airflow dags trigger <dag> --conf '{"name": "Panda"}'` |
| **UI** | the **Trigger DAG** form — edit params, then run |
| **From another DAG** | `TriggerDagRunOperator(..., conf={"name": "Panda"})` |

`--conf` is a **JSON string**. Its values override the defaults for that one run, then
validation runs — a value that breaks a rule **rejects the run before any task
starts**.

**Precedence**, lowest to highest:

```
dag-level Param default   <   task-level param   <   run conf (what the trigger passes)
```

(The run conf overriding the default relies on `core.dag_run_conf_overrides_params`,
which is **True** by default.)

---

## 5. A complete runnable DAG (your reference)

A whole file: three param types (string, bounded integer, boolean), read in one task.
Names are kept plain and distinct.

```python
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task, Param, get_current_context


@dag(
    dag_id="s7_task1",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-7", "params"],
    params={
        "name": Param("world", type="string"),
        "times": Param(1, type="integer", minimum=1, maximum=10),   # 1..10
        "shout": Param(False, type="boolean"),
    },
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():

    @task
    def greet() -> None:
        params = get_current_context()["params"]
        message = f"hello {params['name']}"
        if params["shout"]:
            message = message.upper()
        for _ in range(params["times"]):
            print(message)

    greet()


pipeline()
```

Run it three ways:

```bash
python dags/s7/s7_task1.py                                          # parses
airflow dags test s7_task1 2026-01-01                               # defaults → "hello world"
airflow dags test s7_task1 2026-01-01 --conf '{"name": "Panda", "times": 3, "shout": true}'
```

The last run prints `HELLO PANDA` three times. Now break a rule on purpose:

```bash
airflow dags test s7_task1 2026-01-01 --conf '{"times": 99}'       # maximum=10 → REJECTED
```

The run is rejected **before `greet` runs** — validation caught `times > 10`. That
early rejection is the real payoff of typed params: a bad input fails fast, not three
tasks deep.

---

## 6. Build spec — your challenge (no solution)

**File:** `dags/s7/s7_task3.py`  ·  **dag_id:** `s7_task3`

Build a small **report-config** DAG driven entirely by params.

**The problem:**

- Declare **three** params:
  - `country` — string, default `"IN"`.
  - `limit` — integer, `minimum=1`, default `10`.
  - `env` — string restricted to `enum=["dev", "prod"]`, default `"dev"`.
- One `@task` reads all three from the context and prints a line like
  `report for IN, top 10, env=dev`.
- No BigQuery, no connection.

**Constraints:**

- Read params via `get_current_context()["params"]`.
- Passes the integrity gates: `tags`, real `owner`, `retries >= 1`.

**Acceptance criteria:**

- `python dags/s7/s7_task3.py` parses (prints nothing).
- `airflow dags test s7_task3 2026-01-01` prints the line with the **defaults**.
- `airflow dags test s7_task3 2026-01-01 --conf '{"country": "US", "limit": 5, "env": "prod"}'`
  prints `report for US, top 5, env=prod`.
- `--conf '{"limit": 0}'` is **rejected** (below `minimum`), and
  `--conf '{"env": "staging"}'` is **rejected** (not in `enum`).
- `python -m pytest tests/ -v` stays green.

**One nudge (only if stuck):** the `enum` rule is what makes `env` reject anything
outside your allowed list — you don't validate it yourself in Python.

---

## 7. Production tip — validate the dial, and never put a secret on it

- **Type + bounds turn a bad trigger into an instant, clear failure.** `Param(10,
  type="integer", minimum=1)` rejects `limit=0` at trigger with a readable error,
  instead of a division-by-zero five tasks later. Declare the constraint once; every
  run is guarded for free.
- **Params are visible — never pass a secret through one.** Param values show in the
  UI trigger form and the run's `conf`. Secrets belong in a **Connection** or a
  secrets backend (Session 15), never in a param.
- **Keep conf JSON-serializable.** `--conf` is JSON — no Python objects, no datetimes
  except as strings. If you need a date, pass a string and parse it in the task.

---

## 8. Verify + commit

```bash
python dags/s7/s7_task3.py
airflow dags test s7_task3 2026-01-01
python -m pytest tests/ -v
git add -A && git commit -m "session 07: params" && git push
```

Done when the DAG runs on its defaults and changes behaviour from `--conf`, and a
bad value is rejected. Tick the bytes in `docs/course/README.md`.

**Pre-push habit:** `ruff check dags/ include/ tests/ --select E,F,AIR3 && python -m pytest tests/ -v`
before every push — green locally means green CI.

Sources:
[Params — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/params.html),
[Create and use params — Astronomer](https://www.astronomer.io/docs/learn/airflow-params)
