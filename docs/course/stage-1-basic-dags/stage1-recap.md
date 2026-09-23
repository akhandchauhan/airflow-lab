# Stage 1 Recap — Build Basic DAGs (all the code, one file)

Everything from Stage 1 in one place: **TaskFlow, operators & dependencies,
TaskGroups, retries, params** — plus the boilerplate every DAG needs. Skim it as a
cheat-sheet; each block is copy-runnable. (Sessions 01, 02, 03, 05, 07.)

---

## 0. The skeleton every DAG needs (integrity gates)

Every DAG carries `tags`, a real `owner`, and `retries >= 1` — or CI fails it.

```python
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="my_dag",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,                 # None = manual only (add a cron in Stage 2)
    catchup=False,
    tags=["stage-1"],              # ← required
    default_args={"owner": "akhand", "retries": 1},   # ← required
)
def pipeline():
    ...

pipeline()                         # ← must call it at module level
```

---

## 1. TaskFlow — `@task`, return → XCom, wiring by data flow

A `@task` function is a task. Its **return value** is pushed to XCom; passing it into
another task **draws the edge automatically** — no `>>` needed.

```python
@dag(dag_id="taskflow", schedule=None, start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
     catchup=False, tags=["stage-1"], default_args={"owner": "akhand", "retries": 1})
def pipeline():

    @task
    def extract() -> list[int]:
        return [1, 2, 3, 4]

    @task
    def transform(nums: list[int]) -> int:
        return sum(nums)                     # receives extract()'s return via XCom

    @task
    def load(total: int) -> None:
        print(f"loaded total={total}")

    load(transform(extract()))               # edges: extract → transform → load

pipeline()
```

**`multiple_outputs`** — return a dict and split it into separate XCom keys:

```python
    @task(multiple_outputs=True)
    def split() -> dict[str, int]:
        return {"lo": 1, "hi": 9}            # → two XComs: split()["lo"], split()["hi"]

    out = split()
    load(out["hi"])                          # pull one key
```

- XCom is for **small** values (metadata DB), never big data.
- No `xcom_push`/`xcom_pull` needed — the return + argument passing does it.

---

## 2. Operators & dependencies — `>>`, `chain`, `cross_downstream`

Classic operators are instantiated and wired with `>>`. Use them when there's no
TaskFlow form (e.g. `EmptyOperator`, provider operators).

```python
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import chain, cross_downstream

@dag(dag_id="deps", schedule=None, start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
     catchup=False, tags=["stage-1"], default_args={"owner": "akhand", "retries": 1})
def pipeline():
    a = EmptyOperator(task_id="a")
    b = EmptyOperator(task_id="b")
    c = EmptyOperator(task_id="c")
    d = EmptyOperator(task_id="d")

    a >> b >> c                     # a then b then c
    a >> [b, c]                     # fan-out: b and c after a

    chain(a, b, c, d)               # a → b → c → d (linear, readable)
    cross_downstream([a, b], [c, d])  # every left → every right (a,b each → c,d)

pipeline()
```

**Mixing TaskFlow + `>>`:** `>>` also works on `@task` results, for **ordering with no
data**:

```python
    first() >> second()            # run order only, nothing passed
```

Rule: **data to pass?** wire by passing it. **just ordering?** use `>>`.

---

## 3. TaskGroups — organize a big DAG

`@task_group` bundles tasks under one collapsible node. Purely visual/namespacing —
zero execution cost. A task inside group `g` gets id `g.task_id`.

```python
from airflow.sdk import task_group

@dag(dag_id="groups", schedule=None, start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
     catchup=False, tags=["stage-1"], default_args={"owner": "akhand", "retries": 1})
def pipeline():

    @task_group(group_id="ingest")          # function name != group_id (kept distinct)
    def load_source(source: str) -> None:
        @task
        def download(name: str) -> str:     # → ingest.download
            return f"/raw/{name}"

        @task
        def stage(path: str) -> None:       # → ingest.stage
            print(f"staging {path}")

        stage(download(source))             # TaskFlow wiring inside the group

    # reuse ONE group definition for many inputs — .override gives each a unique id
    for name in ["orders", "users"]:
        load_source.override(group_id=f"ingest_{name}")(name)

pipeline()
```

- **`.override(group_id=...)`** = "same code, new name" — needed only when you create
  the same group more than once **at the same level**.
- Nested groups auto-prefix (`ingest_orders.checks.x`) — no override needed inside.

---

## 4. Retries — survive a flaky step

`retries` re-runs a failed task; `retry_delay` waits between tries. Set once in
`default_args` to cover every task.

```python
from datetime import timedelta

@dag(dag_id="retries", schedule=None, start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
     catchup=False, tags=["stage-1"],
     default_args={"owner": "akhand", "retries": 3, "retry_delay": timedelta(seconds=10)})
def pipeline():

    @task                                    # inherits retries=3, retry_delay=10s
    def call_flaky_api() -> str:
        import random
        if random.random() < 0.5:
            raise RuntimeError("network blip — will retry")
        return "ok"

    @task(retries=5)                         # override for one task
    def critical() -> None:
        print("extra-guarded step")

    call_flaky_api() >> critical()

pipeline()
```

- `retries=3` = **4 attempts total** (1 + 3).
- Retries only fire on an **exception**. They fix *transient* failures, never bugs.

---

## 5. Params — input at trigger time

`params` declares typed, validated inputs with defaults; read them via
`get_current_context()["params"]`; override with `--conf`.

```python
from airflow.sdk import Param, get_current_context

@dag(dag_id="params", schedule=None, start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
     catchup=False, tags=["stage-1"], default_args={"owner": "akhand", "retries": 1},
     params={
         "country": Param("IN", type="string"),
         "limit": Param(10, type="integer", minimum=1),
         "env": Param("dev", type="string", enum=["dev", "prod"]),
     })
def pipeline():

    @task
    def report() -> None:
        inputs = get_current_context()["params"]      # variable named for its role, not "params"
        print(f"report for {inputs['country']}, top {inputs['limit']}, env={inputs['env']}")

    report()

pipeline()
```

```bash
airflow dags test params 2026-01-01 --conf '{"country": "US", "limit": 5, "env": "prod"}'
airflow dags test params 2026-01-01 --conf '{"env": "staging"}'   # REJECTED (not in enum)
```

- `type` = what kind; `enum` / `minimum` / `maximum` = which values are allowed.
- A bad value is **rejected before any task runs** — that's the payoff of typed params.
- Never put a secret in a param (it's visible in the UI/conf).

---

## 6. Jinja templating — runtime values in string fields

Operator **string** fields are templated: Airflow renders `{{ ... }}` **at run time**,
so a classic operator can use the run's date/params/vars without any Python.

```python
from airflow.providers.standard.operators.bash import BashOperator

@dag(dag_id="templating", schedule="@daily", start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
     catchup=False, tags=["stage-1"], default_args={"owner": "akhand", "retries": 1},
     params={"country": Param("IN", type="string")})
def pipeline():
    BashOperator(
        task_id="echo_window",
        bash_command="echo run={{ ds }} from={{ data_interval_start }} country={{ params.country }}",
    )

pipeline()
```

Common template variables:

| Template | Value |
|---|---|
| `{{ ds }}` | logical date as `YYYY-MM-DD` |
| `{{ data_interval_start }}` / `{{ data_interval_end }}` | the run's window (Session 06) |
| `{{ logical_date }}` | the run timestamp (a pendulum datetime) |
| `{{ params.x }}` | a param value |
| `{{ ti }}` | the task instance — e.g. `{{ ti.xcom_pull(task_ids='t') }}` |
| `{{ var.value.my_key }}` | an Airflow **Variable** |
| `{{ conn.my_conn.host }}` | a field of a **Connection** |
| `{{ macros.ds_add(ds, 7) }}` | built-in date math |

- Only fields in an operator's **`template_fields`** are rendered (e.g. `bash_command`,
  `sql`). Non-templated fields stay literal.
- In a **TaskFlow `@task`**, use `get_current_context()` for the same values in Python;
  Jinja is for **operator string args**.
- Airflow 3 removed `execution_date` — use `ds` / `logical_date` / `data_interval_*`.
- See the rendered result in the UI's **Rendered Templates** tab.

---

## 7. Putting it together — one DAG using several

TaskFlow + a group + params + retries in one pipeline:

```python
from datetime import timedelta
from airflow.sdk import dag, task, task_group, Param, get_current_context

@dag(
    dag_id="stage1_combo",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["stage-1", "recap"],
    params={"batch": Param(100, type="integer", minimum=1)},
    default_args={"owner": "akhand", "retries": 2, "retry_delay": timedelta(seconds=5)},
)
def pipeline():

    @task
    def read_batch_size() -> int:
        return get_current_context()["params"]["batch"]

    @task_group(group_id="process")
    def process(size: int) -> None:
        @task
        def chunk(n: int) -> int:
            return n * 2

        @task
        def report(doubled: int) -> None:
            print(f"processed batch, doubled size = {doubled}")

        report(chunk(size))

    process(read_batch_size())

pipeline()
```

---

## 8. Prove any of them works (the Stage 0 loop)

```bash
python dags/<path>.py                     # parse (prints nothing = OK)
airflow dags test <dag_id> 2026-01-01     # run it, no scheduler/DB needed
python -m pytest tests/ -v                # integrity gates: tags, owner, retries, imports
ruff check dags/ include/ tests/ --select E,F,AIR3   # lint (E501/E231/E251 etc.)
```

That's Stage 1: build the pipeline, wire it, make it flexible (params), make it
resilient (retries), keep it readable (groups) — and prove it every time.

---

Next: **Stage 2 — put it on the clock** ([09 · Schedules & intervals](../stage-2-scheduling/06-schedules.md)),
then Stage 3 assets. See [CURRICULUM.md](CURRICULUM.md) for the full path.
