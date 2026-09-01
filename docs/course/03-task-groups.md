# Session 03 - W2 Sat - TaskGroups

**Goal:** organize a DAG's tasks into named, collapsible groups - understand that
a TaskGroup is a *visual/logical* wrapper (not a sub-DAG), how `group_id`
prefixes task ids, how to nest groups, and how to instantiate the same group
many times.

---

## 1. What is a TaskGroup? (and what it is NOT)

A **TaskGroup is a way to bundle tasks under one collapsible node in the UI.**
It is purely **organizational** - it groups tasks visually in the Graph and
namespaces their ids. It does **not** change how tasks are scheduled or executed;
every task inside still runs as an independent task in the same DAG.

Critically, a TaskGroup is **not** a sub-DAG. (Airflow 2 had `SubDagOperator`,
which ran a whole child DAG as one task and caused deadlocks - it was **removed
in Airflow 3**. TaskGroups replaced it precisely because they add zero execution
overhead: they are just a label + a namespace.)

```
Without groups (flat)              With a TaskGroup "ingest"
-------------------------          -------------------------------
download                           ingest            <- collapsible node
validate                             ingest.download
stage                                ingest.validate
                                     ingest.stage
```

Use them when a DAG has enough tasks that the Graph view becomes noise - collapse
each logical stage into one box.

---

## 2. Two ways to make a TaskGroup

### (a) `@task_group` decorator - pairs with TaskFlow (what we use)

`from airflow.sdk import task_group`. Decorate a function; every `@task` defined
and called inside becomes a member of the group.

```python
@task_group(group_id="ingest")     # group_id string
def load():                        # function name - kept different from the group_id
    @task
    def download() -> str:
        return "/raw/file"

    @task
    def stage(path: str) -> None:
        print(f"staging {path}")

    stage(download())        # wiring inside the group, TaskFlow-style
```

### (b) `TaskGroup` context manager - pairs with classic operators

`from airflow.sdk import TaskGroup`. Everything created inside the `with` block
joins the group. (Shown for awareness - this session builds in TaskFlow.)

```python
with TaskGroup(group_id="ingest") as ingest:
    download = EmptyOperator(task_id="download")
    stage    = EmptyOperator(task_id="stage")
    download >> stage
```

Both produce the same collapsible "ingest" node.

---

## 3. `group_id` prefixing - the part that trips people up

Inside a group, a task's **real id becomes `group_id.task_id`.** So `download`
inside group `ingest` is actually `ingest.download`. This is how Airflow keeps
ids unique even if two groups both have a `validate` task (`orders.validate` vs
`users.validate`).

Consequences:

- In `airflow tasks test`, you reference the **full** id: `airflow tasks test <dag> ingest.download 2026-01-01`.
- The prefix is why you can **reuse the same group definition** for multiple inputs without id collisions.
- `prefix_group_id=False` on the group turns prefixing off (rarely needed; you then must keep ids unique yourself).

### Function name vs `group_id`

Like `@dag`/`dag_id`, they are independent:

- The **function name** is a Python label, cosmetic.
- **`group_id`** is the group's real identity (the prefix on child task ids).

They can differ, and this course always keeps them different so it's obvious
which is which. If you omit `group_id`, the **function name becomes it**:

```python
@task_group(group_id="ingest")
def load():            # function name irrelevant -> prefix is "ingest"
    ...

@task_group            # no group_id
def orders():          # function name IS the group_id -> prefix is "orders"
    ...
```

---

## 4. Nesting groups (TaskFlow)

Groups nest. A `@task_group` defined inside another `@task_group` prefixes twice.
Function names (`source`, `checks`) are kept different from the group_id strings
(`"orders"`, `"quality"`) on purpose:

```python
@task_group(group_id="orders")
def source():                                  # function name != group_id "orders"
    @task
    def download() -> str:                     # -> orders.download
        return "/raw/orders"

    @task_group(group_id="quality")
    def checks(path: str):                     # function name != group_id "quality"
        @task
        def check_nulls(p: str) -> str:        # -> orders.quality.check_nulls
            return p

        @task
        def check_schema(p: str) -> str:       # -> orders.quality.check_schema
            return p

        check_nulls(path)
        check_schema(path)

    checks(download())                         # download's output flows into the nested group

source()
```

The id builds up one prefix per nesting level: `orders.quality.check_nulls` -
note the ids come from the **group_id** strings, not the function names. The UI
shows `orders` collapsing to reveal `download` and a nested `quality` box.

Note the wiring is pure TaskFlow: `checks(download())` passes `download`'s
XComArg into the nested group, which draws the edge automatically (Session 01) -
no `>>` needed.

---

## 5. Wiring groups together

### Ordering groups with `>>`

You can set dependencies on **whole groups** just like tasks - `>>` works on a
group object:

```python
group_a >> group_b        # every task in group_b waits for group_a to finish
```

**Does `>>` work in TaskFlow?** Yes. `>>` is defined on `@task` results (XComArgs)
and on group results, not just classic operators:

```python
a() >> b()                # dependency only, no data passed - valid TaskFlow
g1 = first_group()
g2 = second_group()
g1 >> g2                  # order the groups
```

Rule of thumb in TaskFlow:

- **Data to pass?** wire by passing it - `stage(download(x))` - edge is automatic.
- **No data, just ordering?** use `>>` - `a() >> b()`, `g1 >> g2`.

(`g1 >> g2` needs the group calls to return something wireable; if a group
returns `None`, give it a `return` or order by data flow.)

### Reusing the same group for many inputs

You wrote a group once. Give its three pieces different names so it's obvious
which is which - function name `load_source`, group_id `"box"`, input `source`:

```python
@task_group(group_id="box")
def load_source(source):
    ...
```

| Name | What it is |
|---|---|
| `load_source` | the function name - a label for your code (cosmetic) |
| `"box"` | the group_id - the name Airflow shows in the UI (must be unique) |
| `source` | the input you pass in |

You want it for orders, users, products. Calling it three times **crashes** -
all three become a group named `"box"`, and group_ids must be unique:

```python
load_source("orders")     # group "box"
load_source("users")      # group "box" AGAIN -> collision
```

`.override(group_id="...")` means "same code, new name". Give each a unique name:

```python
load_source.override(group_id="box_orders")("orders")
load_source.override(group_id="box_users")("users")
load_source.override(group_id="box_products")("products")
```

Read one line: *"run the `load_source` code, name this group `box_orders`, feed
it `orders`."* The `load_source` before the dot is the **function** (the Python
variable); `.override` is called on it. The two parentheses are two steps -
`.override(...)` sets the name, `(...)` then calls it.

Those three lines only differ by the source name, so shorten them into a loop:

```python
for name in ["orders", "users", "products"]:
    load_source.override(group_id=f"box_{name}")(name)
```

Each pass, `name` is one source and `f"box_{name}"` glues `box_` onto it to build
the unique group_id (`box_orders`, `box_users`, `box_products`). One group
definition, three renamed copies.

---

## 6. A complete runnable DAG (your reference, TaskFlow)

A whole file, end to end, before the spec. One parametrized `@task_group`,
instantiated twice with `.override(group_id=...)`. Names are kept **deliberately
different** so it's obvious which is which: function name `load_source`, group_id
`"src"`.

```python
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task, task_group


@dag(
    dag_id="task_group_demo",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["course", "demo"],
    default_args={"owner": "akhand", "retries": 2},
)
def pipeline():

    # function name: load_source   |   group_id: "src"
    @task_group(group_id="src")
    def load_source(source: str) -> None:
        @task
        def download(name: str) -> str:        # -> src_orders.download
            return f"/raw/{name}"

        @task
        def validate(path: str) -> str:        # -> src_orders.validate
            return path

        @task
        def stage(path: str) -> None:          # -> src_orders.stage
            print(f"staging {path}")

        stage(validate(download(source)))      # TaskFlow wiring inside the group

    # call .override on the FUNCTION (load_source), giving each a distinct group_id
    for name in ["orders", "users"]:
        load_source.override(group_id=f"src_{name}")(name)


pipeline()
```

Read the shape:

- `load_source` is the **function** (Python variable); `"src"` is the default
  `group_id`. They are different names on purpose - `.override` is called on the
  function, and it sets a new `group_id`.
- `.override(group_id=f"src_{name}")` gives each call a unique id so the two
  instances don't collide.
- Inside, wiring is pure TaskFlow - `stage(validate(download(source)))` draws
  `download -> validate -> stage` via data flow (Session 01), no `>>`.
- Task ids come out prefixed: `src_orders.download`, `src_users.stage`, ...
- No SubDAG, no extra scheduling cost - just structure.

Run it:

```bash
python dags/task_group_demo.py
airflow dags test task_group_demo 2026-01-01
```

---

## 7. Build spec - you write this (no solution, TaskFlow)

Create `dags/03_task_groups.py`, dag_id `03_task_groups`. Build a multi-source
ingestion DAG in **pure TaskFlow** that uses **a parametrized group AND a nested
group**. Keep every function name different from its `group_id` string.

**Structure:**

- A `@task_group` (function e.g. `load_source`, group_id `"src"`), instantiated
  for **three** sources: `orders`, `users`, `products` - each with a distinct
  `group_id` (e.g. `src_orders`).
- Inside each group:
  - `@task download(name)` -> returns a path string.
  - a **nested** `@task_group` (function e.g. `run_checks`, group_id `"quality"`)
    containing two `@task`s, `check_nulls(path)` and `check_schema(path)`, each
    returning `path`.
  - `@task stage(path)` -> prints "staged {path}".
  - wired by data flow: `download`'s output feeds the checks group, whose output
    feeds `stage`.

**The thinking part:**

- Instantiate the same group three times without `group_id` collisions using
  `.override(group_id=f"src_{name}")(name)`.
- Nest the checks group inside and confirm the ids come out as
  `src_orders.quality.check_nulls`, etc.
- Thread the path through the nested group with TaskFlow data flow - the nested
  group takes `download`'s output and returns something `stage` consumes. (Decide
  what the nested group should return so `stage` gets a single path.)

**Constraints:**

- Pure TaskFlow - `@task_group` + `@task`, no `EmptyOperator`, no `>>`.
- Function names must differ from their `group_id` strings.
- Integrity gates pass: `tags`, real `owner`, `retries >= 1` via `default_args`.

**Acceptance criteria:**

- `python dags/03_task_groups.py` parses cleanly.
- UI Graph shows three collapsible `src_*` groups, each with a nested `quality`
  box between `download` and `stage`.
- `airflow tasks test 03_task_groups src_orders.quality.check_nulls 2026-01-01`
  runs (proves the nested prefixed id is correct).
- `airflow dags test 03_task_groups 2026-01-01` runs everything green.
- `python -m pytest tests/ -v` stays green.

---

## 8. Production tip - groups organize, they don't isolate

A TaskGroup is cosmetic + namespacing only. It does **not** give you per-group
concurrency limits, retries, or resource isolation - those live on tasks
(`pool`, `priority_weight`, `retries`) or the DAG. Don't reach for TaskGroups
expecting isolation; reach for them to make a 50-task Graph readable. For real
isolation between workloads, that's pools/queues/executors (Sessions 13, 24).

Also: keep `group_id`s meaningful and stable - they become part of every child
task's id, so renaming a group renames all its tasks' history.

---

## 9. Verify + commit

```bash
python dags/03_task_groups.py
airflow dags test 03_task_groups 2026-01-01
python -m pytest tests/ -v
git add dags/03_task_groups.py && git commit -m "course: 03 task groups" && git push
```

Done when CI is green. Tick session 03 in `docs/course/README.md`.

**Pre-push habit:** run
`ruff check dags/ include/ tests/ --select E,F,AIR3 && python -m pytest tests/ -v`
before every push - green locally means green CI, no failure emails.
