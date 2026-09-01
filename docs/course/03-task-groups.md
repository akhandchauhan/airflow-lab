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
which is which. If you omit `group_id`, the **function name becomes it**.

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

Run it (the demo also lives in your session subfolder):

```bash
python dags/task-3/task_group_demo.py
airflow dags test task_group_demo 2026-01-01
```

---

## 7. Build spec - your challenge (no solution)

**File:** `dags/task-3/03_task_groups.py`  ·  **dag_id:** `03_task_groups`

Build a **multi-source ingestion pipeline** in pure TaskFlow.

**The problem:**

- The pipeline ingests **three** data sources: `orders`, `users`, `products`.
- Each source is handled by its **own task group** - but you must **reuse one
  group definition** for all three, not copy-paste it three times.
- Within each source's group, the data must:
  1. be **downloaded** (a task that produces some path/handle),
  2. pass **two quality checks** that live in a **nested group** inside the
     source group,
  3. be **staged** by a final task, which only runs after both checks pass.
- The whole thing is wired by **data flow** (values passed between tasks), the
  TaskFlow way.

**Constraints:**

- Pure TaskFlow - `@task_group` + `@task`. No `EmptyOperator`, no `>>`.
- Reuse the source-group definition; the three instances must not collide on
  `group_id`.
- Keep every **function name different from its `group_id`** string.
- Passes the integrity gates: `tags`, real `owner`, `retries >= 1` via
  `default_args`.

**Acceptance criteria (how you'll know it's right):**

- `python dags/task-3/03_task_groups.py` parses (prints nothing).
- The UI Graph shows **three** collapsible source groups, each containing a
  **nested** checks group sitting between download and stage.
- The nested check tasks have **doubly-prefixed** ids (e.g. something like
  `<source>.<checks>.<check_name>`) - pick one and run it with
  `airflow tasks test 03_task_groups <that.full.id> 2026-01-01`.
- `airflow dags test 03_task_groups 2026-01-01` runs everything green.
- `python -m pytest tests/ -v` stays green.

**One nudge (only if stuck):** you learned the tool for "reuse a group without an
id collision" in section 5. Nesting takes care of the inner group's uniqueness on
its own - see the note right below.

---

## 7b. Why the nested group needs no `.override`

`.override(group_id=...)` exists for **one reason**: to avoid a duplicate
`group_id` when you create the **same group more than once at the same level**.

- The source group is created **3 times at the top level** (once per source).
  Left alone, all three would share one `group_id` -> collision -> so each needs
  a unique name via `.override`.
- The checks group is created **once inside each source group**. Within a single
  source group there is only one checks group -> no collision -> no `.override`.

**"But the source group runs 3 times - isn't the checks group also created 3
times?"** Yes - three checks groups get created. They still don't collide,
because **nesting auto-prefixes with the parent's group_id**:

```
src_orders.quality      <- checks group inside src_orders
src_users.quality       <- checks group inside src_users
src_products.quality    <- checks group inside src_products
```

The parent prefix (`src_orders`, `src_users`, `src_products`) makes each
`quality` unique for free. You override only the **parent**; every nested group
inherits that uniqueness.

| Situation | Need `.override`? |
|---|---|
| Same group created N times **at the same level** | ✅ Yes - rename each |
| Group nested inside an already-unique parent | ❌ No - parent prefix makes it unique |

So you override at the level where the collision would happen (the top loop), and
nesting handles the rest.

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
python dags/task-3/03_task_groups.py
airflow dags test 03_task_groups 2026-01-01
python -m pytest tests/ -v
git add -A && git commit -m "course: 03 task groups" && git push
```

Done when CI is green. Tick session 03 in `docs/course/README.md`.

> Paths use your subfolder layout: session N lives in `dags/task-N/`. The
> `airflow dags test <dag_id>` and pytest commands are path-independent (they use
> the dag_id / scan `dags/` recursively); only the `python dags/...py`
> parse-check needs the real file path.

**Pre-push habit:** run
`ruff check dags/ include/ tests/ --select E,F,AIR3 && python -m pytest tests/ -v`
before every push - green locally means green CI, no failure emails.
