# Session 08 · Dynamic Task Mapping

**Goal:** fan **one** task definition out into **N** parallel task instances at run
time — a `for` loop that *Airflow* runs, where N isn't known until an upstream task
produces the list. Understand that the mapping happens at **run time** (not when the
file is parsed), that each mapped instance is a **real, independent task instance**
(its own retries, logs, UI square), and how to fan the results back in. Plain DAGs, no
BigQuery.

---

## 1. What it is (and what it is NOT)

`.expand()` takes a **list** and creates **one task instance per element**. If the
list has 3 items, you get 3 instances of that task, run in parallel, each receiving
one element.

The point is the same parse-time / run-time split you've seen since Session 01:

```
PARSE TIME                              RUN TIME
----------                              --------
process.expand(x=upstream())            upstream() returns [a, b, c]  →  3 instances:
  → the DAG knows "process is mapped"        process[0]=a  process[1]=b  process[2]=c
    but NOT how many                         (created now, in parallel)
```

What it is **NOT**:

| Not this | Because |
|---|---|
| a plain Python `for` loop in the DAG body | a `for` loop runs at **parse time** — N is baked into the file, and each pass must be a *different* task. `.expand` decides N at **run time** from data |
| one task that loops internally | `.expand` gives you **N separate task instances** — each with its own retry, log, and status square. One fails, only that one retries |
| a TaskGroup | groups *organize* existing tasks; mapping *creates* tasks from data |

Reach for it when the number of things to process depends on data: N files that
landed, N regions returned by an API, N shards for a date range.

---

## 2. `.expand()` — map over a list

Call `.expand(kwarg=list)` on a `@task`. Each element is passed as that kwarg to one
instance.

```python
@task
def process(item: str) -> int:
    return len(item)

process.expand(item=["orders", "users", "products"])   # ← 3 instances, one per item
```

- Only **keyword** arguments — `.expand(item=...)`, never positional.
- The 3 instances run **in parallel** (subject to your concurrency limits).

---

## 3. `.partial()` — the arguments that DON'T change

Some args are the same for every instance. Freeze them with `.partial()`; only the
`.expand()` arg varies.

```python
@task
def process(item: str, bucket: str) -> int:     # bucket is constant; item varies
    print(f"{item} from {bucket}")
    return len(item)

process.partial(bucket="raw-zone").expand(item=["orders", "users"])   # ← THE MECHANIC
```

Read it: *"same `bucket` for all; fan out over `item`."* Every non-expanded argument
must be supplied by `.partial()`.

---

## 4. Mapping over an upstream's output — the real use

The list usually isn't hardcoded — it comes from a task that ran first. Pass the
upstream task's return straight into `.expand()`; Airflow waits for it, then creates
the instances.

```python
@task
def list_items() -> list[str]:
    return ["orders", "users", "products"]     # N decided here, at run time

@task
def process(item: str) -> int:
    return len(item)

process.expand(item=list_items())              # N instances, N unknown until list_items runs
```

This is the whole reason mapping exists: **N is data, not code.**

---

## 5. Reduce — fanning the results back in

A **normal** (non-mapped) task that takes the mapped task's output receives the
**whole list** of results — that's the fan-in / reduce step.

```python
@task
def summarize(sizes: list[int]) -> None:       # receives ALL mapped return values
    print(f"processed {len(sizes)} items, total {sum(sizes)}")

sizes = process.expand(item=list_items())
summarize(sizes)                               # one instance, gets [len0, len1, len2]
```

`process` fans out to N; `summarize` fans back in to 1. (`sizes` is a lazy proxy, not
a real list — iterate it or `sum()` it; don't index it at parse time.)

---

## 6. The rest, briefly

| Tool | Use |
|---|---|
| `.expand_kwargs([{...}, {...}])` | map over a list of **dicts** — each dict supplies *several* kwargs to one instance |
| `list_a.zip(list_b)` | pair up two lists → instances get `(a, b)` tuples (like Python `zip`) |
| `.map(fn)` | transform each element with a **plain function** (not a task) before expanding |
| `max_map_length` | cap on instances per mapped task — default **1024**; exceeding it fails the run |

You'll use `.expand` + `.partial` + reduce 90% of the time; the rest are there when a
shape needs them.

---

## 7. A complete runnable DAG (your reference)

A whole file: list → mapped process (with a constant via `.partial`) → reduce. Plain,
no BigQuery. Names kept distinct (R12): `list_files` (verb), `process` (verb),
`summarize` (verb); the variable `sizes` is the role.

```python
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s8_examples",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-8", "dynamic-mapping"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():

    @task
    def list_files() -> list[str]:
        return ["orders.csv", "users.csv", "products.csv"]     # N decided at run time

    @task
    def process(filename: str, bucket: str) -> int:
        print(f"processing {filename} from {bucket}")
        return len(filename)                                   # a per-item result

    @task
    def summarize(sizes: list[int]) -> None:
        print(f"processed {len(sizes)} files, total name length {sum(sizes)}")

    sizes = process.partial(bucket="raw-zone").expand(filename=list_files())
    summarize(sizes)


pipeline()
```

```bash
python dags/s8/s8_examples.py
airflow dags test s8_examples 2026-01-01
```

In the Graph you'll see `process` as a **mapped** node showing **[3]** — three
instances, one per file — then `summarize` running once on all three results. Add a
file to `list_files` and rerun: four instances, no code change to `process`. That's
the payoff.

---

## 8. Build spec — your challenge (no solution)

**File:** `dags/s8/s8_assignment.py`  ·  **dag_id:** `s8_assignment`

Build a plain fan-out / fan-in DAG.

**The problem:**

- A task returns a list of **region names** (e.g. `["us", "eu", "apac"]`) — the list
  is produced at run time, not hardcoded in `.expand`.
- A mapped task processes **each region**, taking the region plus **one constant**
  arg supplied with `.partial()` (e.g. a `year`), and returns a per-region number.
- A **reduce** task takes all the per-region numbers and prints a total.

**Constraints:**

- Pure TaskFlow, plain DAGs — no BigQuery.
- The mapped list comes from the **upstream task's output**, not a literal in `.expand`.
- Names distinct (R12): the variable holding the mapped result is its role, not `expand`.
- Passes the integrity gates: `tags`, real `owner`, `retries >= 1`.

**Acceptance criteria:**

- `python dags/s8/s8_assignment.py` parses (prints nothing).
- The Graph shows the mapped task as **[N]** with one instance per region, and the
  reduce task running once.
- `airflow dags test s8_assignment 2026-01-01` runs green.
- Add a region to the list, rerun, and see one more instance — with no change to the
  mapped task.
- `python -m pytest tests/ -v` stays green.

---

## 9. Production tip — a mapped task is N real tasks; size it

- **`max_map_length` (default 1024) is a guardrail, not a target.** If the upstream
  list is user- or data-driven, a bad run can try to spawn thousands of instances —
  each a real scheduler/DB row. Cap the list size deliberately, and batch (map over
  100 chunks of 1000, not 100k singletons) when the count is large.
- **Each instance must be idempotent on its own.** A mapped instance can retry alone
  (Session 05). If `process(item=x)` appends or writes, a retry of just that instance
  must land the same result — key the write by the item, don't blind-append.

---

## 10. Verify + commit

```bash
python dags/s8/s8_assignment.py
airflow dags test s8_assignment 2026-01-01
python -m pytest tests/ -v
git add -A && git commit -m "session 08: dynamic task mapping" && git push
```

Done when the Graph shows the mapped `[N]` node and the reduce runs once. Tick
Session 08 in `docs/course/README.md`.

**Pre-push habit:** `ruff check dags/ include/ tests/ --select E,F,AIR3 && python -m pytest tests/ -v`
before every push — green locally means green CI.

Sources:
[Dynamic Task Mapping — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html)
