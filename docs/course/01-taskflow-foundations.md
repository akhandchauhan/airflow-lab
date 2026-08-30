# Session 01 - W1 Sat - TaskFlow Foundations

**Goal:** author a DAG entirely with `@dag` / `@task`, understand the machinery
underneath (parse-time vs run-time, what an `XComArg` really is), and use
`multiple_outputs` to split a returned dict into separate XComs.

---

## 0. The library you didn't recognize: `pendulum`

**What it is:** a third-party Python library for dates and times - a near
drop-in replacement for the standard library's `datetime`.

**The problem it solves.** Python's built-in `datetime` objects come in two forms:

- **naive** - no timezone (`datetime(2026, 1, 1)`). Python can't tell if that's 1 AM in Delhi or 1 AM in New York.
- **aware** - carries a timezone (`datetime(2026, 1, 1, tzinfo=timezone.utc)`).

Airflow's whole job is scheduling across time - intervals, cron, daylight-saving
shifts, backfills over months. A naive datetime silently assumes some local time
and creates off-by-hours bugs that only appear in production. So Airflow
**standardizes on pendulum**, which is timezone-aware by default and refuses to
be ambiguous.

**What you actually use:**

```python
import pendulum

pendulum.datetime(2026, 1, 1, tz="UTC")   # tz-aware instant: midnight 1 Jan 2026 UTC
pendulum.duration(minutes=5)              # a span of time (like timedelta, richer)
```

### `pendulum.datetime()` parameters and defaults

Same positional order as the stdlib `datetime`:

```
pendulum.datetime(year, month, day, hour=0, minute=0, second=0, microsecond=0, tz="UTC")
```

| Position | In the example | Meaning | Default if omitted |
|---|---|---|---|
| `year` | `2026` | the year | (required) |
| `month` | `1` | January (1-12, **not** zero-indexed) | (required) |
| `day` | `1` | 1st of the month (1-31) | (required) |
| `hour` | omitted | hour of day (0-23) | `0` |
| `minute` | omitted | minute | `0` |
| `second` | omitted | second | `0` |
| `microsecond` | omitted | microsecond | `0` |
| `tz` | `"UTC"` | timezone (keyword arg) | `"UTC"` |

So `pendulum.datetime(2026, 1, 1, tz="UTC")` passes only the first three
positionals; hour/minute/second default to `0`, giving **midnight**:

```
2026-01-01 00:00:00+00:00      # the +00:00 offset is the "aware" part
```

Examples with a time component:

```python
pendulum.datetime(2026, 1, 1, 1, tz="UTC")        # 4th positional = hour -> 01:00
pendulum.datetime(2026, 1, 1, 13, 30, tz="UTC")   # 1:30 PM -> 13:30:00
```

For a DAG's `start_date` you almost always want the clean start of a day, so the
midnight form (first three args only) is the normal choice - you rarely set
hours/minutes on a `start_date`.

Verify:

```bash
python -c "import pendulum; print(repr(pendulum.datetime(2026, 1, 1, tz='UTC')))"
# DateTime(2026, 1, 1, 0, 0, 0, tzinfo=Timezone('UTC'))
```

A pendulum `DateTime` **subclasses** the stdlib `datetime`, so anywhere Airflow
expects a datetime, pendulum works. It flows the other way too: the values
Airflow injects into your tasks - `data_interval_start`, `logical_date` - **are
pendulum objects**, so methods like `.to_date_string()` -> `"2026-01-01"` or
`.add(days=1)` are available on them. That is why learning it now pays off for
the whole course.

You *could* use stdlib `datetime` with an explicit UTC tzinfo and it would run.
Pendulum is the convention, and the context objects are pendulum anyway, so use
it.

---

## 1. The core idea: when does my code actually run?

This is the thing that makes TaskFlow click. Your DAG file lives in **two time
worlds**, and the same code means different things in each.

```
  PARSE TIME                              RUN TIME
  (DAG Processor, every ~30s)            (a Worker, when a task is scheduled)
  --------------------------            ----------------------------------
  imports your .py file                  runs the BODY of ONE @task function
  runs top-level code                    e.g. the actual `return {...}`
  runs the @dag function body            produces real values
  -> builds the DAG graph                reads/writes XCom rows
  -> NO task bodies execute here
```

At parse time Airflow is **constructing a graph**, not doing your work. Task
function bodies do not run here - they run later, one at a time, on workers.
Hold that split; everything below follows from it.

---

## 2. What `@dag` actually does

`@dag` turns your `pipeline` function into a **DAG factory**. Decorating it
builds nothing. The DAG is constructed only when you **call** `pipeline()` - the
last line of the file. That call runs the function body once, at parse time, and
the calls inside it register the graph.

Forget the final `pipeline()` call -> no DAG object is created -> it never shows
up in Airflow. (The #1 "my DAG isn't appearing" cause.)

### Do we really need `pipeline()`? Yes.

Without the call, the module contains a function *definition* but **no DAG
object** in its namespace. The DAG Processor's DagBag scans module globals for
DAG instances, finds none, and the DAG silently never appears - no error, just
absent.

Contrast with the classic style (`my_first_dag.py`):

```python
dag = DAG(dag_id="my_first_dag", ...)   # DAG object created immediately at import
```

Here `DAG(...)` is instantiated directly at module level, so the object exists
the moment the file is imported - no extra call needed.

| Style | What creates the DAG object | Extra call needed? |
|---|---|---|
| Classic - `dag = DAG(...)` | the `DAG(...)` call at module level | No |
| TaskFlow - `@dag def pipeline()` | calling `pipeline()` | **Yes** |

Rule of thumb: **a `@dag`-decorated function must be called at the bottom of the
file.**

---

## 3. What `@task` does - and why `data` is not a dict

The part that looks like magic. Inside the `@dag` body:

```python
data = extract()
```

You'd expect `extract()` to run `return {...}` and give you a dict. **It does
not.** A `@task`-decorated function is no longer a normal function. Calling it at
parse time does two things:

1. **Registers a task node** named `extract` in the DAG graph.
2. **Returns an `XComArg`** - a lazy placeholder meaning *"the future output of
   the extract task."*

So `data` is an `XComArg`, a handle to a value that does not exist yet. No dict
has been produced, because `extract`'s body has not run - and won't until a
worker executes it.

Then:

```python
transform(path=data["path"], rows=data["rows"])
```

- `data["path"]` returns **another XComArg** - a sub-reference to the `"path"`
  key of extract's future output.
- Passing those XComArgs into `transform` tells Airflow two things at once:
  - **dependency:** `transform` needs `extract`'s output -> draw edge `extract -> transform`.
  - **wiring:** at run time, pull `extract`'s XCom, take those keys, feed them in.

That is why TaskFlow needs no `>>` and no `xcom_push`/`xcom_pull`: **a data
dependency IS the task dependency.** Airflow reads ordinary-looking function
composition and reinterprets it as graph construction + XCom plumbing. The
official name is **functional DAG building**.

---

## 4. The XComArg -> value handoff (granular)

Trace one value, `path`, from definition to consumption. Watch which world each
step lives in.

```
DEFINITION (parse time)                       what `path` is
---------------------------------------       -------------------------------
data = extract()                              data = XComArg(extract)
                                              (placeholder: "extract's output")

p = data["path"]                              p    = XComArg(extract)["path"]
                                              (placeholder: the 'path' key of it)

transform(path=p, rows=...)                   registers edge extract -> transform,
                                              records "arg 'path' <- that XComArg"
                                              NOTHING has executed yet
===============================================================================
EXECUTION (run time, later, on workers)       what `path` is
---------------------------------------       -------------------------------
1. worker runs extract()                      returns {"path": "...", "rows": 4213}
2. Airflow writes XCom rows:                  DB row: (dag, run, task=extract,
     multiple_outputs -> one row per key          key="path", value="gs://...")
                                              DB row: (..., key="rows", value=4213)
3. transform is scheduled; Airflow            reads XCom row key="path",
   resolves each XComArg arg:                     deserializes -> "gs://..."
4. calls transform(path="gs://...",           path = "gs://..."  (real str!)
                    rows=4213)                 the XComArg is gone; only the value
```

The one-line rule: **XComArg at definition time -> real value at run time.**
The placeholder exists only to build the graph and record the wiring; by the
time your `transform` body runs, `path` is a plain `str`.

Where the values physically live: the `xcom` table in the metadata database,
keyed by `(dag_id, run_id, task_id, map_index, key)`. That composite key is why
XCom is scoped to one DAG + one run - task B in today's run cannot read task A's
value from yesterday's.

---

## 5. `multiple_outputs` precisely

- **Without it:** whatever you return is stored as **one** XCom value. A returned
  dict comes back whole; you subscript it *after* it is pulled.
- **With it (`multiple_outputs=True`):** Airflow iterates the returned dict and
  stores **each key as its own XCom**, and the returned XComArg supports
  `["key"]` at definition time to reference each one. Valid only when the
  function genuinely returns a dict - annotate `-> dict` so it is obvious.

Why care: a downstream task can depend on *just* `path` without dragging the
whole payload through, and each value is separately inspectable in the UI.

---

## 6. API + example (every line, knowing what it means)

```python
from __future__ import annotations       # lets you write `-> dict` etc. cleanly

import pendulum
from airflow.sdk import dag, task         # airflow.sdk = Airflow 3's public authoring API


@dag(
    dag_id="taskflow_example",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),  # tz-aware anchor
    schedule=None,                        # manual trigger only
    catchup=False,
    tags=["course", "w1", "taskflow"],
    default_args={"owner": "akhand", "retries": 2},      # applied to every task
)
def pipeline():
    @task(multiple_outputs=True)
    def extract() -> dict:
        return {"path": "gs://lake/raw/2026-01-01/", "rows": 4213}

    @task
    def transform(path: str, rows: int) -> int:
        print(f"transforming {rows} rows from {path}")
        return rows

    @task
    def load(row_count: int) -> None:
        print(f"loaded {row_count} rows")

    data = extract()                                        # XComArg, not a dict
    load(transform(path=data["path"], rows=data["rows"]))   # builds edges + wiring


pipeline()                                                  # constructs the DAG. Required.
```

This example is a straight line. Your build spec (below) is harder - a diamond.

---

## 7. Build spec - you write this (no solution)

Same concepts (`@task`, `multiple_outputs`, XCom auto-wiring), but a shape you
have to reason about - a **diamond**, not a straight line, with one task that
consumes values from two different upstream tasks.

Create `dags/01_taskflow_foundations.py`, dag_id `01_taskflow_foundations`. A
daily-sales mini-pipeline.

**Tasks:**
1. `fetch_orders` - `@task(multiple_outputs=True)`. Return a dict with:
   - `gross_revenue: float`
   - `order_count: int`
   - `currency: str`
2. `fetch_refunds` - `@task`. Return a single `float`: total refunds for the day. (Not `multiple_outputs` - think about why one value doesn't need it.)
3. `net_revenue` - `@task`. Takes gross revenue and total refunds, returns `gross - refunds` as a `float`.
4. `avg_order_value` - `@task`. Takes net revenue and order count, returns `net / order_count` rounded to 2 decimals.
5. `report` - `@task`. Takes net revenue, average order value, **and** currency, and logs one line like `net 39120.50 USD across 300 orders, AOV 130.40`. Returns `None`.

**The graph you must produce (work out the wiring yourself):**

```
fetch_orders --+--> net_revenue --+--> avg_order_value --+
fetch_refunds -+                  +---------------------+--> report
               (report also needs currency from fetch_orders)
```

**Constraints - this is where you think:**
- Only function calls create dependencies. No `>>`, no `xcom_push`/`xcom_pull`.
- `report` depends on **three** values from **two different tasks** (`net_revenue`, `avg_order_value`, and `currency` from `fetch_orders`). Figure out how to thread `currency` to `report` when `net_revenue` and `avg_order_value` don't carry it.
- `order_count` is used in two places (`avg_order_value` and the `report` message) - pass the same XComArg to both.
- All 5 tasks must satisfy the integrity gates (tags, real owner, `retries >= 1` via `default_args`).

**Acceptance criteria:**
- Graph shows the diamond: both fetches fan into `net_revenue`/onward, and `report` has multiple incoming edges.
- `airflow dags test 01_taskflow_foundations 2026-01-01` runs all 5 tasks green.
- The `report` log line shows the right numbers.
- No `xcom_p` anywhere in the file.

**Hints (don't read unless stuck):**
- An XComArg can be passed to more than one downstream task - reuse `orders["currency"]` and `orders["order_count"]` wherever needed.
- A returned XComArg is just a value handle; hold it in a variable and pass it into several calls.

---

## 8. Production tip - top-level code

Everything at **module scope re-runs on every parse** (~30s per file), because
parse time imports the whole module. Keep heavy imports inside task bodies so
they cost nothing until the task runs:

```python
@task
def transform(path: str):
    import pandas as pd   # runs on a worker, not on every parse cycle
    ...
```

A top-level `import pandas` - or worse, an API/DB call at module scope - steals
scheduler CPU on every parse cycle, forever.

---

## 9. Verify + commit

```bash
python dags/01_taskflow_foundations.py                        # must parse cleanly
airflow dags test 01_taskflow_foundations 2026-01-01          # runs the whole DAG
python -m pytest tests/ -v                                    # integrity gates pass
git add dags/01_taskflow_foundations.py && git commit -m "course: 01 taskflow foundations" && git push
```

Done when CI is green. Tick session 01 in `docs/course/README.md`.

---

## 10. Why `print()` shows up in the logs (no import needed)

You used `print("loaded ...")` and it appeared in the UI Logs tab as
`INFO - loaded 4213 rows` - without importing anything. Here's why.

`print()` writes to **stdout** (the process's standard output stream). Normally
that goes to a terminal, but Airflow does not run your task in a terminal - it
wraps the task's execution and **redirects stdout/stderr to the task's logger**.
That logger has a handler that writes to the per-task log file, which is what the
UI reads.

```
your code:      print("loaded 4213 rows")
                        | writes to stdout
Airflow wrapper:  captures stdout, feeds each line to the task logger
                        | logger formats it
log file:       [2026-08-30 11:34:55] INFO - loaded 4213 rows
                        |
UI Logs tab:    renders the file
```

**The `INFO` and timestamp are not yours.** `print` outputs only the raw text
`loaded 4213 rows`. Airflow's log formatter adds the `INFO` level and the
timestamp when it captures the line. Captured stdout is always tagged **INFO**
(stderr -> ERROR).

**Is `logging` built in? Yes.** Python's `logging` module is part of the
**standard library** - always present, no `pip install`. You did not import it
because you used `print`; Airflow itself imports and configures `logging`
(handlers, formatters, per-task file routing) at startup, and you inherit that.

**Other lines Airflow adds around every task:**

| Line | Who wrote it |
|---|---|
| `Log message source details`, `Pre Execute`, `Post Execute` | Airflow lifecycle markers |
| `loaded 4213 rows` | your `print()` |
| `Done. Returned value was: None` | Airflow logging the task's return value |

### Production tip - use a logger, not `print`

`print` works via stdout capture, but real tasks use the standard logger:

```python
import logging

log = logging.getLogger(__name__)

@task
def load(row_count: int) -> None:
    log.info("loaded %s rows", row_count)      # proper level
    log.warning("row count below threshold")   # impossible with print
```

Why better:
- **Levels** - `print` is always INFO; a logger emits WARNING/ERROR, which you can filter in the UI "All Log Levels" dropdown and alert on.
- **Structured** - `log.info("loaded %s rows", n)` defers formatting and works with remote/structured logging (S3/Elasticsearch) in production.
- **No stdout dependency** - goes straight to the logger.

`print` is fine for learning; switch to `logging.getLogger(__name__)` for
anything real.
