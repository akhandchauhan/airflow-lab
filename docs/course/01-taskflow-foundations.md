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
    dag_id="01_taskflow_foundations",
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

---

## 7. Build spec - you write this (no solution)

Create `dags/01_taskflow_foundations.py`.

**Requirements:**
1. A `@dag` named `01_taskflow_foundations`, `schedule=None`, `catchup=False`, tags include `course`, `default_args` with a real `owner` and `retries >= 1`.
2. `extract` - `@task(multiple_outputs=True)`, returns a dict with at least `source_path: str` and `record_count: int`.
3. `transform` - takes `source_path` and `record_count` as **named arguments**, prints a message, returns the (possibly adjusted) count as an `int`.
4. `load` - takes the transformed count, prints `"loaded N rows"`, returns `None`.
5. Wire by **function calls only** - no `>>`, no `xcom_push`, no `xcom_pull`.

**Acceptance criteria:**
- No `xcom_p` anywhere in the file.
- Graph view shows `extract -> transform -> load`.
- On the `extract` task in the UI, two separate XComs `source_path` and `record_count` (proof `multiple_outputs` worked).

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
airflow tasks test 01_taskflow_foundations transform 2026-01-01
python -m pytest tests/ -v                                    # integrity gates pass
git add dags/01_taskflow_foundations.py && git commit -m "course: 01 taskflow foundations" && git push
```

Done when CI is green. Tick session 01 in `docs/course/README.md`.
