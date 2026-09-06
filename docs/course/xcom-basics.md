# XCom & TaskFlow value passing — reference

How a `@task` return value gets to another task. This is the most important
mechanic in TaskFlow, so it gets its own page. Not a session — a lookup.

## The one trick: calling a `@task` does not run it

```python
count = total_trips()   # count is NOT the number. It's a placeholder ("XComArg").
summarize(count)        # you pass that placeholder into summarize's first argument
```

When you write `total_trips()` in the DAG body, the function **does not execute**.
It hands back a lightweight placeholder object called an **`XComArg`** — think of it
as a **claim ticket** that means *"the future output of the total_trips task."*

## Two phases: wiring (parse) vs running

```
PARSE TIME  (building the graph)            RUN TIME  (executing tasks)
--------------------------------            ----------------------------
count = total_trips()                       total_trips runs, returns 2318447
  → count = <XComArg: total_trips>          → Airflow SAVES it to XCom as
                                              (task_id=total_trips, key=return_value)
summarize(count)
  → Airflow sees an XComArg sitting         summarize is about to run
    in summarize's 1st argument slot        → its arg is that XComArg
  → records: summarize depends on           → Airflow PULLS 2318447 from XCom
    total_trips (an edge in the graph)      → calls summarize(trips=2318447)
```

So the full journey is:

**`return` value → auto-saved to XCom under key `return_value` → pulled back → dropped
into the argument slot you wired it to.**

You never call `xcom_push` / `xcom_pull` yourself. The placeholder plus the function
call **are** the wiring.

## How does the receiving function "know" which value it needs?

**It doesn't decide on its own — you told it at the call site.** This is ordinary
Python argument binding:

```python
def summarize(trips: int) -> None: ...

summarize(count)     # the placeholder goes into the 1st parameter, `trips`
```

- You put the placeholder into a **specific argument slot** (here, the first
  positional argument).
- At run time Airflow swaps the placeholder for the real value **in that same slot**.
- The parameter **name** (`trips`) does not matter for matching — it could be
  `def summarize(x)`. What matters is **which argument you passed the XComArg into**.

The parameter name is just a label inside the function. The wiring is the *position*
(or keyword) you used when you called it.

## Multiple values

- Return a **dict** with `multiple_outputs=True` and each key becomes its own XCom,
  so you can wire keys separately:
  ```python
  @task(multiple_outputs=True)
  def stats() -> dict:
      return {"rows": 100, "cols": 5}

  s = stats()
  load(s["rows"])          # wire one key
  report(s["cols"])        # wire another
  ```
- Pass several placeholders into several parameters — matched by position/keyword
  like any Python call:
  ```python
  combine(count_a(), count_b())      # 1st arg <- count_a, 2nd arg <- count_b
  ```

## What actually sits in XCom

| Producer | XCom key | value |
|---|---|---|
| a `@task` that returns X | `return_value` | X (JSON-serialized by the default backend) |
| `@task(multiple_outputs=True)` returning a dict | one key per dict key | each value |
| `BigQueryInsertJobOperator` | `return_value` | the BigQuery **job id** (not the rows) |
| `@task.branch` | `return_value` | the chosen `task_id` string (used for routing) |

XCom is a **small control-signal store** (backed by the metadata DB). Pass small
values (counts, ids, paths). Never push big data through it — keep large data in the
warehouse or object storage and pass a **reference** (a table name, a GCS path).

## The `@task.branch` special case

A branch looks different because its return is **not** fed into any function header:

```python
path = pick_load_path()                 # path = XComArg, same as always
path >> [run_full_refresh(), run_incremental_load()]
```

The returned string is still saved to XCom (`return_value` of `pick_load_path`), but
`@task.branch` gives it a **special job**: Airflow reads it and treats it as a
**`task_id` to run**. The matching task runs; the others are skipped. Nobody writes
`run_full_refresh(path)`, so you never see it land in a parameter.

- **Normal `@task`:** return → XCom → fed into another task's **argument** (data).
- **`@task.branch`:** return → XCom → tells Airflow **which task_id to run** (routing).

See also: [`gcp-project.md`](gcp-project.md) (the `BigQueryHook.get_first` example
that produces a count), [`04-branching-trigger-rules.md`](04-branching-trigger-rules.md).
