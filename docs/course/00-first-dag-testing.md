# Session 00 · First DAG & Testing

**Goal:** the Stage 0 foundation — understand what a DAG/task/run actually *is*, write
the smallest real DAG, and prove it works **three ways** (parse, `airflow dags test`,
integrity tests) before trusting it with anything. Plain DAG, no BigQuery. *(The thing
a real team does on day one, before betting pipelines on the tool.)*

---

## 1. The mental model

```
   DAG file (.py)                    Scheduler                 Executor
   ─────────────                     ─────────                 ────────
   @dag + @task    ──parsed──▶   decides WHAT runs WHEN   ──▶  RUNS the task
   (the recipe)                  (creates DAG runs)            (a task instance)
```

| Term | What it is |
|---|---|
| **DAG** | the pipeline *definition* — tasks + the order between them (a directed acyclic graph) |
| **Task** | one step in the DAG (a `@task` function, or an operator) |
| **DAG run** | one execution of the whole DAG for a point in time |
| **Task instance** | one task inside one DAG run — the thing that actually runs, retries, logs |
| **Scheduler / DAG processor** | parses your files and creates runs when they're due |
| **Executor** | actually runs the task instances |

A task instance moves through **states** you'll watch in the UI:

```
none → scheduled → queued → running → success
                                    ↘ failed → up_for_retry → running …
                     (branch/short-circuit) ↘ skipped
```

The DAG is just Python that *describes* the graph — it is **not** running your data
logic at parse time. Parsing builds the graph; the executor runs it later.

---

## 2. The smallest real DAG

```python
from airflow.sdk import dag, task

@dag(schedule=None, ...)          # the pipeline
def pipeline():

    @task
    def say_hello() -> str:       # a task that returns a value
        return "hello airflow"

    @task
    def show(msg: str) -> None:   # a task that consumes it (edge drawn automatically)
        print(msg)

    show(say_hello())             # wiring: say_hello → show

pipeline()                        # ← don't forget: call it so Airflow sees the DAG
```

The last line matters: the `@dag` function must be **called** at module level, or
Airflow never registers the DAG.

---

## 3. Prove it works — three checks, cheapest first

| Check | Command | Catches |
|---|---|---|
| **Parse** | `python dags/s0/s0_examples.py` | import errors, syntax, undefined names (prints nothing = OK) |
| **Run it** | `airflow dags test s0_examples 2026-01-01` | runtime errors — runs the whole DAG **without** a scheduler or DB |
| **Gate it** | `python -m pytest tests/ -v` | missing tags/owner/retries, import failures, cycles |

`airflow dags test` is your everyday loop: it executes the DAG in-process for one
logical date and prints the logs, no UI needed. (In Python you can do the same with
`pipeline().test()` under `if __name__ == "__main__":`.)

---

## 4. The integrity tests — the cheapest bug-catcher you own

`tests/dags/test_dag_integrity.py` runs in CI on every push and checks four things
**without** a running Airflow:

| Test | Rule | Why |
|---|---|---|
| `test_no_import_errors` | every file in `dags/` imports cleanly | one broken file can break DAG parsing for the whole repo |
| `test_dags_were_found` | at least one DAG parsed | guards against a silently-empty repo |
| `test_dag_has_tags` | every DAG has non-empty `tags` | how you find a DAG in a UI of 500 |
| `test_tasks_have_retries` | every task `retries >= 1` | a transient blip shouldn't page you |
| `test_tasks_have_real_owner` | `owner` not empty / not `"airflow"` | on-call needs a name |

That's why every DAG in this course carries
`default_args={"owner": "akhand", "retries": 1}` and a `tags=[...]` list — those three
are the gate.

---

## 5. Complete runnable DAG (your reference)

```python
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s0_examples",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-0", "basics"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():

    @task
    def say_hello() -> str:
        return "hello airflow"

    @task
    def show(msg: str) -> None:
        print(f"the task said: {msg}")

    show(say_hello())


dag_obj = pipeline()

if __name__ == "__main__":
    dag_obj.test()          # run it straight from python: `python dags/s0/s0_examples.py`
```

```bash
python dags/s0/s0_examples.py            # with the __main__ block, this RUNS it via .test()
airflow dags test s0_examples 2026-01-01 # or run it through the CLI
python -m pytest tests/ -v               # and prove it passes the gates
```

You'll see `say_hello` run, hand its return to `show`, and `show` print
`the task said: hello airflow`. That's the whole loop: **write → parse → test → gate.**

---

## 6. Your build (no solution)

**File:** `dags/s0/s0_assignment.py`  ·  **dag_id:** `s0_assignment`

Write your **own** first DAG and make it pass every gate.

**The problem:**

- Two `@task`s: one returns your name; one prints a greeting using it.
- Wire them by passing the value (no `>>`).
- Give it `tags`, a real `owner`, and `retries >= 1`.

**Done when:**

- `python dags/s0/s0_assignment.py` parses (prints nothing without the `__main__` block).
- `airflow dags test s0_assignment 2026-01-01` runs green and prints your greeting.
- `python -m pytest tests/ -v` passes — including *your* new DAG.

---

## 7. Production tip — a DAG that won't parse is everyone's problem

- **Parse errors are contagious.** The DAG processor parses every file in `dags/`; a
  syntax error or a heavy import at module level can slow or break parsing for the
  **whole** repo, not just your file. Keep expensive imports **inside** the task body,
  and never do real work at module level.
- **The integrity tests are your seatbelt.** They run in seconds, need no database, and
  catch the "shipped a DAG with no owner/retries" mistakes before they reach the
  scheduler. Run `pytest tests/` before every push — green locally is green CI.

---

## 8. Verify + commit

```bash
python dags/s0/s0_assignment.py
airflow dags test s0_assignment 2026-01-01
python -m pytest tests/ -v
git add -A && git commit -m "session 00: first dag & testing" && git push
```

Done when your DAG runs green and passes the gates. Tick Session 00 in
`docs/course/README.md`, and skim the [Stage 1 recap](stage1-recap.md) to see where
this leads.

Sources:
[Airflow 101 — first DAG & testing (Astronomer)](https://academy.astronomer.io/path/airflow-101),
[Testing DAGs — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/debug.html)
