# Session 26 · Setup & teardown tasks

**Goal:** learn the one dependency shape Airflow has that a plain `>>` arrow cannot express — a resource that must be **spun up before** some work and **always torn down after**, whether the work passed, failed, or was skipped. A normal downstream task runs only when its upstream succeeds; a teardown is the opposite promise — it runs *because* you got that far, not because everything worked. Get this and you stop leaking scratch datasets, idle clusters, and half-open connections that quietly bill you all weekend. This session uses BigQuery via `google_cloud_default` (the scratch dataset is the resource we spin up and tear down).

---

## 1. Why a plain arrow is not enough

Picture the pattern you reach for constantly: create a temporary BigQuery dataset, run a couple of queries into it, then drop it. The naive wiring is `create >> query >> drop`. It works exactly until the day `query` fails — and then `drop` never runs, because in Airflow a downstream task's default trigger rule is `all_success`: it fires only when every upstream succeeded. So the failure that most needs cleanup is precisely the failure that leaks the resource. You wake up to a dataset that has been sitting there (and, for a cluster, *billing*) since the run died at 02:00.

The usual patch is `drop.trigger_rule = "all_done"` so cleanup runs regardless of upstream state. That plugs the leak but breaks two other things: the `drop` task's own red/green now counts toward the DAG run's success (a cleanup hiccup fails an otherwise-fine run), and if you **clear** the `query` task to re-run it, Airflow won't automatically re-run `create` and `drop` around it — the "these three belong together" relationship exists only in your head, not in the graph. Setup/teardown is Airflow's first-class version of that relationship, with the trigger-rule handling, the clearing behavior, and the success accounting all built in.

**The analogy — a campsite.** A setup task is *pitching the tent*; the work tasks are *the camping*; the teardown is *packing the tent away*. You pack up whether the trip was great or it rained the whole time — the one thing you never do is drive home and leave the tent standing in the field. And when you decide to redo the trip (clear the work), you re-pitch and re-pack around it automatically. That "always pack the tent, even after a washout" is the whole feature.

---

## 2. The three roles

| Role | How you mark it | When it runs |
| --- | --- | --- |
| **setup** | `@setup` decorator, or `task.as_setup()` | before its scoped work, like any normal upstream |
| **work** | ordinary `@task` between the setup and teardown | normal `all_success` rules |
| **teardown** | `@teardown` decorator, or `task.as_teardown(setups=...)` | **if its setup succeeded — even if the work failed**; **skips if the setup was skipped** |

The exact guarantee, quoted from the docs: *"A teardown task will run if its setup was successful, even if its work tasks failed. But it will skip if the setup was skipped."* That second half matters — a teardown is not `all_done` cleanup that fires no matter what. It is tied to its **setup**: no tent was pitched, so there is nothing to pack away.

---

## 3. Import paths (Airflow 3, `airflow.sdk`)

```python
from airflow.sdk import dag, setup, task, teardown   # ← THE MECHANIC: setup/teardown live in airflow.sdk
```

`setup` and `teardown` are public decorators in the Task SDK, alongside `dag` and `task`. The older `airflow.decorators` paths still import but are deprecated shims — use `airflow.sdk`.

---

## 4. Marking tasks — two spellings, same result

**Spelling A — decorators (TaskFlow, what we default to):**

```python
@setup
def create_scratch() -> str:                 # ← runs first; its success "arms" the teardown
    return "s26_scratch"

@teardown
def drop_scratch(dataset: str) -> None:      # ← runs even if the work between them failed
    print(f"dropping {dataset}")
```

**Spelling B — `.as_setup()` / `.as_teardown()` on existing tasks (handy for operators):**

```python
create = create_dataset_operator      # any operator/task instance
drop = delete_dataset_operator
create >> run_queries >> drop.as_teardown(setups=create)   # ← wires setup↔teardown in one line
```

`.as_teardown(setups=create)` does three things at once: marks `drop` as a teardown, marks `create` as its setup, and draws the arrows between them. Use Spelling B when the resource is created by a provider operator (like `BigQueryCreateEmptyDatasetOperator`) rather than a `@task`.

---

## 5. Scope — what "between them" means

Every task on a path **between** a setup and its teardown is *in that setup's scope*. Scope is what powers the clearing behavior: clear any scoped work task and Airflow re-runs its setup and teardown with it — you never re-run the camping without re-pitching the tent. You can also nest scopes (an outer cluster teardown wrapping inner per-query teardowns), and a teardown used as a **context manager** wires the scope for you:

```python
with drop_scratch(create_scratch()):   # everything in the block is scoped between them
    summarize()
    validate()
```

---

## 6. `on_failure_fail_dagrun` — does a broken cleanup fail the run?

By default a teardown's own success or failure is **ignored** when Airflow decides whether the DAG run passed — the logic being "the real work is what matters; don't turn a cleanup wobble into a red run." That default is `on_failure_fail_dagrun=False`.

```python
@teardown(on_failure_fail_dagrun=True)   # ← now a failed cleanup DOES fail the run
def drop_scratch(dataset: str) -> None:
    ...
```

Flip it to `True` when the cleanup *is* load-bearing — if failing to drop the dataset means it will leak and bill you, you want that failure loud and red, not swallowed. Rule of thumb: cleanup that only tidies up → leave it `False`; cleanup whose failure costs money or corrupts state → `True`.

---

## 7. Complete runnable reference DAG (BigQuery via `google_cloud_default`)

Spin up a scratch dataset, run a **0-byte** `COUNT` against Stack Overflow, always drop the dataset. `COUNT(*)` reads table metadata only — 0 bytes billed — so no `maximumBytesBilled` cap is needed (R17: prefer aggregates).

```python
# dags/stage-6-scale/s26/setup_teardown_demo.py
from __future__ import annotations

import pendulum
from airflow.sdk import dag, setup, task, teardown

SCRATCH = "s26_scratch"
CONN = "google_cloud_default"


@dag(
    dag_id="s26_setup_teardown_demo",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-26", "setup-teardown"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():

    @setup
    def create_scratch() -> str:
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

        BigQueryHook(gcp_conn_id=CONN).create_empty_dataset(dataset_id=SCRATCH, exists_ok=True)
        return SCRATCH

    @task
    def summarize(dataset: str) -> int:
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

        sql = "SELECT COUNT(*) FROM `bigquery-public-data.stackoverflow.posts_questions`"
        total = BigQueryHook(gcp_conn_id=CONN).get_first(sql)[0]     # 0 bytes billed
        print(f"scratch={dataset}  questions={total}")
        return total

    @teardown(on_failure_fail_dagrun=True)     # a leaked dataset costs money → fail loud
    def drop_scratch(dataset: str) -> None:
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

        BigQueryHook(gcp_conn_id=CONN).delete_dataset(dataset_id=dataset, delete_contents=True)

    scratch = create_scratch()
    scratch >> summarize(scratch) >> drop_scratch(scratch)   # setup → work → teardown


pipeline()
```

```bash
python dags/stage-6-scale/s26/s26_examples.py
airflow dags test s26_setup_teardown_demo 2026-01-01
```

To *see* the guarantee, temporarily `raise RuntimeError("boom")` inside `summarize` and re-run: `summarize` goes red, but `drop_scratch` still runs and the dataset is gone. That is the behavior a plain arrow can never give you.

---

## 8. Build spec — your challenge (no solution)

**File:** `dags/stage-6-scale/s26/s26_assignment.py` · **dag_id:** `s26_assignment`

Build a pipeline that **always cleans up its scratch dataset**, proven against a task that fails.

**The problem:**

- A `@setup` task that provisions a scratch BigQuery dataset (via `BigQueryHook`) and returns its name.
- **Two** work tasks scoped between setup and teardown: one that does a real 0-byte aggregate against `bigquery-public-data.stackoverflow`, and one `flaky` task you can make fail on demand.
- A `@teardown` task that drops the dataset and is wired to the setup, so it runs **even when `flaky` fails**. Mark it `on_failure_fail_dagrun=True` — a leaked dataset should fail the run.

**Constraints:**

- Airflow 3 TaskFlow, `airflow.sdk` imports, connection `google_cloud_default`.
- No `SELECT *`; use `COUNT`/aggregates so queries bill 0 bytes (R17).
- Passes the integrity gates: `tags`, real `owner`, `retries >= 1`.

**Acceptance criteria:**

- `python dags/stage-6-scale/s26/s26_assignment.py` parses.
- With `flaky` succeeding, the run is green and the dataset is dropped.
- With `flaky` raising, the work task goes red **but the teardown still drops the dataset** (check BigQuery — no `s26_*` scratch dataset survives).
- `python -m pytest tests/ -v` stays green.

**One nudge (only if stuck):** the teardown must know its setup — either use the `@teardown` decorator and draw `setup >> work >> teardown`, or call `drop.as_teardown(setups=create)`. Without that link the teardown has no scope and won't behave as one.

---

## 9. Production tip — the cluster that billed all weekend

The bug that pages you Monday: a Friday-evening DAG did `create_cluster >> train_model >> delete_cluster` with plain arrows. `train_model` OOM'd and went red at 18:40. `delete_cluster` never ran, because its upstream failed — and the GPU cluster sat idle, fully billed, from Friday night to Monday morning. Nobody noticed until finance did. The habit that prevents it: **the task that deletes a paid resource is always a `teardown`, never a plain downstream.** A teardown runs on its setup's success regardless of what the work in between did, so the *failure* that leaks money is exactly the case it covers. And when the resource is expensive enough that a failed cleanup is itself an incident, set `on_failure_fail_dagrun=True` so the cleanup failure pages you at 02:00 instead of hiding until the invoice.

Sources:
[Setup and Teardown — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/howto/setup-and-teardown.html),
[Task SDK — airflow.sdk imports](https://airflow.apache.org/docs/task-sdk/stable/examples.html),
[Public Interface for Airflow 3.0+](https://airflow.apache.org/docs/apache-airflow/stable/public-airflow-interface.html),
[Use setup and teardown tasks — Astronomer](https://www.astronomer.io/docs/learn/airflow-setup-teardown)
