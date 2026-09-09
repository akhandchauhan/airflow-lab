# Session 05 · When a Task Falls Over

**Retries** — teaching a task to try again before it wakes you up.

> ## 📟 Cold open
> 03:14. A task hit a one-second network blip and failed. Nothing was actually
> broken — a retry 10 seconds later would have worked. But there was no retry, so
> the run went red and the pager went off. You woke up to fix something that had
> already fixed itself.
>
> **Today:** two tiny settings that make Airflow try again on its own.

No BigQuery this session. Just plain tasks — you'll *watch* one fail and recover.

---

## 1. `retries` — how many times to try again

Put `retries` on a task. If it raises, Airflow runs it again instead of failing.

```python
@task(retries=2)          # ← THE MECHANIC: on failure, try up to 2 more times
def call_flaky_api() -> str:
    import random
    if random.random() < 0.7:        # fails ~70% of the time
        raise RuntimeError("network blip")
    return "ok"
```

- `retries=2` means **3 attempts total** (the first try + 2 retries).
- Airflow only retries on a **failure** (an exception). A task that succeeds never retries.
- No `retries` at all = **0 retries** = one strike and it's red.

---

## 2. `retry_delay` — how long to wait between tries

Retrying instantly is usually pointless — the blip needs a moment to pass. Add a wait.

```python
from datetime import timedelta

@task(retries=2, retry_delay=timedelta(seconds=10))    # ← wait 10s before each retry
def call_flaky_api() -> str:
    ...
```

- `retry_delay` is a `timedelta` — seconds, minutes, whatever you need.
- Common real values: 30s–5min for network calls; longer for a slow external system.

That's the whole idea. Two settings.

---

## 3. Complete runnable reference DAG (plain — no BigQuery)

A tiny DAG with one flaky task. Run it a few times and watch it retry.

```python
# dags/s5/flaky_task_demo.py
from __future__ import annotations

from datetime import timedelta
import random

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s5_flaky_task_demo",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-5", "retries"],
    default_args={"owner": "akhand", "retries": 2, "retry_delay": timedelta(seconds=5)},
)
def pipeline():

    @task
    def call_flaky_api() -> str:
        if random.random() < 0.7:          # fails ~70% of the time
            raise RuntimeError("network blip — will retry")
        return "ok"

    @task
    def use_result(status: str) -> None:
        print(f"downstream ran with status={status}")

    use_result(call_flaky_api())


pipeline()
```

Setting `retries` in `default_args` applies it to **every task** in the DAG, so you
don't repeat it. Run it:

```bash
python dags/s5/flaky_task_demo.py
airflow dags test s5_flaky_task_demo 2026-01-01
```

In the logs you'll see `call_flaky_api` raise, then a line like *"Task will retry…"*,
a pause, and another attempt — until it succeeds. That recovery is the whole point.

---

## 4. Your build (no solution)

**File:** `dags/s5/flaky_task.py` · **dag_id:** `s5_flaky_task`

Build a tiny 2-task plain DAG that survives a flaky task.

**The job:**

- Task 1 (`fetch`) raises an exception **about half the time** (use `random`).
- Give it **`retries=3`** and a **`retry_delay`** of 10 seconds.
- Task 2 (`report`) runs after it and prints a done message.

**Done when:**

- `python dags/s5/flaky_task.py` parses (prints nothing).
- `airflow dags test s5_flaky_task 2026-01-01` runs green — the logs show at least
  one retry before success.
- `python -m pytest tests/ -v` stays green.

No BigQuery, no connection, no cost cap — just a plain DAG.

---

## 5. Production tip — retries are for *transient* failures only

- **Retry a network call, never a bug.** Retries fix flaky things: a timeout, a rate
  limit, a service restarting. They do **nothing** for a real bug (a typo, bad SQL) —
  it just fails 3 times instead of once and delays the alert. If a failure isn't
  random, don't paper over it with retries.
- **Add a small `retry_delay`.** Instant retries often hit the same blip mid-blip.
  Even 10–30 seconds dramatically raises the odds the second try succeeds.

---

## 6. Verify + commit

```bash
mkdir -p dags/s5
python dags/s5/flaky_task.py
airflow dags test s5_flaky_task 2026-01-01
python -m pytest tests/ -v
git add -A && git commit -m "session 05: retries" && git push
```

Done when the DAG recovers from the flaky task on its own. Then tick the byte in
`README.md`.

Sources:
[Tasks / retries — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/tasks.html),
[Best practices — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)
