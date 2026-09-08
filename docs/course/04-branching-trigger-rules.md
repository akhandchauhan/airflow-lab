# Mission 04 · Give the Pipeline a Brain

**Branching & trigger rules** — teaching a DAG to make decisions.

> ## 📟 Cold open — 06:00, your phone buzzes
> You run the **Product Health** report for Stack Overflow. Every morning it counts
> the unanswered-question backlog and breaks it down by tag, so the team knows what's
> drowning. Last Tuesday the upstream load was late, the report ran on an **empty
> table**, and it cheerfully told the VP that Stack Overflow had **0 unanswered
> questions**. Slack lit up. Not great.
>
> **Today's mission:** give the pipeline a brain. If there's nothing to report, it
> should **stop itself**. When the backlog is huge, it should do a **deep tag
> breakdown**; when it's small, a **light summary**. And no matter what happens, it
> must **always post a status** so you're never guessing whether it ran.

Three tools do exactly that:

| Tool                  | The decision it makes            | Road-trip picture                          |
| --------------------- | -------------------------------- | ------------------------------------------ |
| `@task.branch`        | *which path?*                    | a **fork** — one road runs, the other's closed |
| `@task.short_circuit` | *continue at all?*               | a **"BRIDGE OUT"** barrier — stop           |
| `TriggerRule`         | *when may a task start?*         | the **rule at a junction** for when you can go |

The full pipeline you'll build:

```
count_unanswered ─(guard: 0? STOP)─▶ triage_by_size ─┬─▶ deep_triage ──┐
                                                      └─▶ light_triage ─┴─▶ publish ─▶ notify
                                                              (NONE_FAILED_MIN_ONE_SUCCESS)   (ALL_DONE)
```

---

## 1. `@task.branch` — pick a path

The branch is the fork. It returns the **`task_id`** (a string) of the path to run;
every other task wired directly below it is **skipped**.

```python
@task
def count_unanswered() -> int:
    return 6_000_000                # a stub for now; the real BigQuery count arrives in §6

@task.branch                        # ← THE MECHANIC
def triage_by_size(backlog: int) -> str:
    return "deep_triage" if backlog > 5_000_000 else "light_triage"   # returns a TASK_ID

@task(task_id="deep_triage")
def run_deep_triage():
    print("break the backlog down by tag")

@task(task_id="light_triage")
def run_light_triage():
    print("just log the total")

path = triage_by_size(count_unanswered())
path >> [run_deep_triage(), run_light_triage()]   # the returned task_id runs; the other is skipped
```

- The branch returns a **`task_id` string**, not the function object.
- Return a **list** of task_ids to run several paths at once.
- The branch and its options must be **directly wired** (`path >> [a, b]`).

> **🎯 Challenge — reuse Mission 01 (XCom hand-off).** Right now `count_unanswered`
> returns a stub. That's already the return→argument hand-off from Mission 01 —
> prove you get it: add a second upstream task that returns a *threshold*, and make
> `triage_by_size` take **both** the backlog and the threshold as arguments.

---

## 2. `@task.short_circuit` — stop early

The guard. It returns **True** (keep going) or **False** (skip everything below).
This is what stops the empty-table embarrassment from the cold open.

```python
@task.short_circuit                 # ← THE MECHANIC
def has_backlog(backlog: int) -> bool:
    return backlog > 0             # 0 unanswered -> False -> skip the whole run

@task
def build_report():
    print("building the product-health report")

has_backlog(count_unanswered()) >> build_report()
```

Use it as a **guard in front of expensive work**: "only run if there's actually
something to report." Branch vs short-circuit: **branch chooses between paths;
short-circuit decides whether to continue at all.**

> **🎯 Challenge — reuse Mission 03 (TaskGroups).** Put `build_report` and a
> follow-up `write_summary` inside a `@task_group`, and wire the guard **before** the
> group. Confirm that when `has_backlog` returns False, the **whole group** goes
> skipped in one shot — not task by task.

---

## 3. `TriggerRule` — when is a task allowed to run?

By default a task runs only when **all** its upstream tasks **succeeded** (the
`all_success` rule). The `notify` task must run *even when things fail* — so you
change its rule:

```python
from airflow.sdk import TriggerRule

@task
def build_report():
    print("building report")

@task(trigger_rule=TriggerRule.ALL_DONE)     # ← THE MECHANIC: change WHEN a task may run
def notify():
    print("status sent (ran no matter what happened upstream)")

build_report() >> notify()
```

With `ALL_DONE`, `notify` runs whatever happens to `build_report`. On the default
`ALL_SUCCESS`, a failed or skipped upstream would skip `notify` too — and you'd get
no status at all.

The trigger rules you'll actually use:

| Trigger rule                  | Task runs when…                                    | Use it for                                |
| ----------------------------- | -------------------------------------------------- | ----------------------------------------- |
| `ALL_SUCCESS` (default)       | every upstream succeeded                           | normal flow                               |
| `NONE_FAILED_MIN_ONE_SUCCESS` | no upstream failed **and** ≥1 succeeded (skips OK) | a **join after a branch**                 |
| `ALL_DONE`                    | every upstream finished (success, fail, or skip)   | **status / cleanup** that must always run |
| `ONE_SUCCESS`                 | any one upstream succeeded                         | fan-in where any success is enough        |
| `ALL_FAILED`                  | every upstream failed                              | run only on total failure                 |

> **🎯 Challenge — reuse Mission 02 (parallel wiring).** Using the parallel style
> from Mission 02 (`[check_a, check_b] >> gate`), build two parallel data-quality
> checks where **one fails on purpose**, both feeding a `gate` task. Which trigger
> rule lets `gate` run because at least one check passed — `ONE_SUCCESS` or
> `ALL_DONE`? Explain the difference.

---

## 4. The trap that pages you at 2am: skips flow downstream

When the branch **skips** a task, that "skipped" status **passes down**. So the
`publish` task below the two triage paths has one skipped parent every run — and on
the default `all_success`, **`publish` gets skipped too**, even though the other
path succeeded. Your report silently never publishes, and the DAG still shows green.

```
triage_by_size ──▶ deep_triage ────┐
              └──▶ light_triage ────┴──▶ publish   # one parent is ALWAYS skipped
```

**Fix:** give the join `trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS` —
"run as long as nothing failed and at least one parent actually ran." Whenever a
task sits below a branch, set its trigger rule **on purpose**. This is the #1
branching bug in production.

> **🎯 Challenge — reuse P1 (BigQuery).** Swap the stub `count_unanswered` for a
> **real** count: `BigQueryHook.get_first("SELECT COUNT(*) FROM
> \`bigquery-public-data.stackoverflow.posts_questions\` WHERE answer_count = 0")`.
> Now the whole brain runs on live Stack Overflow data. This is your on-ramp to §6.

---

## 5. Complete runnable reference DAG (BigQuery · Stack Overflow)

The whole brain, wired on the real `posts_questions` table via the P1 connection.
Every query is cost-capped.

```python
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task, TriggerRule
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

QUESTIONS = "bigquery-public-data.stackoverflow.posts_questions"
CAP = "2000000000"          # 2 GB max bytes billed per query — safety cap
BIG_BACKLOG = 5_000_000     # above this many unanswered -> the deep path


@dag(
    dag_id="m04_product_health_demo",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["mission-04", "branching", "stackoverflow"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():

    @task
    def count_unanswered() -> int:
        hook = BigQueryHook(gcp_conn_id="google_cloud_default", location="US", use_legacy_sql=False)
        row = hook.get_first(f"SELECT COUNT(*) FROM `{QUESTIONS}` WHERE answer_count = 0")
        return int(row[0])

    @task.short_circuit
    def has_backlog(backlog: int) -> bool:
        print(f"unanswered questions = {backlog:,}")
        return backlog > 0                       # nothing to triage -> skip the run

    @task.branch
    def triage_by_size(backlog: int) -> str:
        return "deep_triage" if backlog > BIG_BACKLOG else "light_triage"   # returns a TASK_ID

    # heavy path: which tags are drowning? (unnests the pipe-delimited tags column)
    deep_path = BigQueryInsertJobOperator(
        task_id="deep_triage",
        gcp_conn_id="google_cloud_default",
        location="US",
        configuration={"query": {
            "query": f"""
                SELECT tag, COUNT(*) AS unanswered
                FROM `{QUESTIONS}`, UNNEST(SPLIT(tags, '|')) AS tag
                WHERE answer_count = 0
                GROUP BY tag ORDER BY unanswered DESC LIMIT 10
            """,
            "useLegacySql": False, "maximumBytesBilled": CAP,
        }},
    )

    # light path: just the headline number
    light_path = BigQueryInsertJobOperator(
        task_id="light_triage",
        gcp_conn_id="google_cloud_default",
        location="US",
        configuration={"query": {
            "query": f"SELECT COUNT(*) AS unanswered FROM `{QUESTIONS}` WHERE answer_count = 0",
            "useLegacySql": False, "maximumBytesBilled": CAP,
        }},
    )

    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def publish() -> None:               # join after the branch — survives the skipped path
        print("product-health summary published")

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def notify() -> None:                # always runs — success, failure, or skip
        print("daily run finished — status sent")

    backlog = count_unanswered()
    guard = has_backlog(backlog)
    decision = triage_by_size(backlog)

    guard >> decision >> [deep_path, light_path] >> publish() >> notify()


pipeline()
```

Run it (needs the P1 BigQuery connection):

```bash
python dags/task-4/m04_product_health_demo.py
airflow dags test m04_product_health_demo 2026-01-01
```

Stack Overflow's unanswered backlog is in the millions (> `BIG_BACKLOG`), so
`deep_triage` runs and `light_triage` goes **grey** (skipped); `publish` still runs
thanks to its trigger rule; `notify` runs last. Check **BigQuery Job history** —
`count_unanswered` scans one column, `deep_triage` stays under the 2 GB cap. Lower
`BIG_BACKLOG` to flip the branch; set the guard's threshold impossibly high to watch
the whole run short-circuit while `notify` *still* fires.

---

## 6. Your mission build (no solution)

**File:** `dags/task-4/m04_product_health.py` · **dag_id:** `m04_product_health`

Ship the real Product Health brain. It reads a live metric from
`bigquery-public-data.stackoverflow`, guards against an empty run, branches on the
metric, and always posts a status.

**The mission:**

- A first task reads a **real metric** from Stack Overflow (unanswered backlog, new
  questions in a period, a tag's volume — your call).
- A **guard** (`@task.short_circuit`) stops the whole run if the metric is 0.
- A **branch** (`@task.branch`) picks one of two BigQuery query paths based on the
  metric (a heavy breakdown vs a light summary); the other path is skipped.
- A **join** task publishes the result and must survive the skipped branch.
- A **status** task runs **no matter what** (success, failure, or short-circuit).

**Rules of engagement:**

- BigQuery work via the Google provider + `google_cloud_default`; **every query
  cost-capped** (`maximumBytesBilled`), no `SELECT *`.
- Correct `TriggerRule` on the join and the status task.
- Names clearly distinct (R12): a `@task.branch` returns the **noun `task_id`**, the
  functions are verbs (`run_deep_triage`).
- Passes the integrity gates: `tags`, real `owner`, `retries >= 1`.

**Mission complete when:**

- `python dags/task-4/m04_product_health.py` parses (prints nothing).
- `airflow dags test m04_product_health 2026-01-01` runs green.
- Graph: one path runs, the other skipped, the join runs, status runs.
- Flip the guard and the branch threshold and watch the outcome change.
- **BigQuery Job history** shows every query within the cap.
- `python -m pytest tests/ -v` stays green.

---

## 7. War story — the production tip that would've saved Tuesday

- **A short-circuit guard in front of every expensive stage is the cheapest
  insurance you'll ever write.** The Tuesday incident — reporting on an empty table —
  is a `has_backlog`-style guard away from never happening. "Is there new data? Is
  this the right day? Does the partition exist?" One cheap check saves a wrong number
  in front of a VP *and* a wasted warehouse bill.
- **A post-branch task on the default trigger rule is a silent time bomb.** It skips
  because one branch skipped, the DAG goes green, and the important step quietly never
  ran — you find out days later when someone asks where the report went. Set
  `NONE_FAILED_MIN_ONE_SUCCESS` on joins and `ALL_DONE` on status/alerting **on
  purpose**, with a comment saying why.

---

## 8. Verify + commit

```bash
python dags/task-4/m04_product_health.py
airflow dags test m04_product_health 2026-01-01
python -m pytest tests/ -v
git add -A && git commit -m "mission 04: product-health brain (branching + trigger rules)" && git push
```

Mission complete when the graph shows one path taken, one skipped, the join running,
and status always running. Then update the scoreboard in `README.md`.

Sources:
[Branching — Astronomer](https://www.astronomer.io/docs/learn/airflow-branch-operator),
[Stack Overflow dataset — BigQuery](https://console.cloud.google.com/marketplace/product/stack-exchange/stack-overflow)
