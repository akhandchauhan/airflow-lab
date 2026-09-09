# Session 04 · Give the Pipeline a Brain

**Branching & trigger rules** — teaching a DAG to make decisions.

> ## 📟 Cold open — 06:00, your phone buzzes
> You run the **Product Health** report for Stack Overflow. Every morning it counts
> the unanswered-question backlog and breaks it down by tag, so the team knows what's
> drowning. Last Tuesday the upstream load was late, the report ran on an **empty
> table**, and it cheerfully told the VP that Stack Overflow had **0 unanswered
> questions**. Slack lit up. Not great.
>
> **Today's job:** give the pipeline a brain. If there's nothing to report, it should
> **stop itself**. When the backlog is huge, do a **deep tag breakdown**; when small,
> a **light summary**. And no matter what happens, **always post a status** so you're
> never guessing whether it ran.

---

## 📊 The tables you're working with

One project spine: `bigquery-public-data.stackoverflow`. Sample rows below
(illustrative values, real columns) so you can see the **grain** and shape of each
table — join keys in **bold**.

**`posts_questions`** · **grain: one row = one question**

| **id**   | creation_date       | answer_count | score | tags                        | view_count |
| -------- | ------------------- | ------------ | ----- | --------------------------- | ---------- |
| 231767   | 2008-10-23 22:21:00 | 42           | 11200 | `python\|iterator\|generator` | 3921544    |
| 11227809 | 2012-06-27 13:51:00 | 25           | 3120  | `python\|list\|dictionary`    | 1450233    |
| 4700614  | 2011-01-16 04:07:00 | 18           | 2600  | `python\|string\|split`       | 987410     |
| 79911002 | 2026-08-02 17:44:00 | **0**        | 1     | `python\|airflow\|scheduling` | 47         |
| 79923410 | 2026-08-14 09:02:00 | **0**        | 0     | `python\|pandas\|bigquery`    | 12         |

**`posts_answers`** · **grain: one row = one answer** (no `tags` / `title`)

| **id**   | **parent_id** | creation_date       | score | owner_user_id |
| -------- | ------------- | ------------------- | ----- | ------------- |
| 231855   | 231767        | 2008-10-23 22:34:00 | 6412  | 28169         |
| 11227902 | 11227809      | 2012-06-27 13:58:00 | 1890  | 190597        |
| 4700620  | 4700614       | 2011-01-16 04:15:00 | 3104  | 47214         |
| 231903   | 231767        | 2008-10-23 22:41:00 | 512   | 9951          |
| 4700655  | 4700614       | 2011-01-16 04:29:00 | 88    | 63051         |

**`tags`** · **grain: one row = one tag** (tiny dimension table, ~60k rows)

| id  | tag_name     | count   |
| --- | ------------ | ------- |
| 16  | `javascript` | 2512000 |
| 17  | `python`     | 2148000 |
| 3   | `java`       | 1901000 |
| 9   | `c#`         | 1583000 |
| 820 | `pandas`     | 312000  |

**`users`** · **grain: one row = one user**

| **id** | display_name  | reputation | creation_date       | location        |
| ------ | ------------- | ---------- | ------------------- | --------------- |
| 22656  | Jon Skeet     | 1402000    | 2008-09-26 12:01:00 | Reading, UK     |
| 190597 | user190597    | 84200      | 2009-10-15 08:20:00 | Berlin, Germany |
| 28169  | Greg Hewgill   | 621000     | 2008-08-27 03:10:00 | New Zealand     |
| 47214  | Martijn Pieters | 998000    | 2010-11-02 19:44:00 | London, UK      |
| 9951   | user9951      | 15300      | 2008-09-16 11:05:00 | (null)          |

> `tags` on `posts_questions` is pipe-delimited — explode it with
> `UNNEST(SPLIT(tags, '|'))`. Cost note: `COUNT(*)` scans **0 bytes**; filtering or
> counting one column scans only **that** column, never the whole row.

---

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
    return 6_000_000                # a stub for now; the real BigQuery count arrives in §5

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
- The branch and its options must be **directly wired** (`path >> [a, b]`).

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

Use it as a **guard in front of expensive work**. Branch vs short-circuit: **branch
chooses between paths; short-circuit decides whether to continue at all.**

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

---

## 5. Complete runnable reference DAG (BigQuery · Stack Overflow)

The whole brain, wired on the real `posts_questions` table via the P1 connection.
Every query is cost-capped.

```python
# dags/s4/product_health_demo.py
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task, TriggerRule
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

QUESTIONS = "bigquery-public-data.stackoverflow.posts_questions"
CAP = "2000000000"          # 2 GB max bytes billed per query — safety cap
BIG_BACKLOG = 5_000_000     # above this many unanswered -> the deep path


@dag(
    dag_id="s4_product_health_demo",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-4", "branching", "stackoverflow"],
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
python dags/s4/product_health_demo.py
airflow dags test s4_product_health_demo 2026-01-01
```

Stack Overflow's unanswered backlog is in the millions (> `BIG_BACKLOG`), so
`deep_triage` runs and `light_triage` goes **grey** (skipped); `publish` still runs
thanks to its trigger rule; `notify` runs last. Check **BigQuery Job history** —
`count_unanswered` scans one column, `deep_triage` stays under the 2 GB cap. Lower
`BIG_BACKLOG` to flip the branch; set the guard's threshold impossibly high to watch
the whole run short-circuit while `notify` *still* fires.

---

## 6. Assignment — "The Answer-Speed Watchdog" (no solution)

**File:** `dags/s4/product_health.py` · **dag_id:** `s4_product_health`

### The scenario

New VP, new obsession: *"I don't care how big the pile is — I care how **fast** we
answer. Take last month's questions, measure the median hours to the **first**
answer, and if we blew past our **24-hour SLA**, tell me which tags are slowest. If
a month has no data yet, don't send me a garbage number."*

This is a **latency** question, not a size one — so the metric is a `JOIN` between
`posts_questions` and `posts_answers` with `TIMESTAMP_DIFF`, and the branch fires on
an **SLA breach**, not a row count. Same S4 shape as the reference; completely
different brain.

### The pipeline (5 tasks)

```
measure_answer_speed ─(guard: no data? STOP)─▶ route_by_sla ─┬─▶ breach_analysis ──┐
                                                             └─▶ healthy_summary ──┴─▶ publish ─▶ notify
```

| # | task_id                | type                        | what it must do                                                                                                                                          |
| - | ---------------------- | --------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1 | `measure_answer_speed` | `@task` + BigQueryHook      | for questions created in `WINDOW` that got ≥1 answer, return the **median hours** to the first answer (`APPROX_QUANTILES`); return `None` if the window is empty |
| 2 | `has_data`             | `@task.short_circuit`       | stop the whole run if the median is `None` (empty window — the "late upstream" trap, time-based)                                                          |
| 3 | `route_by_sla`         | `@task.branch`              | median `> 24.0` → `"breach_analysis"`, else `"healthy_summary"` (return the **task_id string**)                                                           |
| 4a | `breach_analysis`     | `BigQueryInsertJobOperator` | top-10 **slowest tags** in `WINDOW`: median hours-to-first-answer per tag (`UNNEST` the tags), worst first                                                |
| 4b | `healthy_summary`     | `BigQueryInsertJobOperator` | single-row headline: the median hours + how many questions it covered                                                                                     |
| 5a | `publish`             | `@task`                     | logs the result — **must survive the skipped branch path**                                                                                                |
| 5b | `notify`              | `@task`                     | logs "run finished" — **runs on success, failure, OR short-circuit**                                                                                      |

Use a fixed `WINDOW` that has data — the public dataset ends **~Sept 2022**, so
`WINDOW_START = "2022-08-01"`, `WINDOW_END = "2022-09-01"` works.

### Rules of engagement

- All BigQuery via the Google provider + `google_cloud_default`. **Every operator
  query cost-capped** with `maximumBytesBilled`; no `SELECT *`. The join reads whole
  columns (`creation_date`, `parent_id`, `tags`) — **dry-run first** (`bq query
  --dry_run`) and set the cap just above what it reports (~1–2 GB for the join, more
  for the tag breakdown since `tags` is a fat column). The `@task` scalar read may
  use `BigQueryHook.get_first`.
- **Pick the two trigger rules yourself** — one for `publish` (join below a branch),
  one for `notify` (always-run). That choice is the whole point of §3–§4.
- Names clearly distinct (R12): `task_id` = noun; the branch returns the **noun
  task_id string**, never a function name; functions are verbs (`run_breach_analysis`).
- Passes the integrity gates: non-empty `tags`, real `owner`, `retries >= 1`.

### Acceptance criteria (this is the grade)

| Check | Points |
| ----- | ------ |
| `python dags/s4/product_health.py` parses; `python -m pytest tests/ -v` green | 4 |
| `airflow dags test s4_product_health 2026-01-01` runs green end-to-end | 4 |
| Graph proves it: one branch path runs, the other is **skipped (grey)**, `publish` **still runs** | 5 |
| `notify` runs even when you force `measure_answer_speed` to return `None` (short-circuit) — prove it | 4 |
| **BigQuery Job history** shows every operator query **within its cap** | 3 |

**20 / 20** = the build byte. Tick **4.4** on the scoreboard.

### Prove it works (do all three)

1. Real run on Aug 2022 → note which path fired (breach vs healthy) from the logs;
   `publish` and `notify` both run.
2. Flip the branch: temporarily hardcode `measure_answer_speed` to return `2.0`
   (well under the 24h SLA) → `healthy_summary` runs, `breach_analysis` goes grey.
   Then `99.0` → the other way.
3. Short-circuit: hardcode `measure_answer_speed` to return `None` → everything
   skips **except `notify`**, which still fires.

### Stretch (optional, no extra points — just sharper)

- Make `SLA_HOURS = 24.0` and the `WINDOW_*` dates module constants so the DAG
  retargets in one line.
- Add a third branch path `page_oncall` for a `> 72.0` "SLA on fire" median.

### Nudges (only if stuck)

- Time-to-first-answer per question: `TIMESTAMP_DIFF(MIN(a.creation_date),
  q.creation_date, HOUR)` after `JOIN posts_answers a ON a.parent_id = q.id`, grouped
  per question. Median over that with `APPROX_QUANTILES(h, 2)[OFFSET(1)]`.
- An empty window makes the median query return one row of `NULL` — read it as
  `None` in Python and short-circuit on it.
- `publish` has one skipped parent every run — that's the §4 trap; its trigger rule
  is `NONE_FAILED_MIN_ONE_SUCCESS`, not the default. `notify` needs `ALL_DONE`.

---

## 7. War story — the production tip that would've saved Tuesday

- **A short-circuit guard in front of every expensive stage is the cheapest
  insurance you'll ever write.** The Tuesday incident — reporting on an empty table —
  is a `has_backlog`-style guard away from never happening. One cheap check saves a
  wrong number in front of a VP *and* a wasted warehouse bill.
- **A post-branch task on the default trigger rule is a silent time bomb.** It skips
  because one branch skipped, the DAG goes green, and the important step quietly never
  ran — you find out days later when someone asks where the report went. Set
  `NONE_FAILED_MIN_ONE_SUCCESS` on joins and `ALL_DONE` on status/alerting **on
  purpose**, with a comment saying why.

---

## 8. Verify + commit

```bash
python dags/s4/product_health.py
airflow dags test s4_product_health 2026-01-01
python -m pytest tests/ -v
git add -A && git commit -m "session 04: product-health brain (branching + trigger rules)" && git push
```

Done when the graph shows one path taken, one skipped, the join running, and status
always running. Then update the scoreboard in `README.md`.

Sources:
[Branching — Astronomer](https://www.astronomer.io/docs/learn/airflow-branch-operator),
[Stack Overflow dataset — BigQuery](https://console.cloud.google.com/marketplace/product/stack-exchange/stack-overflow)
