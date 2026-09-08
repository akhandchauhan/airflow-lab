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

One project spine: `bigquery-public-data.stackoverflow`. These are the columns this
session (and the ones after) lean on — join keys in **bold**.

**`posts_questions`** — one row per question

| column                | type      | meaning                                             |
| --------------------- | --------- | --------------------------------------------------- |
| **`id`**              | INT64     | question id (answers point here via `parent_id`)    |
| `creation_date`       | TIMESTAMP | when it was asked                                   |
| `answer_count`        | INT64     | number of answers — **`0` = unanswered**            |
| `accepted_answer_id`  | INT64     | null if nothing was accepted                        |
| `score`               | INT64     | net votes                                           |
| `view_count`          | INT64     | views                                               |
| `tags`                | STRING    | **pipe-delimited**, e.g. `"python\|pandas\|bigquery"` |
| `owner_user_id`       | INT64     | FK → `users.id`                                     |
| `title`               | STRING    | question title                                      |

**`posts_answers`** — one row per answer (no `tags` / `title`)

| column          | type      | meaning                          |
| --------------- | --------- | -------------------------------- |
| **`id`**        | INT64     | answer id                        |
| **`parent_id`** | INT64     | FK → `posts_questions.id`        |
| `creation_date` | TIMESTAMP | when it was posted               |
| `score`         | INT64     | net votes                        |
| `owner_user_id` | INT64     | FK → `users.id`                  |

**`tags`** — one row per tag (tiny dimension table, ~60k rows)

| column     | type  | meaning                                       |
| ---------- | ----- | --------------------------------------------- |
| `tag_name` | STRING | e.g. `python` (one tag per row — no pipes)   |
| `count`    | INT64 | how many questions carry this tag             |

**`users`** — one row per user

| column          | type      | meaning     |
| --------------- | --------- | ----------- |
| **`id`**        | INT64     | user id     |
| `display_name`  | STRING    | name        |
| `reputation`    | INT64     | rep score   |
| `creation_date` | TIMESTAMP | signup date |

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

## 6. Assignment — "The Python Backlog Brain" (no solution)

**File:** `dags/s4/product_health.py` · **dag_id:** `s4_product_health`

### The scenario

The VP just narrowed the ask: *"Stop giving me the whole site. I only care about
the **`python` tag** — how big is its unanswered backlog, and when it's bad, which
sub-topics are drowning?"* Build the brain that answers exactly that, every
morning, and never lies when the data is late.

### The pipeline (5 tasks, this exact shape)

```
measure_backlog ─(guard: 0? STOP)─▶ triage ─┬─▶ deep_breakdown ──┐
                                             └─▶ light_summary ───┴─▶ publish ─▶ notify
```

| # | task_id            | type                  | what it must do                                                                                          |
| - | ------------------ | --------------------- | -------------------------------------------------------------------------------------------------------- |
| 1 | `measure_backlog`  | `@task` + BigQueryHook | `COUNT(*)` of `posts_questions` where `answer_count = 0` **and** `tags LIKE '%\|python\|%'`; return the int |
| 2 | `has_backlog`      | `@task.short_circuit` | stop the whole run if the count is `0`                                                                    |
| 3 | `triage`           | `@task.branch`        | `> 50_000` → `"deep_breakdown"`, else `"light_summary"` (return the **task_id string**)                  |
| 4a | `deep_breakdown`  | `BigQueryInsertJobOperator` | top-10 **co-tags** of unanswered python questions (`UNNEST(SPLIT(tags,'\|'))`, exclude `python` itself) |
| 4b | `light_summary`   | `BigQueryInsertJobOperator` | single-row headline: the unanswered count                                                          |
| 5a | `publish`         | `@task`               | logs the result — **must survive the skipped branch path**                                               |
| 5b | `notify`          | `@task`               | logs "run finished" — **runs on success, failure, OR short-circuit**                                     |

### Rules of engagement

- All BigQuery via the Google provider + `google_cloud_default`. **Every operator
  query cost-capped** with `maximumBytesBilled` (use `"2000000000"` = 2 GB); no
  `SELECT *`. The `@task` count read may use `BigQueryHook.get_first` (aggregate,
  uncapped is fine).
- **Pick the two trigger rules yourself** — one for `publish` (join below a
  branch), one for `notify` (always-run). Getting these right is the whole point of
  §3–§4.
- Names clearly distinct (R12): `task_id` = noun; the branch returns the **noun
  task_id string**, never a function name; functions are verbs (`run_deep_breakdown`).
- Passes the integrity gates: non-empty `tags`, real `owner`, `retries >= 1`.

### Acceptance criteria (this is the grade)

| Check | Points |
| ----- | ------ |
| `python dags/s4/product_health.py` parses; `python -m pytest tests/ -v` green | 4 |
| `airflow dags test s4_product_health 2026-01-01` runs green end-to-end | 4 |
| Graph proves it: one branch path runs, the other is **skipped (grey)**, `publish` **still runs** | 5 |
| `notify` runs even when you force `measure_backlog` to return `0` (short-circuit) — prove it | 4 |
| **BigQuery Job history** shows every operator query **within the 2 GB cap** | 3 |

**20 / 20** = the build byte. Tick **4.4** on the scoreboard.

### Prove it works (do all three)

1. Real run: python's unanswered backlog is well over 50k → `deep_breakdown` runs,
   `light_summary` goes grey, `publish` + `notify` run.
2. Flip the branch: temporarily hardcode `measure_backlog` to return `100` (below
   the 50k threshold) → `light_summary` runs and `deep_breakdown` goes grey.
3. Short-circuit: hardcode `measure_backlog` to return `0` → everything skips
   **except `notify`**, which still fires.

### Stretch (optional, no extra points — just sharper)

- Make the focus tag a module constant `FOCUS_TAG = "python"` so the DAG retargets
  in one line.
- Add a third branch path `escalate` for a `> 200_000` "on fire" backlog.

### Nudges (only if stuck)

- The `LIKE '%|python|%'` trick works because `tags` is stored pipe-delimited *and*
  the provider wraps the whole string in pipes — but safest is to `UNNEST` and match
  `tag = 'python'`.
- `publish` has one skipped parent every run — that's the §4 trap; its trigger rule
  is `NONE_FAILED_MIN_ONE_SUCCESS`, not the default.
- `notify` needs `ALL_DONE`.

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
