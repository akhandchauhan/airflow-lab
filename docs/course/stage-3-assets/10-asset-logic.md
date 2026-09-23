# Session 10 · Asset logic (conditions, nesting, and runtime-resolved assets)

**Goal:** Session 09 gave you one bell, one listener. Real pipelines are messier: a mart needs `questions` **and** `answers`, or it can settle for `questions` **or** a nightly `tags` reload; and sometimes you don't even know *which* asset a run produced until the run happens (today's export path has today's date in it). This session is the advanced wiring on top of §12: composing assets with `&` / `|` into arbitrarily nested boolean conditions you drop straight into `schedule=`, and `AssetAlias` — a stable doorbell whose *wiring* is decided each run. No new concept, just harder shapes of the same doorbell. Plain DAGs, no BigQuery; asset names stay on the Stack Overflow spine (`questions`, `answers`, `tags`). _(Stage 3 — react to data, not the clock.)_

---

## 1. Where §12 stops and §13 starts

Session 09 taught the atom: one producer task lists an asset in `outlets`, its success writes an asset event, and any DAG with that asset in `schedule=` runs. It also *introduced* `&` and `|` and named `AssetAlias` on the "advanced ladder" — but only as a glance. This session is the part you actually reach for in production: how to **compose** many bells into one trigger, how conditions **reset**, when to pick AND vs OR vs a time floor, and how to depend on an asset whose identity isn't known until the run is underway.

The doorbell picture from §12 still holds — do not throw it away. Everything here is: **more bells, and smarter wiring between the bells and the chime.** If "producer rings, consumer listens" isn't reflex yet, re-read §12 before this.

| §12 (the atom)                     | §13 (the logic)                                                       |
| ---------------------------------- | -------------------------------------------------------------------- |
| one asset in `schedule=[a]`        | a **boolean expression** in `schedule=(a & b) \| c`                  |
| you knew the URI at parse time     | `AssetAlias` — the real URI is decided **at run time**               |
| "run when it rings"                | "run when **this combination** of rings has happened since last run" |

---

## 2. The mechanism under `&` and `|` — what the operators actually build

Here is the part everyone skips and then gets confused by: `questions & answers` is **not** special scheduler syntax. It is ordinary Python operator overloading. The `Asset` class defines `__and__` and `__or__`, so `&` and `|` **build objects** at parse time — a whole expression tree — long before the scheduler ever looks at it.

- `questions & answers` evaluates to an **`AssetAll`** object (AND).
- `questions | answers` evaluates to an **`AssetAny`** object (OR).
- Both are importable, so you can build the same tree explicitly: `AssetAll(questions, answers)` is identical to `questions & answers`.

```python
from airflow.sdk import Asset, AssetAll, AssetAny   # ← both live in airflow.sdk

questions = Asset("questions")
answers = Asset("answers")

cond = questions & answers          # ← THE MECHANIC: this IS AssetAll(questions, answers)
same = AssetAll(questions, answers) # identical object, written the long way
```

Why this matters: because it's a plain object, you can **assign it to a variable, nest it, and pass it around** like any value. The scheduler receives a finished tree and evaluates it as events land. You are not writing waiting logic — you are declaring a boolean expression over "has this asset had an event since my last run."

**Analogy — the alarm panel.** Session 09's single doorbell now becomes an **alarm panel** wired to several door sensors. `&` is "arm the chime only when *all* these zones report"; `|` is "chime if *any* zone reports." The panel (the scheduler) does the boolean logic; each sensor (asset) just reports its own event. You wire the panel once; it decides when the whole thing fires.

---

## 3. Nesting — `(a & b) | c` and why grouping is load-bearing

Because the operators return objects, they nest like arithmetic, and — critically — they obey **Python's** precedence, not any Airflow rule. `&` binds tighter than `|` (same as `and`/`or` intuition), but **do not lean on that** — parenthesize everything, because a misplaced group silently changes *when your DAG runs*, and nothing errors.

```python
# "run when BOTH questions and answers are fresh, OR when tags reloaded on its own"
schedule = (questions & answers) | tags        # ← group the AND, then OR the fallback

# WITHOUT the parens this parses differently and quietly reweights the whole trigger:
schedule = questions & answers | tags          # = questions & (answers | tags)  ← NOT what you meant
```

Read the two right-hand sides aloud and you can hear the bug: the first fires when *(questions and answers)* land or when *tags* lands; the second fires when *questions* lands **and** *(answers or tags)* has — so a lone `tags` event never triggers it, and a `questions`+`tags` pair does. Same characters, different pipeline. **Rule: one pair of parens per logical group, always.**

Nesting has no depth limit — `(a & b) | (c & d)`, `a | (b & (c | d))` are all fine — but depth is a smell. If a schedule needs three levels, the honest question is whether you actually have **two** consumers hiding in one. Split them: a tired reader (you, at 2am) should be able to say what triggers a DAG in one breath.

---

## 4. The reset rule — the single most misread behavior of conditions

§12 mentioned this in passing; it is worth its own byte because it is where people file bugs that aren't bugs. **A multi-asset condition tracks events since the DAG's last run, and once it fires it forgets everything and starts the next round from empty.**

Walk the `questions & answers` case concretely:

1. `questions` fires. Condition is half-satisfied. DAG does **not** run.
2. `questions` fires **again** (chatty producer). Still just "questions seen." Does **not** run. The second event does not stack.
3. `answers` fires. Now both sides are satisfied → DAG runs **once**.
4. Immediately after firing, the set **resets**. `questions` and `answers` are both "unseen" again; the DAG waits for a fresh full round.

Two consequences that bite:

- **A flood on one side does not cause a flood of runs.** Ten `questions` events plus one `answers` event = **one** run, not ten. This is a feature: your two-input mart can't be stampeded by a chatty upstream.
- **After a run, old events don't carry over.** If `answers` fires twice in a row while `questions` stays quiet, you get **zero** runs until `questions` finally lands — the extra `answers` event is not banked.

The paused-DAG trap from §12 stacks on top of this: events that arrive while the consumer is **paused** don't count at all. Paused + reset together mean assets are strictly for *going forward* — backfilling stays a time-based job.

---

## 5. When to use which — a decision table, not a feature list

Deep understanding is knowing which tool the situation calls for. All four of these go in `schedule=`:

| You want…                                                                 | Use                                    | Fires when…                                                    |
| ------------------------------------------------------------------------- | -------------------------------------- | ------------------------------------------------------------- |
| all inputs fresh before running (a join / mart)                            | `a & b` (`AssetAll`)                   | every asset has an event since last run; then resets          |
| run as soon as *any* input refreshes (a cache invalidation, a fan-in log) | `a \| b` (`AssetAny`)                  | any one asset has an event; resets                            |
| a primary path with a fallback trigger                                    | `(a & b) \| c`                         | the group is satisfied, or the fallback fires                 |
| data-driven **but** guaranteed to run on a clock even if the data is dead | `AssetOrTimeSchedule` (§6)             | the asset condition fires **or** the timetable's tick arrives |

The failure mode of picking wrong: use `&` where you meant `|` and your DAG silently never runs (one input is chronically late, so "all fresh" is never true); use `|` where you meant `&` and your mart publishes off half-loaded inputs. Neither errors. Both page you. Say the trigger in English first, then write the operator that matches.

---

## 6. `AssetOrTimeSchedule` — the data-or-clock floor

The most common real requirement: "run when the data lands, **but** if the producer is dead, still run once a day so the dashboard isn't stale silently." Pure asset scheduling can't do that — a dead producer means a dead consumer, quietly. `AssetOrTimeSchedule` marries an asset condition to a timetable: it fires on **either**.

This is the one place in these two sessions where the API is **not** in `airflow.sdk` — it lives in `airflow.timetables`:

```python
from airflow.timetables.assets import AssetOrTimeSchedule
from airflow.timetables.trigger import CronTriggerTimetable

@dag(
    schedule=AssetOrTimeSchedule(                                   # ← THE MECHANIC
        timetable=CronTriggerTimetable("0 6 * * *", timezone="UTC"),  # daily floor at 06:00
        assets=(questions & answers),                                 # …or the moment both land
    ),
    ...
)
def report():
    ...
```

`timetable=` is any timetable object (here `CronTriggerTimetable`, the Airflow-3 cron timetable), and `assets=` takes the **same expression tree** from §2–3. A run triggered by the clock and a run triggered by the assets are indistinguishable to your task except by inspecting `triggering_asset_events` (empty ⇒ it was the clock). We treat the *external*-trigger cousin of this — `AssetWatcher`, where the ring comes from outside Airflow entirely — in Session 11.

---

## 7. `AssetAlias` — depending on an asset you can't name yet

Everything so far assumed you can write the URI at parse time. Sometimes you can't: the export path is `s3://exports/2026-09-23.parquet` — the date is only known when the task runs; or *which* table you wrote depends on the input. You cannot hard-code a URI you don't have. But downstream still needs to depend on "whatever today's export turned out to be." That's the `AssetAlias`.

**The idea:** declare a **stable alias name** now (parse time); at **run time** the producer resolves it to one or more concrete `Asset` objects and attaches them. The alias is a permanent doorbell; its *wiring to a real courier* is decided each run.

**Producer** — list the alias in `outlets`, then bind the real asset via the `outlet_events` accessor:

```python
from airflow.sdk import Asset, AssetAlias, task

@task(outlets=[AssetAlias("daily-export")])            # stable name, unknown target
def export(*, outlet_events) -> None:
    path = f"s3://exports/{pendulum.now().date()}.parquet"   # decided at run time
    outlet_events[AssetAlias("daily-export")].add(     # ← THE MECHANIC: bind alias → real Asset
        Asset(path), extra={"path": path},
    )
```

**Consumer** — schedule or `inlets` on the **alias**; you receive whichever concrete asset the run resolved:

```python
@task(inlets=[AssetAlias("daily-export")])
def load(*, inlet_events) -> None:
    latest = inlet_events[AssetAlias("daily-export")][-1]   # newest resolved event
    print(latest.extra["path"])                             # the path this run actually wrote
```

You can also resolve an alias by `yield`ing `Metadata(asset, extra={...}, alias=AssetAlias("daily-export"))` from an `@asset`-style producer — same result, matching the two producer styles from §12.4. Pick one; never write both.

**The subtlety that trips people:** an alias with **no concrete asset ever added** resolves to *nothing*, so a DAG scheduled on it **never runs**. The alias only becomes a live trigger after the first run resolves it to a real asset — until then there's a doorbell but no wire behind it. If your alias-scheduled DAG is dead on arrival, check that the producer actually called `.add(Asset(...))`.

---

## 8. Complete runnable DAGs (your reference)

Three producers and one consumer that uses a **nested** condition, plus an alias resolved at run time. Plain, no BigQuery; type this into `dags/stage-3-assets/s10/s10_examples.py`.

```python
from __future__ import annotations

import pendulum
from airflow.sdk import Asset, AssetAlias, dag, task

# shared dependencies — defined once, on the Stack Overflow spine
questions = Asset(uri="file:///data/questions.csv", name="questions")
answers = Asset(uri="file:///data/answers.csv", name="answers")
tags = Asset(uri="file:///data/tags.csv", name="tags")

COMMON = {
    "start_date": pendulum.datetime(2026, 1, 1, tz="UTC"),
    "catchup": False,
    "tags": ["session-10"],
    "default_args": {"owner": "akhand", "retries": 1},
}


@dag(dag_id="s10_load_questions", schedule="@daily", **COMMON)
def load_questions():
    @task(outlets=[questions])
    def run_load() -> None:
        print("questions loaded → event written")

    run_load()


@dag(dag_id="s10_load_answers", schedule="@daily", **COMMON)
def load_answers():
    @task(outlets=[answers])
    def run_load() -> None:
        print("answers loaded → event written")

    run_load()


@dag(dag_id="s10_reload_tags", schedule="@daily", **COMMON)
def reload_tags():
    # resolves the alias to a concrete, run-time-named asset AND fires the plain `tags` bell
    @task(outlets=[tags, AssetAlias("daily-export")])
    def run_reload(*, outlet_events) -> None:
        path = f"file:///data/tags-{pendulum.now().date()}.csv"   # name known only now
        outlet_events[AssetAlias("daily-export")].add(Asset(path), extra={"path": path})
        print(f"tags reloaded → {path}")

    run_reload()


@dag(dag_id="s10_health_mart", schedule=((questions & answers) | tags), **COMMON)
def health_mart():                                   # ← nested condition drives the trigger
    @task
    def build(**context) -> None:
        fired = context["triggering_asset_events"]
        names = [a.name for a in fired]
        print(f"mart building — triggered by: {names or 'time/manual'}")

    build()


load_questions()
load_answers()
reload_tags()
health_mart()
```

```bash
python dags/stage-3-assets/s10/s10_examples.py                 # parses all four DAGs
airflow dags test s10_reload_tags 2026-01-01    # resolves the alias; fires `tags`
```

In a running scheduler with `s10_health_mart` **unpaused**: a `tags` reload alone triggers it (the `| tags` branch); `questions` alone does not; `questions` then `answers` triggers it once, then the AND set resets. Watch it in the UI **Assets** view. (`dags test` runs one DAG in isolation, so use the scheduler/UI to see the cross-DAG trigger, or `airflow dags test s10_health_mart 2026-01-01` to run just its body.)

---

## 9. Build spec — your challenge (no solution)

**File:** `dags/stage-3-assets/s10/s10_assignment.py` · **dag_ids:** `s10_prod_questions`, `s10_prod_answers`, `s10_prod_votes`, `s10_tiered_mart`

Build a **tiered mart** whose trigger is a nested boolean condition, fed by one runtime-resolved alias.

**The problem:**

- Define three assets on the spine: `questions`, `answers`, `votes`.
- **Three producer DAGs**, each with a task that `outlets` one asset and logs a message. The `votes` producer must **also** resolve an `AssetAlias("votes-partition")` to a concrete `Asset` whose URI includes the run date, attaching `extra={"partition": <uri>}`.
- **One consumer** `s10_tiered_mart` scheduled on `(questions & answers) | votes` — it runs when **both** core inputs are fresh, **or** when a standalone `votes` refresh lands. Its task logs which asset events triggered it via `triggering_asset_events`.

**Constraints:**

- Plain TaskFlow, no BigQuery. Use the `&` / `|` operators (or `AssetAll` / `AssetAny` explicitly) — do not write any waiting logic yourself.
- The consumer references the **assets/expression**, never the producers' dag_ids.
- Parenthesize the condition explicitly (§3).
- All DAGs pass the integrity gates: non-empty `tags`, real `owner`, `retries >= 1`.

**Acceptance criteria:**

- `python dags/stage-3-assets/s10/s10_assignment.py` parses all four DAGs.
- The UI **Assets** view shows all three assets and the alias, with `s10_tiered_mart` as a consumer.
- With `s10_tiered_mart` unpaused: running **votes alone** triggers it once; running **questions alone** does **not**; running **questions then answers** triggers it exactly once, and the AND set then resets.
- `python -m pytest tests/ -v` stays green.

**One nudge (only if stuck):** the schedule is *one expression* — `schedule=((questions & answers) | votes)`. The alias resolution is `outlet_events[AssetAlias("votes-partition")].add(Asset(uri), extra={...})` inside the votes producer task (add `*, outlet_events` to its signature).

---

## 10. Production tip — the mart that silently never ran (2am)

The page: the exec dashboard is 40 hours stale and nobody got an alert. Root cause: someone "tightened" the mart's schedule from `(questions | answers)` to `(questions & answers)` to stop it publishing on half-loaded data — reasonable — but the `answers` loader had been quietly failing for two days. Under `|` the mart still ran off `questions`; under `&` it needs **both**, so it correctly, silently, **never fired**. No error, no run, no alert — assets did exactly what the operator says. The `&`/`|` swap is a one-character edit that changes *whether the DAG runs at all*, and it's invisible in review.

- **Every `&` needs a floor.** If a mart must have all inputs, wrap it in `AssetOrTimeSchedule` (§6) with a daily tick, so a dead producer produces a *late run on stale data you can see and alert on* instead of *no run and silence*. Silence is the worst failure mode.
- **Alert on absence, not just on failure.** A DAG that never starts throws no task error. Add a "hasn't run in N hours" monitor on any asset-scheduled consumer — the condition-never-satisfied bug is invisible to failure-based alerting.
- **Treat the boolean as a public contract.** Changing `|` to `&`, or re-nesting the parens, changes downstream firing for everyone who reasoned about the old shape. Review a schedule change like an API change, because it is one.

---

Sources:
[Asset-Aware Scheduling — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/asset-scheduling.html),
[Asset Definitions — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/assets.html),
[Timetables (AssetOrTimeSchedule) — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/timetable.html),
[airflow.sdk API reference — Task SDK](https://airflow.apache.org/docs/task-sdk/stable/api.html),
[Advanced asset scheduling — Astronomer](https://www.astronomer.io/docs/learn/airflow-datasets)
