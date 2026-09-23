# Session 21 · Executors & concurrency

**Goal:** understand **what actually runs your tasks** (the executor) and **how many can run at once** (the concurrency funnel). Two ideas to nail: (1) the **executor** is a swappable component that decides *where* a task runs — one machine's subprocesses, a Celery worker pool, a Kubernetes pod, an edge box — and you pick it in config, not in DAG code; (2) concurrency is a **stack of narrowing funnels** — `parallelism` → pools → `max_active_tasks` (per DAG) → `max_active_runs` (per DAG) → `priority_weight` — where the **narrowest tier wins** and decides real throughput. Get the funnel order straight and "why is my task stuck in `queued`" stops being a mystery. This is a config/infra topic, so the reference DAG is a **plain DAG** demonstrating the per-DAG knobs — BigQuery adds nothing here (flagged per R16). *(Stage 6 — scale & harden.)*

---

## 1. Why this session exists, and the running analogy

So far you've written DAGs and trusted that "something" ran the tasks. That something is the **executor**, and *how many* it lets run at once is governed by a set of limits that, misconfigured, produce the two most common Airflow complaints: "everything runs at once and melts the DB" and "my task sits in `queued` forever and I don't know why." Both are concurrency problems, and both are solved by understanding one picture.

**The running analogy: a restaurant kitchen.**

- The **executor** is *how the kitchen is staffed*: one cook doing everything (LocalExecutor), a brigade of line cooks you keep on payroll (CeleryExecutor), temps you hire per-dish and send home after (KubernetesExecutor), or a cook stationed in a different building (EdgeExecutor). Same recipes (your DAGs), different staffing model.
- **Concurrency** is *the funnel of limits on how many dishes cook at once*: the total burners in the building (`parallelism`), a reserved station for one station's dishes (a **pool**), a cap on dishes from one menu (`max_active_tasks` per DAG), a cap on simultaneous seatings of that menu (`max_active_runs`), and — when burners are scarce — who gets the next free burner (`priority_weight`).

Every "stuck in queued" incident is the kitchen hitting one of those caps. §5 is just: **which burner limit did you hit.**

---

## 2. What an executor is (the mechanism)

The scheduler decides *which* task instances are ready to run; the **executor** decides *where and how* they actually run. It's a pluggable class set once per deployment — **not in DAG code** — in the `[core]` section:

```ini
[core]
executor = LocalExecutor
```

or via env: `AIRFLOW__CORE__EXECUTOR=LocalExecutor`. Built-in names are used bare; a custom executor uses its full module path (`my.pkg.MyExecutor`). The key mechanism: **swapping the executor changes where tasks run without touching a single DAG** — your `@dag`/`@task` code is executor-agnostic. That separation is the point.

Airflow 3 also supports **multiple executors at once** — a comma-separated list in `executor`, with the first as the default; a task or DAG can then opt into a specific one via the `executor` argument. (The old statically-fused hybrids `LocalKubernetesExecutor`/`CeleryKubernetesExecutor` were **removed in Airflow 3.0** — use the multiple-executors feature instead.)

---

## 3. The executor types (concept level)

| Executor | Where tasks run | Scales to | Reach for it when |
|---|---|---|---|
| **LocalExecutor** | subprocesses on the **scheduler's own machine** | one box's cores | dev, small/single-node prod; simple, no broker |
| **CeleryExecutor** | a pool of **long-running worker processes** across machines, via a message broker (Redis/RabbitMQ) | many always-on workers | steady, high task volume; workers you keep warm |
| **KubernetesExecutor** | **one pod per task**, created on demand and torn down after | elastic, per-task isolation | bursty/heterogeneous workloads; per-task resources & images |
| **EdgeExecutor** | workers running **outside the core cluster** (remote/edge sites) pulling work over HTTP | distributed/edge locations | tasks that must run near data or behind another network |

The two dimensions that separate them: **persistent workers vs per-task provisioning** (Celery keeps workers warm → low per-task latency, always-on cost; Kubernetes spins a pod per task → clean isolation and elastic scale, pod-startup latency), and **local vs distributed** (Local is one machine; the rest span machines). You don't need to configure these to learn concurrency — the funnel in §4–5 behaves the same regardless of executor.

**Non-stdlib components named here:** *Celery* is a distributed task-queue library Airflow uses to hand tasks to workers via a *broker* (Redis or RabbitMQ, the message bus that queues tasks); *Kubernetes* is the container orchestrator that runs each task as an isolated pod. You configure them at deploy time; your DAGs never import them.

---

## 4. The concurrency funnel — the core mental model

Concurrency in Airflow is a series of **nested caps**, from cluster-wide down to a single task. A task instance can only start if it passes **every** tier. The narrowest one it hits is the bottleneck:

```
parallelism            ← whole deployment: max task instances running at once, everywhere
   └─ pool slots       ← a named budget shared by any tasks assigned to that pool
        └─ max_active_tasks (per DAG)   ← cap on running tasks across all runs of ONE dag
             └─ max_active_runs (per DAG) ← cap on concurrently active runs of ONE dag
                  └─ priority_weight      ← tiebreak: when slots are scarce, who goes first
```

Read it top-down: even if a DAG *allows* 32 parallel tasks, they won't run if `parallelism` is 16, or if their pool has 4 slots, or if only 1 run of the DAG is allowed active. **The tightest tier decides throughput** — which is exactly why raising one limit often changes nothing (you weren't bound by that one).

---

## 5. The funnel tier by tier

**`parallelism`** — `[core] parallelism` (env `AIRFLOW__CORE__PARALLELISM`). The absolute ceiling on task instances running **across the entire deployment**, per scheduler. This is the total burners in the building. Default is 32. Nothing runs beyond this no matter what any DAG asks for.

**Pools** — a named slot budget you assign tasks to (Session-independent admin object), created in **Admin → Pools** or `airflow pools set <name> <slots> <desc>`. A task joins with `pool="name"` and can claim more than one slot with `pool_slots=N`. Tasks with no pool go to **`default_pool`** (128 slots, editable, not removable). Pools throttle an **arbitrary set of tasks** — e.g. cap everything that hits one fragile API to 3 concurrent regardless of which DAG they're in.

```python
run_query = SomeOperator(task_id="run_query", pool="warehouse", pool_slots=1)  # ← joins the pool budget
```

**`max_active_tasks`** — per-DAG, set on `@dag(max_active_tasks=...)` (config default `max_active_tasks_per_dag`, formerly `dag_concurrency`). Max task instances running **at once across all active runs of that one DAG** — stops a single wide DAG from eating the whole cluster.

**`max_active_runs`** — per-DAG, `@dag(max_active_runs=...)` (default `max_active_runs_per_dag`). Max **DAG runs** active simultaneously for that DAG. Set it to `1` when runs must not overlap (e.g. an incremental load that would double-count if two ran together).

**`priority_weight`** — per-task, `@task`/operator arg (default `1`). **Not a cap — a tiebreak.** When more tasks are ready than there are free slots in a pool, the scheduler runs higher `priority_weight` first. `weight_rule` (`downstream` default, `upstream`, `absolute`) decides whether a task's effective weight also sums its dependents' weights, so a task that unblocks a lot of work naturally floats up the queue.

```python
@dag(max_active_tasks=8, max_active_runs=1, ...)   # ← per-DAG funnel tiers
def pipeline():
    @task(priority_weight=10, pool="warehouse")     # ← tiebreak + pool membership
    def urgent() -> None: ...
```

---

## 6. Complete runnable DAG (your reference)

A plain DAG that **demonstrates the per-DAG funnel** — `max_active_runs=1`, `max_active_tasks` limiting a fan-out, a pool, and `priority_weight` ordering — so you can watch the caps bite in the UI. No BigQuery: this topic is about *scheduling limits*, and a warehouse call would only add cost and obscure the tiers (flagged per R16). *(Create the pool once first: `airflow pools set demo_pool 2 "session 21 demo"`.)*

```python
from __future__ import annotations

import time

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s21_concurrency_demo",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,        # ← per-DAG: never overlap runs of this DAG
    max_active_tasks=2,       # ← per-DAG: at most 2 of this DAG's tasks run at once
    tags=["session-21", "concurrency"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():

    @task(pool="demo_pool", pool_slots=1, priority_weight=1)   # low priority; shares the 2-slot pool
    def slow_worker(n: int) -> None:
        time.sleep(5)                                          # hold a slot long enough to see queuing
        print(f"worker {n} done")

    @task(pool="demo_pool", pool_slots=1, priority_weight=10)  # ← higher weight: grabs a free slot first
    def priority_worker() -> None:
        print("priority worker ran first when slots were scarce")

    priority_worker()
    slow_worker.expand(n=[1, 2, 3, 4])   # 4 mapped tasks, but max_active_tasks=2 + a 2-slot pool gate them


pipeline()
```

```bash
airflow pools set demo_pool 2 "session 21 demo"
python dags/s21/s21_examples.py
airflow dags test s21_concurrency_demo 2026-01-01
```

Watch the four `slow_worker` mapped instances: even though the DAG has five runnable tasks, **at most two run at once** — pinned by both `max_active_tasks=2` and the 2-slot `demo_pool`, whichever you narrow. Drop the pool to 1 slot (`airflow pools set demo_pool 1 "…"`) and they serialise. Bump one worker's `priority_weight` and it jumps the queue when a slot frees. That's the funnel, live.

---

## 7. Build spec — your challenge (no solution)

**File:** `dags/s21/s21_assignment.py` · **dag_id:** `s21_assignment`

Build a DAG that **provably** throttles itself through the funnel.

**The problem:**

- Create a pool with a **small** slot count (e.g. `airflow pools set so_pool 2 "assignment"`).
- A `@task` mapped/expanded to **at least 6** instances, each assigned `pool="so_pool"` and sleeping a few seconds, so more are *ready* than the pool allows.
- Set the DAG's `max_active_runs=1` and a `max_active_tasks` that is **not** the binding limit (i.e. larger than the pool), so the **pool** is demonstrably the bottleneck.
- Give one task a higher `priority_weight` and confirm it starts ahead of the others when slots are scarce.

**Constraints:**

- Plain TaskFlow, no BigQuery.
- Executor/pool configuration lives in **config/CLI**, never invented in DAG code (only `pool`, `pool_slots`, `priority_weight`, `max_active_*` args belong in the DAG).
- Passes the integrity gates: `tags`, real `owner`, `retries >= 1`.

**Acceptance criteria:**

- `python dags/s21/s21_assignment.py` parses (prints nothing).
- Running it, **no more than `so_pool` slots** of the mapped task run concurrently, even though `max_active_tasks` would allow more — proving the narrowest tier wins.
- The high-`priority_weight` task starts before equal-priority peers when the pool is saturated.
- `python -m pytest tests/ -v` stays green.

**One nudge (only if stuck):** to *see* concurrency you need tasks that overlap in time — `time.sleep(...)` in each mapped task. If nothing appears throttled, your pool is bigger than the number of ready tasks; shrink the pool or widen the fan-out.

---

## 8. Production tip — the task stuck in `queued` that wasn't broken

The bug that pages you at 2am: a critical report task sits in **`queued`** for hours. Logs are empty (it never started), the DAG is unpaused, the code is fine, and someone's already restarted the scheduler twice. The real cause: a neighbouring backfill DAG with `max_active_tasks=64` had flooded the **`default_pool`** (128 slots) and pinned `parallelism` (32); the report's task was *ready* but there was **no free slot in any tier**, so the scheduler correctly left it `queued`. Nothing was broken — a funnel was full, and the report had `priority_weight=1` like everything else, so it never jumped the line.

- **`queued` for a long time = a full funnel, not a bug.** Walk the tiers in order: is `parallelism` maxed? Is the task's **pool** out of slots? Is the DAG at `max_active_tasks` / `max_active_runs`? The tier that's full is your answer — restarting the scheduler fixes none of them.
- **Isolate greedy workloads with a pool.** A backfill or a fragile-API task should live in its **own** pool so it can't starve everything sharing `default_pool`. Pools are the cheapest insurance against one DAG eating the cluster.
- **Give genuinely urgent tasks real `priority_weight`.** If everything is weight `1`, "important" and "batch cleanup" compete equally for the last free slot. Weight the tasks that page you above the ones that don't.

---

## 9. Verify + commit

```bash
airflow pools set so_pool 2 "assignment"
python dags/s21/s21_assignment.py
airflow dags test s21_assignment 2026-01-01
python -m pytest tests/ -v
git add -A && git commit -m "session 21: executors & concurrency" && git push
```

Done when the mapped task is visibly capped by the pool (not `max_active_tasks`) and the weighted task jumps the queue. Tick Session 21 in `docs/course/README.md`.

**Pre-push habit:** `ruff check dags/ include/ tests/ --select E,F,AIR3 && python -m pytest tests/ -v`.

Sources:
[Executor concepts — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/executor/index.html),
[Pools — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/pools.html),
[Scheduler & concurrency config — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/scheduler.html),
[Priority weights — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html),
[FAQ (parallelism / max_active_tasks) — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/faq.html)
