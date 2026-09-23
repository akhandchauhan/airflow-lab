# Session 31 · Multi-tenancy

**Goal:** run **many teams on one Airflow** without them stepping on each other — separate code ownership, separate permissions, separate compute budgets, separate blast radius. One idea to nail: Airflow is a **shared platform**, and by default everything is global — one DAGs folder, one set of pools, one permission surface — so multi-tenancy is the deliberate work of drawing **boundaries** across four axes: **code** (which team's DAGs live where → DAG bundles), **access** (who can see/run what → RBAC roles + the auth manager), **compute** (whose tasks run where and how many → per-team `queue` and pools), and **isolation** (a bad DAG from team A must not take down team B). This is a concept/governance topic, so the reference is a **plain DAG** modelling per-team ownership and compute — no BigQuery, which adds nothing here (flagged per R16). *(Stage 6 — scale & harden.)*

---

## 1. Why multi-tenancy is hard, and the running analogy

A single-team Airflow is simple: everyone trusts everyone, one folder of DAGs, one admin. The moment a **second** team shares the instance, silent conflicts appear. Team A ships a DAG that hammers `default_pool` and starves team B's nightly load (Session 21's funnel, filled by a stranger). Team B can see — and *trigger*, and *delete* — team A's DAGs in the UI because permissions are global. A syntax error in team A's DAG file breaks parsing and team B's DAGs vanish from the list. None of these is a bug in Airflow; they're the *absence of boundaries*. Multi-tenancy is the work of adding them.

**The running analogy: one office building, many companies.** A single-tenant office is one company with a key to everything. A **multi-tenant building** rents floors to different companies: each has its own locked floor (code ownership), its own keycard access (RBAC), its own metered utilities so one tenant's server room doesn't trip another's breaker (pools/queues), and fire doors so a flood on floor 3 doesn't reach floor 4 (isolation). The building (Airflow) is shared; the **boundaries between floors** are what make it safe to rent to strangers. Every section below is one kind of wall.

The rule underneath: **Airflow's defaults are single-tenant — global folder, global pools, global permissions.** Multi-tenancy is not a switch you flip; it's four boundaries you draw on purpose.

---

## 2. Code ownership — DAG bundles

Airflow 3 replaced the single global DAGs folder with **DAG bundles**: *"a collection of one or more Dags ... along with their associated files."* Instead of every team dumping files into one directory, **each team gets its own bundle** — its own Git repo, branch, or bucket — and Airflow loads them all. That gives each team independent ownership of *its* code: team A pushes to team A's repo, team B to team B's, and neither can edit the other's source.

Bundles are configured under `[dag_processor]` with **`dag_bundle_config_list`** — a list where each entry has a `name`, a `classpath`, and `kwargs`. Built-in classpaths include `airflow.dag_processing.bundles.local.LocalDagBundle` (a local dir), `airflow.providers.git.bundles.git.GitDagBundle` (a Git repo, **versioned** — so a run uses consistent code even if the repo changes mid-run), and cloud variants (`S3DagBundle`, `GCSDagBundle`).

```ini
[dag_processor]
dag_bundle_config_list = [
  {"name": "team_a", "classpath": "airflow.providers.git.bundles.git.GitDagBundle",
   "kwargs": {"git_conn_id": "team_a_git", "tracking_ref": "main"}},
  {"name": "team_b", "classpath": "airflow.providers.git.bundles.git.GitDagBundle",
   "kwargs": {"git_conn_id": "team_b_git", "tracking_ref": "main"}}
]
```

Two teams, two repos, one Airflow — each owning its floor. (Security note from the docs: reference bundle credentials through a **Connection**, never inline them, since config can be exposed via the Config API.)

---

## 3. Access — auth managers and RBAC roles

Who can *see, trigger, and edit* what is decided by the **auth manager** — a pluggable component (set via **`[core] auth_manager`**) that Airflow delegates all authentication and authorisation to. There are two you must know, and the Airflow 3 default is a genuine trap for a team instance:

- **`SimpleAuthManager`** — the **Airflow 3 default**. Fully config-driven, **no database, no persistent user table**. Users are declared in config: `[core] simple_auth_manager_users = "bob:admin,peter:viewer"` (username:role pairs), with passwords auto-generated into a file. Roles: `admin`, `op`, `user`, `viewer`. It's explicitly *"intended for development and testing"* — fine for this lab, **wrong for a real multi-team prod instance** because there are no real accounts.
- **`FabAuthManager`** — the production choice for RBAC, from the **`apache-airflow-providers-fab`** provider (install it, then set `[core] auth_manager = airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager`). It uses the metadata DB for real users, roles, and **DAG-level permissions**.

FAB's five built-in roles are cumulative:

| Role | Can do (cumulative) |
|---|---|
| **Public** | nothing (anonymous) |
| **Viewer** | read-only: view DAGs, task logs, runs |
| **User** | Viewer + trigger/edit/delete DAGs, manage task instances & runs |
| **Op** | User + connections, pools, variables, assets, admin menus |
| **Admin** | everything + manage users, roles, and permissions |

The multi-tenant lever is **DAG-level permissions**: each DAG is a resource (`DAG:team_a_load`) with `can_read`/`can_edit` actions, so you create a **custom role per team** granting access only to *that team's* DAGs. A team-B member with the `team_b` role literally cannot see or trigger team A's DAGs. You can also declare it in DAG code with `access_control={"team_b": {"can_read", "can_edit"}}` (authoritative — it overwrites existing perms for that DAG).

> **Note on scope:** true end-to-end **multi-team isolation** (AIP-67 — teams that can't even reach each other's connections/variables) is still maturing in Airflow 3 and the community is moving *away* from FAB long-term. For today, the practical, shippable boundary is **FAB custom roles + DAG-level permissions**. Know that per-DAG RBAC is the tool you actually have.

---

## 4. Compute — per-team `queue` and pools

Access boundaries stop a team from *seeing* another's DAGs; **compute boundaries** stop a team from *starving* another's tasks. Two knobs, both from earlier sessions, now used as tenancy walls:

- **`queue`** — a per-task label (`@task(queue="team_a")` or on an operator) that routes the task to workers **subscribed to that queue**. Run team A's tasks on team A's workers and team B's on team B's, and a flood of A's work never touches B's worker capacity. (Queues are meaningful with distributed executors like Celery/Kubernetes, where workers subscribe to named queues.)
- **Pools** — a named slot budget (Session 21). Give each team its **own** pool (`airflow pools set team_a 8 "team A budget"`) and assign its tasks `pool="team_a"`. Now team A can use at most 8 slots no matter how wide its fan-out, and **can't drain `default_pool`** out from under team B. This is the single cheapest multi-tenancy win: one pool per tenant, nobody in `default_pool`.

```python
@task(queue="team_a", pool="team_a", pool_slots=1)   # ← routes to team A's workers, spends team A's budget
def load_team_a() -> None:
    ...
```

The mental model: **`queue` decides *where* a team's tasks run; the pool decides *how many* at once.** Together they cap a tenant's blast radius on shared compute.

---

## 5. Isolation — the blast radius you forget

The boundary teams forget until it bites: a **bad DAG file from one team must not break another's.** Two failure modes and their walls:

1. **Parse-time failure.** A syntax error or a slow top-level import in team A's DAG can, in a shared DAG processor, delay or break parsing for everyone. Bundles help (separate sources), but the real defence is **keeping DAG files import-light** (no heavy work at module top level) and, at scale, isolating the DAG processor per bundle.
2. **Run-time resource hogging.** Covered in §4 — pools and queues cap it.

There's also a **connection/variable** boundary: on the default setup, connections and variables are global — any team with `Op` can read any connection. Genuine secret isolation between tenants needs a per-team secrets backend or the maturing multi-team features (§3 note). For this lab's purposes, the shippable isolation is: **separate bundles + import-light DAGs + per-team pools/queues + per-team DAG-level RBAC.** That's four walls, and they cover the incidents that actually happen.

---

## 6. The four boundaries at a glance

| Boundary | Question it answers | Tool |
|---|---|---|
| **Code** | whose DAGs are these? | DAG **bundles** (`dag_bundle_config_list`, per-team repo) |
| **Access** | who can see/run/edit them? | **auth manager** + RBAC (FAB custom role per team, DAG-level perms) |
| **Compute** | where do they run, how many at once? | per-team **`queue`** + **pool** |
| **Isolation** | can one team break another? | import-light DAGs, separate bundles, capped pools |

Draw all four and you have a real multi-tenant platform. Skip one and that's your next incident: skip access → teams triggering each other's DAGs; skip compute → one team starving the rest; skip code → merge conflicts in a shared folder; skip isolation → one bad file darkening everyone's DAG list.

---

## 7. Complete runnable DAG (your reference)

A plain DAG modelling **two tenants on one Airflow**: two independent pipelines, each pinned to its **own owner, tag, `queue`, and pool**, so the compute and ownership boundaries are visible in one file. RBAC and bundles are deployment config (§2–3, shown as config, not runnable Python), so the *runnable* part is the compute boundary — the part that lives in DAG code. No BigQuery: multi-tenancy is about *boundaries*, not warehouse work (flagged per R16).

> Create the two pools once first: `airflow pools set team_a 2 "team A"` and `airflow pools set team_b 2 "team B"`. The `queue` labels are inert without distributed workers subscribed to them, but they're the real production knob — they're here to show *where* the boundary is declared.

```python
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s31_team_a",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-31", "team-a"],                     # tag = the tenant, for filtering & RBAC scoping
    default_args={"owner": "team_a", "retries": 1},    # owner = who to page; a real tenant boundary
)
def team_a():

    @task(queue="team_a", pool="team_a", pool_slots=1)  # ← routes to team A workers, spends team A's budget
    def load() -> None:
        print("team A: loading on team A's queue and pool")

    load()


@dag(
    dag_id="s31_team_b",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-31", "team-b"],
    default_args={"owner": "team_b", "retries": 1},
)
def team_b():

    @task(queue="team_b", pool="team_b", pool_slots=1)  # ← team B never touches team A's pool
    def load() -> None:
        print("team B: loading on team B's queue and pool")

    load()


team_a()
team_b()
```

```bash
airflow pools set team_a 2 "team A"
airflow pools set team_b 2 "team B"
python dags/stage-6-scale/s31/s31_examples.py
airflow dags test s31_team_a 2026-01-01
```

Both DAGs share one Airflow but touch **nothing** of each other's: distinct owners (distinct on-call), distinct tags (filterable, and the unit a per-team RBAC role scopes to), distinct pools (team A saturating its pool can't slow team B), distinct queues (each runs on its own workers). That's the compute-and-ownership boundary in code; §2–3 add the code-ownership and access boundaries in deployment config.

---

## 8. Build spec — your challenge (no solution)

**File:** `dags/stage-6-scale/s31/s31_assignment.py` · **dag_id:** `s31_assignment`

Carve a **complete tenant boundary** for one team's DAG.

**The problem:**

- Build a single team-owned DAG whose `default_args["owner"]` is a real team name (not `akhand`) and whose `tags` mark the tenant.
- Its tasks **only ever** claim that team's `pool` (create it via CLI) and route to that team's `queue` — **never** `default_pool`.
- In the docstring, document the **access** boundary you'd pair with it: which FAB custom role would own this DAG, and the `access_control={...}` (or per-team role) that would scope it — and which **bundle** the team's code would live in.

**Constraints:**

- Pool/queue/role/bundle configuration is **deployment config or CLI**, never invented in DAG code — only `owner`, `tags`, `queue`, `pool`, `pool_slots` belong in the DAG (the RBAC/bundle part is documented in the docstring).
- Plain TaskFlow, no BigQuery.
- Passes the integrity gates: non-empty `tags`, real `owner`, `retries >= 1`.

**Acceptance criteria:**

- `python dags/stage-6-scale/s31/s31_assignment.py` parses (prints nothing).
- Every task in the DAG declares a non-default `pool` and a `queue` — no task silently falls back to `default_pool`.
- The docstring names the FAB role + DAG-level permission and the bundle that would complete the tenant boundary.
- `python -m pytest tests/ -v` stays green.

**One nudge (only if stuck):** a task with no `pool=` argument silently joins `default_pool` — which is the shared floor every tenant fights over. Give **every** task an explicit team pool, or the boundary has a hole in it.

---

## 9. Production tip — the tenant that took down the instance from `default_pool`

The bug that pages you at 2am: every team's nightly loads are late, dashboards are stale across the whole org, and the Airflow instance *looks* healthy — scheduler up, workers up. The cause: a new team onboarded a backfill DAG that fanned out to 500 mapped tasks, all in **`default_pool`** (128 slots) because nobody set a pool, and all on the **default queue** because nobody set one. Their backfill ate every shared slot; every *other* team's tasks sat `queued` behind it. One tenant with no boundary starved all the rest — and RBAC didn't help, because RBAC governs *access*, not *compute*.

- **Every tenant gets its own pool; nobody lives in `default_pool`.** `default_pool` is the shared commons — the moment two teams both rely on it, the greediest wins. One pool per team, sized to that team's budget, is the cheapest wall in this whole session.
- **Route tenants to their own queues.** A pool caps *how many* of a team's tasks run; a queue decides *whose workers* run them. Without queue separation, a well-behaved team still competes for the same worker processes as a misbehaving one.
- **RBAC is not resource isolation — don't confuse the two.** A per-team role stops team B *seeing* team A's DAGs; it does nothing to stop team A's tasks eating team B's compute. You need **both** the access wall (RBAC/bundles) and the compute wall (pools/queues). The 2am page is almost always the compute wall someone forgot.

---

## 10. Verify + commit

```bash
airflow pools set team_a 2 "team A"
airflow pools set team_b 2 "team B"
python dags/stage-6-scale/s31/s31_examples.py
airflow dags test s31_team_a 2026-01-01
python dags/stage-6-scale/s31/s31_assignment.py
python -m pytest tests/ -v
```

Done when both reference DAGs run pinned to their own owner/queue/pool, and the assignment documents the matching RBAC role and bundle. Tick Session 31 in `docs/course/README.md`.

**Pre-push habit:** `ruff check dags/ include/ tests/ --select E,F,AIR3 && python -m pytest tests/ -v`.

Sources:
[DAG bundles — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/dag-bundles.html),
[Auth manager (concepts) — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/auth-manager/index.html),
[Simple auth manager — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/auth-manager/simple/index.html),
[Access Control with FAB auth manager — apache-airflow-providers-fab](https://airflow.apache.org/docs/apache-airflow-providers-fab/stable/auth-manager/access-control.html),
[Pools — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/pools.html)
