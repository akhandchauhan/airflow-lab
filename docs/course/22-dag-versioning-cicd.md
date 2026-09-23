# Session 22 · DAG Versioning & CI/CD

**Goal:** make a DAG run **reproducible** and make bad code **impossible to merge**. Two ideas, one habit. First: in Airflow 3 a run is **pinned to the DAG code version it started with** — the scheduler and the workers use the code that existed at the moment the run was created, not whatever is on disk now, so a deploy mid-run never rewrites history. Second: the gate that keeps `main` deployable is not code review vibes, it's three cheap automated checks on every push — `ruff check --select AIR3` (no Airflow-2 leftovers), a **dag-integrity test** (no import errors, every DAG has tags/owner/retries), and **GitHub Actions** running both before merge. Get versioning and the gate right and the 2am "which version was running when it broke?" question has an answer, and the "who merged the DAG that took down the scheduler?" question stops happening. _(Plain DAGs, no BigQuery — this is an infrastructure session; the reference DAG is flagged plain.)_

---

## 1. Why versioning exists — the Airflow 2 hole it closes

Here is the analogy for the whole session: think of a DAG run as a **plane already in the air**. In Airflow 2, if you pushed new DAG code while a run was executing, you were **rebuilding the plane mid-flight**. The scheduler re-parsed the file, saw the new shape, and started making decisions for the *in-progress* run against code it never took off with. Tasks that existed at takeoff vanished from the grid; new tasks appeared with no history; a cleared task reran with logic that didn't exist when the run started. The run's own past became unreadable.

The concrete failures this caused, all from the same root:

| Symptom in Airflow 2 | Root cause |
| --- | --- |
| Grid view "loses" tasks after a deploy | the grid renders the *current* file, not the run's file |
| A cleared/retried task runs new logic | retry re-parses current code, not the code at run creation |
| "Which code produced this failed run?" is unanswerable | nothing recorded the code version per run |
| A push during a running backfill corrupts it | mid-flight structure change |

Airflow 3's fix is one sentence: **each DAG run is bound to the DAG version it was created with, for its entire life** — scheduling decisions and task execution both use that pinned version. The plane lands as the plane that took off.

---

## 2. What a "DAG version" actually is (the mechanism)

A DAG version is **a snapshot of the DAG's structure**, recorded in the metadata DB, created **lazily**: Airflow writes a new version only when a run is created for a DAG whose *structure changed* since the last version. It is not a git commit and it is not a timestamp of every file save — save the file ten times with no structural change and there is still one version.

**What counts as a structural change** (per the Astronomer docs, a new version is created when a run starts after any of these): changes to **DAG or task parameters, task dependencies, task IDs, or adding or removing tasks**. Reformatting the file, editing a comment, or tweaking the *body* of a Python task without changing its wiring does **not** mint a new version.

The rule that makes the rest fall out:

> The **scheduler** uses the DAG version that existed at the time of the DAG run to decide **which task instances to create**; the **workers** use the code in the bundle version that existed at run-creation time to **execute** them. Both ends read the same pinned snapshot.

Two consequences you will actually see in the UI:

- The **grid view retains history for all tasks, even ones removed in the latest version** — because each run remembers its own shape. A task you deleted yesterday still shows its green squares on yesterday's runs.
- An Options menu lets you **select which version of the DAG graph to display**, so you can look at a run through the exact code it ran, not today's code.

---

## 3. DAG bundles — where the pinned code actually lives

Pinning "which task instances" is only half of reproducibility. To rerun a task with the *original code*, Airflow has to still **have** the original code. That is what a **DAG bundle** is for. This is a new, non-stdlib Airflow 3 concept, so: a **DAG bundle** is "a collection of one or more DAGs and their associated files" (other Python modules, configs, resources) sourced from some location — a folder, a git repo — that Airflow's DAG processor loads DAGs from. Bundles are configured under `[dag_processor]` via the `dag_bundle_config_list` key; each entry has a `name`, a `classpath`, and `kwargs`.

The two bundle classes and the single property that separates them — **versioning**:

| Bundle | Classpath | Versioned? | Behavior |
| --- | --- | --- | --- |
| **LocalDagBundle** (default) | `airflow.dag_processing.bundles.local.LocalDagBundle` | **No** | tasks always run using the **latest** code on disk |
| **GitDagBundle** | `airflow.providers.git.bundles.git.GitDagBundle` | **Yes** | a run is pinned to the exact **git commit** it started with, even after the repo moves on |

This is the catch that trips people: **DAG versioning's "rerun with the original code" only fully works on a *versioned* bundle.** The default `LocalDagBundle` records the run's *structure* (so the grid history is right), but because it has no way to fetch old code, a retry runs whatever the folder holds now. If you want a cleared task from three deploys ago to rerun with *its* code, you need `GitDagBundle`, which checks out the run's pinned commit. Key `GitDagBundle` kwargs: `tracking_ref` (the branch/tag/commit to follow) and `git_conn_id` (credentials). `refresh_interval` (settable globally or per-bundle in `kwargs`) controls how often the DAG processor looks for new files.

**Rerun default — `rerun_with_latest_version`:** when a user clears a DAG run/task instance or runs a backfill, this parameter decides whether the new run uses the **most recent** bundle version or the run's **original** version. Set to `True`, clears and backfills jump to the latest code unless overridden per-action; left default, a rerun honors the pinned version. Know which one your team wants *before* someone clears a month-old run and is surprised by which code executes.

---

## 4. The other half: the CI/CD gate

Versioning tells you *what ran*. CI/CD stops the *wrong thing from ever running*. The gate is three checks, cheapest-first, all runnable locally and all wired into `main` so a red check blocks the merge.

### a) `ruff check --select AIR3` — no Airflow-2 leftovers

Ruff ships Airflow-specific rules under the **`AIR3`** prefix that flag code the Airflow 3 runtime removed or deprecated. The two families:

| Rule family | Severity | Catches |
| --- | --- | --- |
| **AIR301 / AIR302** (`AIR30`) | breaking — **must** fix | removed imports/params/values (e.g. `schedule_interval=`, `airflow.operators.*` legacy paths, `airflow.models` public imports) |
| **AIR311 / AIR312** (`AIR31`) | suggested | deprecated-but-still-working syntax with a compatibility shim |

Run it:

```bash
ruff check --preview --select AIR3 dags/        # AIR3 rules are still behind --preview
ruff check --preview --select AIR3 --fix dags/  # auto-fix the safe ones
```

Many fixes apply with `--fix`; import rewrites are marked **unsafe** and need `--fix --unsafe-fixes`. Gate on the breaking family at minimum (`--select AIR301,AIR302`) so no Airflow-2 idiom reaches `main`.

### b) The dag-integrity test — the cheapest bug-catcher you own

This is the `tests/dags/test_dag_integrity.py` you already ship. It parses `dags/` into a **DagBag** with **no scheduler, no running Airflow, no database**, and asserts the boring things that otherwise surface as a red DAG at 3am:

- `test_no_import_errors` — **every file imports cleanly** (a single typo'd import in one DAG breaks parsing for the whole folder).
- `test_dag_has_tags` — every DAG has `tags` (findable in a UI with 500 DAGs).
- `test_tasks_have_retries` — every task has `retries >= 1` (a blip shouldn't page you).
- `test_tasks_have_real_owner` — no task owned by `"airflow"` (on-call needs a name).

Two Airflow-3 specifics baked into that test, because every Airflow-2 tutorial gets them wrong: import the file-parsing `DagBag` from **`airflow.dag_processing.dagbag`** (the old `airflow.models.DagBag` is a DB-backed shim), and read **`DAG_BAG.dags[dag_id]`**, never `get_dag()` (which hits the metadata DB and dies on CI with `no such table: dag`).

### c) GitHub Actions — the gate that runs itself

The point of CI is that these checks run on **every push**, not when someone remembers. A minimal workflow at `.github/workflows/ci.yml` that gates `main`:

```yaml
name: ci
on:
  push:
  pull_request:
    branches: [main]

jobs:
  dag-checks:
    runs-on: ubuntu-latest
    env:
      AIRFLOW__CORE__LOAD_EXAMPLES: "False"   # keep example DAGs out of the DagBag
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - run: pip install -r requirements.txt
      - run: ruff check --preview --select AIR3 dags/
      - run: python -m pytest tests/ -v
```

Make this workflow a **required status check** in the repo's branch-protection settings so a red run *blocks the merge button* — CI that doesn't block is just a slower way to find out you broke `main`.

---

## 5. Complete runnable reference DAG (plain — flagged)

**Flagged plain per R16:** no BigQuery, no connection. This DAG's whole job is to *demonstrate versioning*: run it, then change its structure (add a task, rename a `task_id`, or rewire the dependency) and trigger it again — the second run mints a **new DAG version**, and the grid keeps *both* shapes.

**File:** `dags/s22/versioning_demo.py` · **dag_id:** `s22_versioning_demo`

```python
# dags/s22/versioning_demo.py
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s22_versioning_demo",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-22", "versioning"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():

    @task
    def extract() -> int:
        print("extract → 42 rows")
        return 42

    @task
    def report(rows: int) -> None:
        # STRUCTURAL EXPERIMENT: add `validate` between these two, or rename this
        # task_id, then trigger again — that mints a new DAG version. ← THE MECHANIC
        print(f"report on {rows} rows")

    report(extract())


pipeline()
```

```bash
python dags/s22/versioning_demo.py            # parses cleanly
airflow dags test s22_versioning_demo 2026-01-01   # run v1
# now add a `validate` task between extract and report, save, and:
airflow dags test s22_versioning_demo 2026-01-02   # run v2 — new version, old grid kept
```

In a running scheduler, open the DAG's grid, use the Options menu to switch the graph between versions, and confirm the first run still renders with the *old* shape.

---

## 6. Build spec — your challenge (no solution)

**File:** `dags/s22/s22_assignment.py` · **dag_id:** `s22_assignment`

Wire the CI gate for this repo and prove it catches a bad DAG.

**The problem:**

- Add a **GitHub Actions workflow** (`.github/workflows/ci.yml`) that, on every push and PR to `main`, runs **both** `ruff check --preview --select AIR3 dags/` and `python -m pytest tests/ -v`.
- Make the workflow a **required status check** on `main` (branch protection) so a failing run blocks merge.
- In `dags/s22/s22_assignment.py`, build a small **plain** DAG (no BigQuery) that *passes* every integrity gate.

**Constraints:**

- Plain TaskFlow, `airflow.sdk` only, no Airflow-2 idioms (it must survive `--select AIR3`).
- The DAG passes the integrity gates: non-empty `tags`, real `owner`, `retries >= 1`.
- The workflow sets `AIRFLOW__CORE__LOAD_EXAMPLES: "False"` so example DAGs don't pollute the DagBag.

**Acceptance criteria:**

- `python dags/s22/s22_assignment.py` parses (prints nothing on import).
- `ruff check --preview --select AIR3 dags/` and `python -m pytest tests/ -v` both pass locally.
- **Prove the gate bites:** open a throwaway branch, add a task with `retries=0` (or a bad import), push, and watch the Actions run go **red** and the merge button lock. Revert it.

**One nudge (only if stuck):** you don't write any "is this DAG valid?" logic yourself — `test_dag_integrity.py` already parameterizes over every `dag_id` in the DagBag. Your job is to make CI *run* it and make a red run *block the merge*, not to reinvent the check.

---

## 7. Production tip — the deploy that rewrote a running backfill (2am)

The page that defines this session: someone kicked off a **30-day backfill** at 22:00, then at 23:30 merged a PR that renamed a `task_id` and pushed. On Airflow 2, the DAG processor re-parsed the file, the *in-flight* backfill's remaining runs picked up the new structure, and half the backfill created tasks under the new id while the other half had already run the old one. The grid became unreadable, some days silently skipped the renamed task, and reconstructing "which days actually completed" took until sunrise.

The habits that prevent it:

- **Versioning is the seatbelt, not the fix for careless deploys.** In Airflow 3 the *running* backfill stays pinned to its start version, so the mid-flight rename no longer corrupts it — but only a **versioned bundle (`GitDagBundle`)** lets you later rerun a failed day with *its* original code. On the default `LocalDagBundle`, a cleared task reruns with today's file. Pick the bundle that matches how you rerun.
- **Know your `rerun_with_latest_version` default before you clear anything.** The 2am mistake mutates into a 2am *surprise* if someone clears an old run expecting original code and gets latest, or vice versa. Decide it as a team, in config, not per-panic.
- **Let CI, not courage, guard `main`.** The rename that started this would have been caught pre-merge by the dag-integrity import test if it broke parsing — and a required GitHub Actions check makes "I'll just push the hotfix" impossible to do around the gate. The cheapest incident is the one that never merges.

---

Sources:
[DAG Bundles — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/dag-bundles.html),
[DAG Versioning and DAG Bundles — Astronomer Learn](https://www.astronomer.io/docs/learn/airflow-dag-versioning),
[DAG Versioning — Astronomer Astro](https://www.astronomer.io/docs/astro/dag-versioning),
[Upgrading to Airflow 3 — Airflow docs](https://airflow.apache.org/docs/apache-airflow/stable/installation/upgrading_to_airflow3.html),
[airflow3-removal (AIR301) — Ruff](https://docs.astral.sh/ruff/rules/airflow3-removal/),
[airflow3-suggested-update (AIR311) — Ruff](https://docs.astral.sh/ruff/rules/airflow3-suggested-update/),
[Upgrade Airflow 2 to 3 — Astronomer](https://www.astronomer.io/docs/learn/airflow-upgrade-2-3)
</invoke>
