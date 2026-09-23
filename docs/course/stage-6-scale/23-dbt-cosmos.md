# Session 23 · dbt via Cosmos

**Goal:** stop running your whole dbt project as one opaque `dbt run` inside a single Airflow task, and start orchestrating it so **each dbt model is its own Airflow task** — with its own logs, retries, and green/red square. You already know dbt: `dbt run` builds your models in dependency order and `dbt test` checks them. The problem is *orchestration*: wrapped in one `BashOperator`, a 50-model project is one black box — model 40 fails and you retry all 50, and the grid can't tell you *which* model broke. **Astronomer Cosmos** reads your dbt project's dependency graph and renders it into Airflow's own graph, so dbt's DAG *becomes* an Airflow DAG. Get that one idea — Cosmos is a *translator from dbt's graph to Airflow's graph* — and the config classes (`ProjectConfig`, `ProfileConfig`, `ExecutionConfig`, `RenderConfig`) are just how you tell it where the project is, how to connect, how to run dbt, and which models to include. On the Stack Overflow spine, this is where your dbt transforms run on BigQuery through `google_cloud_default`.

---

## 1. Why Cosmos exists — the black-box `dbt run` problem

The analogy: a plain `dbt run` in a `BashOperator` is a **sealed shipping container**. dbt builds all 50 models inside it in the right order, and from the outside Airflow sees exactly one thing — the container arrived, or it didn't. You get one log blob to grep, one retry button that reruns the *entire* container, and one status square for the whole transform layer.

What that costs you in production:

| With one `BashOperator: dbt run` | What you actually want |
| --- | --- |
| Model 40 of 50 fails → retry reruns **all 50** | retry **only** the failed model + its downstream |
| One giant log to grep for the failing model | per-model logs, click the red square |
| Airflow's graph shows **1 node**; dbt's real DAG is invisible | Airflow's graph mirrors dbt's model dependencies |
| A slow model is hidden inside the blob | per-model duration in the Airflow UI |
| dbt tests pass/fail inside the box, silently | each model's tests are their own visible task |

The fix is **per-model observability**: expose dbt's internal dependency graph *as* Airflow tasks. You could hand-write one `BashOperator` per model and wire the dependencies yourself — but dbt already *knows* the graph (it's in the project), so re-typing it in Airflow is duplicated, drift-prone work. Cosmos reads the graph from dbt and builds the Airflow tasks for you.

---

## 2. What Cosmos actually is (non-stdlib — read this once)

**Astronomer Cosmos** is an **open-source Python library** (`pip install astronomer-cosmos`) that "bridges Apache Airflow and dbt, allowing you to transform your dbt projects into Airflow DAGs." It is **not** part of core Airflow and **not** part of dbt — it's a third-party package Airflow imports, the same way the Google provider is. Why Airflow needs it: Airflow orchestrates *tasks*, dbt describes *models*; Cosmos is the adapter that turns the second into the first.

The mechanism, precisely:

> Cosmos **parses your dbt project** (running `dbt ls` or reading a compiled `manifest.json`) to recover the model dependency graph, then **renders one Airflow task (or task group) per dbt node** — wiring the Airflow dependencies to match dbt's `ref()` graph. When the DAG runs, each task shells out to dbt to build **just that one model** (`dbt run --select <model>`), and typically a paired task runs **just that model's tests**.

So the dbt DAG and the Airflow DAG end up **the same shape**. A `stg_questions → fct_question_health` dbt chain becomes a `stg_questions → fct_question_health` chain of Airflow tasks. Nothing about your dbt project changes — Cosmos reads it, it doesn't rewrite it.

---

## 3. The two ways to render — `DbtDag` vs `DbtTaskGroup`

Cosmos gives you two entry points, imported from the top-level package:

```python
from cosmos import DbtDag, DbtTaskGroup      # ← THE MECHANIC: two render targets
```

| Class | What it produces | Reach for it when… |
| --- | --- | --- |
| **`DbtDag`** | a **whole Airflow DAG** that *is* your dbt project | the DAG's only job is to run this dbt project |
| **`DbtTaskGroup`** | a **task group** you drop inside a DAG you already wrote | dbt is *one stage* of a bigger pipeline (extract → **dbt** → publish) |

`DbtDag` is the fast path — one object and your whole project is scheduled. `DbtTaskGroup` is the composable path — you keep your own `@dag`, and the dbt models appear as a collapsible group between your extract and your publish tasks, sharing the same run. Same rendering engine underneath; the only difference is whether Cosmos hands you a DAG or a group.

---

## 4. The four config objects (all you actually configure)

Everything Cosmos needs is split across four dataclasses, each answering one question. All import from the top-level `cosmos` package:

```python
from cosmos import ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
```

| Config | Answers | Key arguments |
| --- | --- | --- |
| **`ProjectConfig`** | *where is the dbt project?* | `dbt_project_path` (path to the folder with `dbt_project.yml`) |
| **`ProfileConfig`** | *how does dbt connect to the warehouse?* | `profile_name`, `target_name`, `profile_mapping` |
| **`ExecutionConfig`** | *how do we run dbt?* | `dbt_executable_path` (path to the `dbt` binary), `execution_mode` |
| **`RenderConfig`** | *which models, parsed how?* | `select`, `exclude`, `load_method` |

### ProfileConfig — the part that connects to BigQuery

You already have a `google_cloud_default` connection (Session 13). Cosmos can **generate dbt's `profiles.yml` from that Airflow connection** via a **profile mapping** — so you don't maintain warehouse credentials in two places. For BigQuery with a service-account key file:

```python
from cosmos.profiles import GoogleCloudServiceAccountFileProfileMapping

profile_config = ProfileConfig(
    profile_name="stackoverflow",
    target_name="dev",
    profile_mapping=GoogleCloudServiceAccountFileProfileMapping(
        conn_id="google_cloud_default",              # ← reuse the P1/S6 connection
        profile_args={"project": "your-gcp-project", "dataset": "so_marts"},
    ),
)
```

The mapping reads the connection at run time and writes a temporary `profiles.yml` for dbt — `project`/`dataset` in `profile_args` say *where your models get written*. Your models still *read from* `bigquery-public-data.stackoverflow` in their SQL; they *write to* the dataset named here. (Sibling mappings exist: `GoogleCloudServiceAccountDictProfileMapping` for a key stored inline in the connection, `GoogleCloudOauthProfileMapping` for OAuth.)

### RenderConfig — parsing method and model selection

`load_method` controls how Cosmos learns the graph — `LoadMode.DBT_LS` (default; runs `dbt ls`, needs dbt installed at parse time) or `LoadMode.DBT_MANIFEST` (reads a pre-compiled `target/manifest.json`, no dbt at parse time — faster, better for CI). `select`/`exclude` filter to a subset with dbt's own selector syntax: `["tag:nightly"]`, `["path:models/marts"]`, `["config.materialized:table"]`.

---

## 5. Complete runnable reference DAG (BigQuery via Cosmos)

**File:** `dags/stage-6-scale/s23/so_transforms_demo.py` · **dag_id:** `s23_so_transforms_demo`

One `DbtDag` that renders a dbt project transforming the Stack Overflow public data into a small marts layer in *your* dataset. Every model becomes its own task; every model's tests become their own task.

```python
# dags/stage-6-scale/s23/so_transforms_demo.py
from __future__ import annotations

import os

from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.constants import LoadMode
from cosmos.profiles import GoogleCloudServiceAccountFileProfileMapping
from pendulum import datetime

AIRFLOW_HOME = os.environ["AIRFLOW_HOME"]
DBT_PROJECT_PATH = f"{AIRFLOW_HOME}/include/dbt/stackoverflow"     # your dbt project
DBT_EXECUTABLE = f"{AIRFLOW_HOME}/dbt_venv/bin/dbt"               # the dbt binary

profile_config = ProfileConfig(
    profile_name="stackoverflow",
    target_name="dev",
    profile_mapping=GoogleCloudServiceAccountFileProfileMapping(
        conn_id="google_cloud_default",
        # models WRITE here; they READ from bigquery-public-data.stackoverflow
        profile_args={"project": os.environ["GCP_PROJECT"], "dataset": "so_marts"},
    ),
)

so_transforms_demo = DbtDag(
    dag_id="s23_so_transforms_demo",
    project_config=ProjectConfig(dbt_project_path=DBT_PROJECT_PATH),
    profile_config=profile_config,
    execution_config=ExecutionConfig(dbt_executable_path=DBT_EXECUTABLE),
    render_config=RenderConfig(                       # ← one task per selected model
        load_method=LoadMode.DBT_LS,
        select=["path:models/marts"],                # only the marts layer
    ),
    start_date=datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-23", "dbt", "cosmos"],
    default_args={"owner": "akhand", "retries": 1},  # passes the integrity gates
    operator_args={"install_deps": True},            # `dbt deps` before running
)
```

```bash
# needs: pip install astronomer-cosmos dbt-bigquery, and a dbt project at the path
python dags/stage-6-scale/s23/so_transforms_demo.py                 # parses; Cosmos runs `dbt ls`
airflow dags test s23_so_transforms_demo 2026-01-01   # each model runs as its own task
```

In the grid you'll see one task per model (each paired with its test task) wired in dbt's dependency order — retry a single failed model without touching the others. Every query your models run must be **cost-capped and column-scoped** (Session 13): no `SELECT *` on `posts_questions`/`posts_answers`; aggregate, and cap bytes in the dbt model config.

> **Scaffold note:** the two starter files (`dags/stage-6-scale/s23/s23_examples.py`, `s23_assignment.py`) are **plain `airflow.sdk` stubs** that do **not** import `cosmos` — so CI stays green even if `astronomer-cosmos` isn't installed. Type the real Cosmos code above into `s23_examples.py` only once you've `pip install`ed Cosmos and a dbt project exists.

---

## 6. Build spec — your challenge (no solution)

**File:** `dags/stage-6-scale/s23/s23_assignment.py` · **dag_id:** `s23_assignment`

Embed a dbt transform as **one stage** of a larger Airflow pipeline using `DbtTaskGroup`.

**The problem:**

- Build a DAG shaped **extract → dbt models → publish**: a leading `@task` that logs "sources ready", a **`DbtTaskGroup`** rendering a subset of your Stack Overflow dbt project, and a trailing `@task` that logs "marts published".
- The dbt group must connect to BigQuery via a `ProfileConfig` built from **`google_cloud_default`** (a profile mapping), never hard-coded credentials.
- Use `RenderConfig(select=...)` to render **only** a chosen layer (e.g. `["tag:daily"]` or `["path:models/marts"]`), not the whole project.

**Constraints:**

- Models **read** `bigquery-public-data.stackoverflow` and **write** to your own dataset; every model is cost-capped and column-scoped — no `SELECT *` (R16/R17).
- The wrapping DAG passes the integrity gates: `tags`, real `owner`, `retries >= 1`.
- `DbtTaskGroup` sits **between** the extract and publish tasks — the ordering must be real Airflow dependencies (`extract() >> group >> publish()`).

**Acceptance criteria:**

- `python dags/stage-6-scale/s23/s23_assignment.py` parses (with Cosmos + a dbt project installed).
- The grid shows the dbt models as **individual tasks inside a collapsible group**, wired in dbt's dependency order, between extract and publish.
- Failing one model and clearing it reruns **only** that model and its downstream — not the whole group.
- BigQuery Job history shows every model's query within its cap.

**One nudge (only if stuck):** `DbtTaskGroup` takes the *same* `project_config` / `profile_config` / `execution_config` as `DbtDag` — the only new thing is that you instantiate it *inside* your `@dag` function and place it in the dependency chain like any other task, instead of it being the whole DAG.

---

## 7. Production tip — the model-40 retry that reran everything (2am)

The page that defines this session, from the pre-Cosmos world: a nightly transform was one `BashOperator` running `dbt run` over 50 models. Model 40 — a mart joining `posts_questions` to `posts_answers` — hit a transient BigQuery quota error and failed. The task went red, Airflow's retry fired, and dbt started again **from model 1**, rebuilding the 39 healthy models a second time (real BigQuery bytes, real dollars), only to hopefully clear model 40. The on-call engineer couldn't tell from the single square *which* model broke without scrolling a 4,000-line log, and the "fix" burned the cost of the whole layer twice.

The habits that prevent it:

- **Per-model tasks make retries surgical.** With Cosmos, model 40 is its own Airflow task — it retries *alone*, and its downstream picks up after it, while the 39 healthy models stay green and untouched. The failure is a click, not a log-scroll, and the rerun costs one model, not fifty.
- **Parse from the manifest in CI, run `dbt ls` in prod — but pin the version.** `LoadMode.DBT_LS` runs dbt at *parse* time on every scheduler heartbeat; a slow project can make DAG parsing crawl. Switch to `LoadMode.DBT_MANIFEST` (a pre-compiled `manifest.json`) for fast, dbt-free parsing — just make sure the manifest is regenerated in your build, or Airflow renders a *stale* graph and silently drops your newest model.
- **The profile mapping is the single source of truth for credentials.** Building `ProfileConfig` from `google_cloud_default` means you rotate the key in *one* Airflow connection, not in a `profiles.yml` that drifts. A hand-maintained `profiles.yml` with a stale key is the other 2am page — the whole transform layer fails auth at once.

---

Sources:
[Cosmos — Getting Started (Astronomer)](https://astronomer.github.io/astronomer-cosmos/getting_started/index.html),
[Cosmos — dbt & Airflow concepts](https://astronomer.github.io/astronomer-cosmos/getting_started/dbt-airflow-concepts.html),
[Cosmos — Render Config](https://astronomer.github.io/astronomer-cosmos/configuration/render-config.html),
[Cosmos — Selecting & Excluding](https://astronomer.github.io/astronomer-cosmos/configuration/selecting-excluding.html),
[Cosmos — GoogleCloudServiceAccountFile profile](https://astronomer.github.io/astronomer-cosmos/profiles/GoogleCloudServiceAccountFile.html),
[Cosmos — Parsing Methods (LoadMode)](https://astronomer.github.io/astronomer-cosmos/configuration/parsing-methods.html),
[Airflow BigQuery operators & connection](https://airflow.apache.org/docs/apache-airflow-providers-google/stable/operators/cloud/bigquery.html)
