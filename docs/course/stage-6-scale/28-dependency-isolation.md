# Session 28 · Dependency isolation

**Goal:** learn how to run a task that needs a **different set of Python packages** than the Airflow process itself — without letting that task's dependencies poison the scheduler, the API server, or every other DAG. One idea to nail: an Airflow deployment is **one shared Python environment**, and every `@task` that runs in-process imports from that same `site-packages`; the moment two tasks demand incompatible versions of the same library, one of them loses. The fix is to move the conflicting task into its **own** Python environment — either built fresh per run (`PythonVirtualenvOperator` / `@task.virtualenv`) or pointed at a prebuilt interpreter (`ExternalPythonOperator` / `@task.external_python`). This is an infra/isolation topic, so the reference DAG is a **plain DAG** — it demonstrates the two isolation mechanics without BigQuery, which would only add cost and hide the point (flagged per R16). *(Stage 6 — scale & harden.)*

---

## 1. Why a shared environment breaks, and the running analogy

Everything you've written so far runs **in the same Python process family** as Airflow: the scheduler parses your DAG file, and a TaskFlow `@task` executes its callable inside a worker that already has Airflow, its providers, and every pinned library loaded. That's efficient — until two teams disagree about a version. Team A's model task needs `pandas==1.5` and an old `numpy`; team B's just added a transform that needs `pandas==2.2`. There is exactly **one** `pandas` in that environment. Whoever `pip install`s last wins, the other team's task starts throwing `AttributeError` or `ImportError` at 2am, and nobody changed *their* code. Worse: Airflow itself pins libraries (its own `pydantic`, `sqlalchemy`, `protobuf`), so a task that force-upgrades one of those can break the **scheduler**, not just a task.

**The running analogy: a shared office kitchen.** The Airflow environment is one fridge everyone shares. It works until two people need the same shelf for incompatible things — one needs it at 2°C for a science experiment, one needs it at 8°C for cheese. You can't set the shared fridge to both. Dependency isolation is **giving the conflicting job its own mini-fridge**: either a disposable cooler you pack fresh each time it runs (`virtualenv`), or a second full-size fridge already plugged in in the next room that you just walk over to (`external_python`). Same food (your code), separate cold chain (its dependencies) — so neither job can spoil the other's.

The rule underneath everything below: **the conflict is real and unavoidable in one interpreter; isolation means giving the task a second interpreter.** The two operators differ only in *when and where* that second interpreter is built.

---

## 2. What actually happens to your function (the mechanism)

Both isolation operators do something surprising that you must internalise or your code won't run: **they do not run your function in the current process.** They serialise the callable, hand it to a *separate* Python interpreter as a standalone script, run it there, and serialise the return value back. The documentation is blunt about it: *"The Python function body defined to be executed is cut out of the Dag into a temporary file w/o surrounding code."* Three consequences fall straight out of that, and each one bites a beginner:

1. **Every import must live inside the callable.** The function is ripped out of your DAG file with none of the surrounding code, so a top-level `import pandas` at the top of the DAG is invisible to it. Import *inside* the function body.
2. **You cannot close over module-level variables.** No referencing a global constant defined outside the function — it isn't copied. Pass what you need as arguments.
3. **Arguments and return values cross a serialization boundary.** They get pickled (by default `cloudpickle`) to move between the two interpreters. Return small, plain data (a number, a dict, a list) — not a live DB connection or a giant DataFrame.

There's a second, sharper limit on **Airflow context**: *"Airflow does not support serializing `var`, `ti` / `task_instance`"* across that boundary. So inside a venv task you don't get the full context object. If you need datetime context like `data_interval_start`, the isolated environment must itself contain `pendulum` and `lazy_object_proxy` (add them to `requirements`). The safe habit: compute what you need in a normal upstream task, and pass the plain value in.

---

## 3. `PythonVirtualenvOperator` / `@task.virtualenv` — build a venv per run

**Import:** `from airflow.providers.standard.operators.python import PythonVirtualenvOperator` — or the TaskFlow form `@task.virtualenv`. (This lives in the **`apache-airflow-providers-standard`** provider, which ships the built-in Python/Bash operators; it's installed with core Airflow 3, nothing extra to add.)

The mechanism, quoting the docs: *"Setup of virtual environments is made per task execution in a temporary directory. After execution the virtual environment is deleted again."* So every run: create a temp venv → `pip install` your `requirements` into it → run your function in it → tear it down. That's the disposable cooler packed fresh each time.

Key constructor args (same names on the decorator):

| Arg | What it does |
|---|---|
| `requirements` | list of pins (`["scikit-learn==1.4.2"]`) **or** a path to a `requirements.txt`. This is the isolated dep set. |
| `python_version` | Python version for the venv (e.g. `"3.11"`); lets a task run on a different interpreter version. |
| `system_site_packages` | if `True`, the venv can see the base env's packages too (fewer installs, less isolation); `False` for a clean room. |
| `index_urls` | custom package index(es). Pass `index_urls=[]` to forbid remote calls and force use of a cache. |
| `pip_install_options` | extra flags handed to `pip`. |
| `venv_cache_path` | reuse built venvs across runs instead of rebuilding — the main speed lever (see §5). |
| `serializer` | how args/returns cross the boundary: `"pickle"`, `"dill"`, or `"cloudpickle"` (default). |

```python
from airflow.sdk import task

@task.virtualenv(requirements=["scikit-learn==1.4.2"], system_site_packages=False)  # ← THE MECHANIC: fresh venv, pinned dep
def score(rows: list[dict]) -> float:
    from sklearn.linear_model import LinearRegression   # import INSIDE — the body is cut out of the DAG
    import numpy as np
    x = np.array([[r["x"]] for r in rows]); y = np.array([r["y"] for r in rows])
    return float(LinearRegression().fit(x, y).coef_[0])  # return a plain scalar across the boundary
```

**Reach for it when:** the dep set changes per task or you can't pre-bake environments (ad-hoc, many different one-off requirements). **Cost:** a `pip install` on every single run — slow, and it hits your package index each time unless cached.

---

## 4. `ExternalPythonOperator` / `@task.external_python` — point at a prebuilt interpreter

**Import:** `from airflow.providers.standard.operators.python import ExternalPythonOperator` — or `@task.external_python`. Same provider.

Here **nothing is built at run time.** You've already created a virtualenv (at image-build time, in your Dockerfile or on the host), and you just tell the operator where its interpreter is. The critical arg, from the docs: `python` *"should point to the python binary inside the virtual environment (usually in `bin` subdirectory of the virtual environment)."* That's the second full-size fridge already plugged in next door — you walk over, you don't build it.

```python
from airflow.sdk import task

@task.external_python(python="/opt/venvs/ml/bin/python")   # ← THE MECHANIC: prebuilt interpreter, zero install at run time
def score(rows: list[dict]) -> float:
    from sklearn.linear_model import LinearRegression
    import numpy as np
    x = np.array([[r["x"]] for r in rows]); y = np.array([r["y"] for r in rows])
    return float(LinearRegression().fit(x, y).coef_[0])
```

The prebuilt env must already contain everything the function imports; if you rely on datetime context, it also needs `pendulum` and `lazy_object_proxy`, and if you use `dill` serialization its `dill` version must match Airflow's. **Reach for it when:** the dep set is stable and reused across many runs — you pay the build cost **once** (at deploy) instead of every run.

---

## 5. The trade-off, straight

| | `@task.virtualenv` (build per run) | `@task.external_python` (prebuilt) |
|---|---|---|
| When the env is built | **every run**, in a temp dir, then deleted | **once**, ahead of time (image/host) |
| Per-run latency | slow — a full `pip install` each time | fast — interpreter already exists |
| Network at run time | hits your package index (unless cached) | none |
| Flexibility | change `requirements` freely, per task | fixed to what you pre-baked |
| Best for | ad-hoc / frequently-changing deps | stable deps reused across many runs |

Two ways to blunt `virtualenv`'s cost without switching operators: set `venv_cache_path` so built environments are **reused** across runs instead of rebuilt, and pass `index_urls=[]` to forbid remote index calls (forcing the cache). But if a dep set is stable, the honest answer is: **stop rebuilding it and use `external_python`.** The decision tree: deps stable and reused → `external_python`; deps ad-hoc or per-task → `virtualenv`; deps so heavy or system-level (a C toolchain, a GPU driver) that a venv can't express them → that's Session 29's container task, not this.

---

## 6. When isolation is the wrong tool

Isolation is not free and not always the answer. If **only one** version of a library is ever needed across the whole deployment, just install it in the base environment — no operator, no boundary, no serialization tax. If the task needs **non-Python** system libraries, a specific OS, or a totally different runtime, a venv can't give you that; you want a container (Session 29). And if you find yourself putting *most* tasks behind `virtualenv`, that's a smell that your base environment is wrong — fix the base, don't wrap everything. Isolation is a scalpel for the genuinely conflicting task, not a default wrapper.

---

## 7. Complete runnable DAG (your reference)

A plain DAG showing **both** mechanics side by side: a `@task.virtualenv` that installs a pinned dep in a throwaway venv, and a `@task.external_python` that would use a prebuilt interpreter — plus a normal task that consumes the scalar each returns, proving the serialization boundary works. No BigQuery: this topic is about *where a task's dependencies live*, and a warehouse call adds cost without teaching isolation (flagged per R16).

> The `external_python` task points at `/opt/venvs/demo/bin/python`. Create that venv once before running: `python -m venv /opt/venvs/demo && /opt/venvs/demo/bin/pip install "colorama==0.4.6"`. On Windows adjust to `\\Scripts\\python.exe`. If you can't make a prebuilt venv, comment that task out — the `virtualenv` task alone still demonstrates isolation.

```python
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s28_isolation_demo",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-28", "isolation"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():

    @task.virtualenv(requirements=["colorama==0.4.6"], system_site_packages=False)  # ← fresh venv per run, pinned dep
    def in_fresh_venv() -> str:
        import colorama                                   # import INSIDE — body is cut out of the DAG file
        return colorama.__version__                       # return a plain string across the boundary

    @task.external_python(python="/opt/venvs/demo/bin/python")  # ← prebuilt interpreter, no install at run time
    def in_prebuilt_venv() -> str:
        import colorama
        return colorama.__version__

    @task
    def compare(fresh: str, prebuilt: str) -> None:       # normal in-process task consumes both scalars
        print(f"colorama in fresh venv={fresh}, in prebuilt venv={prebuilt}")
        assert fresh == "0.4.6"                           # the pinned version really came from the isolated env

    compare(in_fresh_venv(), in_prebuilt_venv())


pipeline()
```

```bash
python -m venv /opt/venvs/demo && /opt/venvs/demo/bin/pip install "colorama==0.4.6"
python dags/stage-6-scale/s28/s28_examples.py
airflow dags test s28_isolation_demo 2026-01-01
```

Watch the logs: the `virtualenv` task spends its first seconds doing a `pip install colorama==0.4.6` into a temp dir, then reports the version; the `external_python` task reports instantly because its env already existed. That timing difference **is** the trade-off from §5, live.

---

## 8. Build spec — your challenge (no solution)

**File:** `dags/stage-6-scale/s28/s28_assignment.py` · **dag_id:** `s28_assignment`

Isolate a **genuinely conflicting** dependency so a task can use a version the base Airflow env does not have.

**The problem:**

- Pick a library where you can pin a version that differs from what the base environment ships (e.g. an old `colorama`, or a `pandas` pin that differs from core's).
- Put the work in a **`@task.virtualenv`** (or `@task.external_python` if you pre-bake the env) that pins that version in its **own** environment, does a small computation, and **returns a plain scalar** (a number or short string).
- A downstream **plain `@task`** consumes that scalar and `assert`s it — proving the value crossed the serialization boundary intact and came from the isolated version, not the base one.

**Constraints:**

- All imports the isolated callable needs live **inside** the callable — never at DAG top level (§2).
- The isolated task returns only JSON/pickle-friendly data; no live objects, no giant DataFrames.
- Plain TaskFlow, no BigQuery.
- Passes the integrity gates: non-empty `tags`, real `owner`, `retries >= 1`.

**Acceptance criteria:**

- `python dags/stage-6-scale/s28/s28_assignment.py` parses (prints nothing).
- `airflow dags test s28_assignment 2026-01-01` runs; the isolated task's logs show the pinned version being installed/used, distinct from the base env's version.
- The downstream `assert` passes, confirming the returned scalar is the isolated version's result.
- `python -m pytest tests/ -v` stays green.

**One nudge (only if stuck):** if you get `ModuleNotFoundError` for a library you clearly listed in `requirements`, you almost certainly imported it at the top of the DAG file instead of inside the function body — move the import in.

---

## 9. Production tip — the task that broke because someone fixed the base image

The bug that pages you at 2am: a `@task.virtualenv` model task that ran clean for months suddenly fails with a cryptic `ImportError` deep in a transitive dependency. Nobody touched the task. What happened: it had `requirements=["scikit-learn==1.4.2"]` but relied on `numpy` **leaking in from the base env** because `system_site_packages` defaulted loose — and a platform engineer bumped `numpy` in the base image that night. The task's pinned `scikit-learn` was compiled against the old `numpy` ABI; the new one broke it. The isolation was **fake**: half the deps came from the shared fridge after all.

- **Pin the whole isolated set, not just the headline package.** If a task needs `scikit-learn`, pin `numpy`/`scipy` too, and set `system_site_packages=False` so nothing leaks in from the base. Partial isolation is worse than none — it looks isolated and isn't.
- **Cache, don't rebuild blindly.** A `virtualenv` task that reinstalls from PyPI every run will one day install a *new* patch release that breaks you. Use `venv_cache_path` and pin exact versions so the cached env is reproducible.
- **If deps never change, stop building them per run.** A stable dep set behind `@task.virtualenv` is paying a `pip install` tax on every run for no benefit — and re-rolling the dice on transitive versions each time. Bake it once into a prebuilt env and use `@task.external_python`: faster *and* frozen.

---

## 10. Verify + commit

```bash
python -m venv /opt/venvs/demo && /opt/venvs/demo/bin/pip install "colorama==0.4.6"
python dags/stage-6-scale/s28/s28_examples.py
airflow dags test s28_isolation_demo 2026-01-01
python dags/stage-6-scale/s28/s28_assignment.py
python -m pytest tests/ -v
```

Done when the `virtualenv` task visibly installs and uses a pinned version distinct from the base env, and the downstream `assert` on the returned scalar passes. Tick Session 28 in `docs/course/README.md`.

**Pre-push habit:** `ruff check dags/ include/ tests/ --select E,F,AIR3 && python -m pytest tests/ -v`.

Sources:
[PythonOperator / virtualenv / external_python — apache-airflow-providers-standard](https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/operators/python.html),
[Best Practices (isolation, serialization) — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html),
[Run tasks in an isolated environment — Astronomer](https://www.astronomer.io/docs/learn/airflow-isolated-environments)
