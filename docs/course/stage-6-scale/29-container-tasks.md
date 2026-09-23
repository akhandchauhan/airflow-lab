# Session 29 · Container tasks

**Goal:** run a task as a **whole container or pod** — its own OS, its own runtime, its own everything — instead of as Python inside the Airflow worker. One idea to nail: `virtualenv` (Session 28) isolates *Python packages* but still runs inside Airflow's process on Airflow's OS; a **container task** isolates the *entire environment* — a task can be Go, R, a CUDA image, or a vendor tool that has nothing to do with Python, and Airflow's only job is "start this image, wait, collect the exit code." The two operators are `DockerOperator` (run a container on a Docker host) and `KubernetesPodOperator` (run a pod in a Kubernetes cluster). Because these providers may not be installed in this lab's CI, the **scaffolds stay plain `airflow.sdk` stubs** — the real operator code lives only in this note (per the assignment's CI rule). Concept/infra topic, so a plain reference, no BigQuery (flagged per R16). *(Stage 6 — scale & harden.)*

---

## 1. Why a container, not just a venv, and the running analogy

Session 28 gave a task its own Python packages. But some tasks need more than packages: a system library (`libgdal`, `ffmpeg`), a specific OS, a GPU driver, a non-Python binary, or an image a vendor ships that you don't control. A virtualenv can't express any of that — it's still your Airflow worker's OS and your Airflow worker's Python. When the isolation you need is **the whole machine**, you run the task as a container.

**The running analogy — Session 28 was the mini-fridge; this is a food truck.** A venv is a separate shelf in your kitchen: your kitchen, your stove, just a different ingredient set. A container task is **wiring in a whole self-contained food truck** — its own kitchen, its own power, its own recipes — that pulls up, cooks one dish, and drives off. Airflow isn't cooking anymore; it's the dispatcher that tells the truck "go," waits for it to finish, and records whether the dish came out. The truck could be a sushi kitchen or a taco kitchen (any language, any OS) and your dispatcher doesn't care.

The mechanism that makes both operators tick: **Airflow does not run your code — it runs an image and watches the exit code.** Exit `0` = success, non-zero = failed task. Everything the task actually *does* is baked into the image; Airflow orchestrates, it doesn't compute. That's the same "orchestrate, don't compute" rule you've seen for BigQuery jobs, taken to its logical end.

---

## 2. What these providers actually are (R6)

Neither ships with core Airflow — each is a **separate provider package** you install, and each depends on an external system being reachable.

- **`apache-airflow-providers-docker`** — gives you `DockerOperator`. It talks to a **Docker daemon** (the background service that builds and runs containers on a host) over a socket (`unix://var/run/docker.sock`) or TCP. *Docker* is the container runtime; the daemon is the thing that pulls images and starts containers. Airflow needs network access to that daemon — usually on the same host or a remote Docker host.
- **`apache-airflow-providers-cncf-kubernetes`** — gives you `KubernetesPodOperator` (KPO). *Kubernetes* is the container **orchestrator**: a cluster that schedules containers (grouped as *pods*) across many machines, handles restarts, resources, and networking. The KPO asks the cluster's API server to create one pod, streams its logs, and cleans it up. `CNCF` is the Cloud Native Computing Foundation, which stewards Kubernetes — hence the package name.

You install these at deploy time (`pip install apache-airflow-providers-docker` / `...-cncf-kubernetes`, pinned per R17), and your DAG imports the operator. **In this lab they may be absent**, which is exactly why the scaffolds don't import them — an import of a missing provider would fail parsing and turn CI red.

---

## 3. `DockerOperator` — run a container on a Docker host

**Import:** `from airflow.providers.docker.operators.docker import DockerOperator`.

It pulls an image, starts a container with your command, waits, and maps the exit code to task success/failure. Key constructor args:

| Arg | What it does |
|---|---|
| `image` | required — the image to run (`"python:3.12-slim"`); defaults to the `latest` tag if you omit one. |
| `command` | the command run inside the container (str or list; templated). |
| `docker_url` | where the Docker daemon is; defaults to `DOCKER_HOST` env or `unix://var/run/docker.sock`. |
| `auto_remove` | `"never"` (default), `"success"`, or `"force"` — whether to delete the container after it exits. |
| `network_mode` | `"bridge"`, `"host"`, `"none"`, or a named network. |
| `mounts` | list of volumes to bind into the container. |
| `mount_tmp_dir` | bind-mount a host temp dir into the container (default `True`). |
| `environment` | env vars for the container (templated). |

```python
from airflow.providers.docker.operators.docker import DockerOperator

run_transform = DockerOperator(          # ← THE MECHANIC: Airflow starts an image, not Python
    task_id="run_transform",
    image="ghcr.io/acme/so-transform:1.4.2",   # your prebuilt image (pin the tag, R17)
    command=["python", "/app/transform.py", "--date", "{{ ds }}"],
    docker_url="unix://var/run/docker.sock",
    auto_remove="success",               # clean up the container when it exits 0
    network_mode="bridge",
)
```

**Reach for it when:** you have a single Docker host (or a small setup) and a task that needs its own image but not a cluster. It's the lightest way to run "a task as a container."

---

## 4. `KubernetesPodOperator` — run a pod in a cluster

**Import:** `from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator`.

Same idea, at cluster scale: it asks Kubernetes to create **one pod** running your image, streams the logs back, and tears the pod down. This is the elastic, isolated, per-task-resources option — each task gets a clean pod, and the cluster handles placement and scaling. Key args:

| Arg | What it does |
|---|---|
| `image` | container image the pod runs. |
| `cmds` / `arguments` | entrypoint command and its arguments. |
| `name` | pod name (a random suffix is added). |
| `namespace` | Kubernetes namespace to launch in (defaults to current or `"default"`). |
| `get_logs` | stream the pod's logs into the Airflow task log (`True`). |
| `on_finish_action` | pod cleanup: `"delete_pod"`, `"delete_succeeded_pod"`, `"keep_pod"`, etc. (replaces the old `is_delete_operator_pod`). |
| `in_cluster` | `True` when Airflow itself runs inside the same cluster (uses the in-cluster service account). |
| `do_xcom_push` | if `True`, the pod writes XCom by writing `/airflow/xcom/return.json`. |

```python
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

run_model = KubernetesPodOperator(       # ← THE MECHANIC: one pod per task, cluster-scheduled
    task_id="run_model",
    name="so-model",
    namespace="ml",
    image="ghcr.io/acme/so-model:2.0.1",
    cmds=["python", "/app/score.py"],
    arguments=["--date", "{{ ds }}"],
    get_logs=True,
    on_finish_action="delete_pod",       # don't leave dead pods lying around
    in_cluster=True,
)
```

**Reach for it when:** you already run on Kubernetes, or you need per-task resource requests (CPU/GPU/memory), clean per-task isolation, and elastic scale that a single Docker host can't give.

---

## 5. The deferrable KPO — stop holding a worker slot while the pod runs

A normal KPO **occupies an Airflow worker slot for the entire pod lifetime** — if the pod runs for two hours, a worker sits there for two hours doing nothing but waiting. That's the same waste you saw with poll-based sensors in Session 19, and the fix is the same: **deferrable mode.** Set `deferrable=True` and the operator, once the pod is launched, hands off to the **triggerer** (Airflow's async component) and **releases the worker slot**; the triggerer watches the pod with async polling and only wakes a worker back up when the pod finishes.

```python
run_model = KubernetesPodOperator(
    task_id="run_model",
    name="so-model",
    image="ghcr.io/acme/so-model:2.0.1",
    cmds=["python", "/app/score.py"],
    deferrable=True,                     # ← THE MECHANIC: free the worker slot; the triggerer watches the pod
    on_finish_action="delete_pod",
)
```

On a cluster running dozens of long container tasks, this is the difference between needing a large worker pool and a small one — the workers are freed the instant each pod is airborne. (`DockerOperator` has no deferrable equivalent; the pattern belongs to the cluster-scale KPO.)

---

## 6. When to reach for containers vs virtualenv (the decision that matters)

| Need | Use |
|---|---|
| One conflicting **Python** dependency set | `@task.virtualenv` / `@task.external_python` (Session 28) — lightest |
| **Non-Python** runtime, system libs, GPU, a vendor image, a specific OS | container task |
| A single host, no cluster | `DockerOperator` |
| A Kubernetes cluster, per-task resources, elastic scale | `KubernetesPodOperator` (deferrable for long runs) |

The honest rule: **containers are heavier than venvs — reach for the lightest tool that solves the actual isolation you need.** If the only problem is Python versions, a venv is faster to build, cheaper to run, and simpler to debug. Move up to a container only when the isolation you need is bigger than Python: a different OS, a system dependency, or an image you don't build yourself. Every container task adds an image to build, pin, scan, and pull — real operational weight. Don't pay it for a problem a venv already solves.

---

## 7. Complete runnable DAG (your reference)

Because the `docker` and `cncf.kubernetes` providers may not be installed here, the **runnable** reference is a plain DAG that models the *shape* of a container pipeline — a normal task standing in for each container step — so it parses and runs green everywhere. The **real** operator code is in §3–5 above: to run it for real, install the provider (`pip install apache-airflow-providers-docker`), point `docker_url` at a reachable daemon, and swap the stub for the `DockerOperator` from §3. No BigQuery: this is about *where a task runs*, not warehouse work (flagged per R16).

```python
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s29_container_demo",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-29", "containers"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():

    @task
    def prep() -> str:                                   # normal in-process task: prepares the run
        return "2026-01-01"

    @task
    def run_container_stub(run_date: str) -> None:       # ← stands in for DockerOperator/KPO so CI stays green
        # In prod this is a DockerOperator(image=..., command=[...]) or KubernetesPodOperator(...).
        # Airflow would launch the image, wait, and map the exit code to success — see §3–5.
        print(f"[stub] would launch container image for {run_date}; exit 0 == task success")

    run_container_stub(prep())


pipeline()
```

```bash
python dags/stage-6-scale/s29/s29_examples.py
airflow dags test s29_container_demo 2026-01-01
```

The stub is deliberate: it keeps the lab CI-green with no provider installed, while §3–5 give you the exact operator to drop in the moment a Docker daemon or a cluster is available. Swapping the stub for a real `DockerOperator` changes nothing about the DAG's structure — that's the point of "orchestrate, don't compute."

---

## 8. Build spec — your challenge (no solution)

**File:** `dags/stage-6-scale/s29/s29_assignment.py` · **dag_id:** `s29_assignment`

Model a pipeline whose middle step runs **as a container**.

**The problem:**

- Build a three-task DAG: a `prep` task that produces a small input (e.g. a date string), a **container step**, and a `finish` task that runs after it.
- If the `docker` / `cncf.kubernetes` provider is installed in your environment: make the middle step a real `DockerOperator` (or `KubernetesPodOperator`) running a public image (e.g. `python:3.12-slim`) with a `command` that echoes the input and exits `0`.
- If the provider is **not** installed (CI): keep the middle step a plain `@task` stub that *documents* the operator it stands for — the file must stay parseable and CI-green with no provider import.
- Wire `prep → container step → finish`.

**Constraints:**

- The committed file must **parse and pass CI without any provider installed** — so the version you commit uses the stub, and the real operator lives in a comment or a locally-run variant.
- Airflow orchestrates only: the container's work is in its image/command, not in Python in the DAG.
- Plain DAG, no BigQuery.
- Passes the integrity gates: non-empty `tags`, real `owner`, `retries >= 1`.

**Acceptance criteria:**

- `python dags/stage-6-scale/s29/s29_assignment.py` parses (prints nothing), with **no** `docker`/`kubernetes` import at module top level.
- `airflow dags test s29_assignment 2026-01-01` runs the three tasks in order.
- The DAG clearly documents (comment or docstring) the real `DockerOperator`/`KPO` call the stub replaces, including `image` and `command`.
- `python -m pytest tests/ -v` stays green.

**One nudge (only if stuck):** the CI trap is a top-level `from airflow.providers.docker...` import when the provider isn't installed — that fails at parse time and reddens every test. Keep the real import out of the committed file; describe it in a comment instead.

---

## 9. Production tip — the container task that "hung" and burned the worker pool

The bug that pages you at 2am: a fleet of `KubernetesPodOperator` model tasks all show `running` for hours, new DAG runs pile up `queued`, and the on-call thinks the cluster is wedged. The cluster is fine. The DAGs were written with plain (non-deferrable) KPOs, each pod runs ~90 minutes, and **each one pins an Airflow worker slot for its whole life** just to wait. A nightly burst of 40 model runs asked for 40 simultaneous worker slots that don't exist, so everything after the pool limit sat `queued` — the exact funnel from Session 21, filled by *waiting*, not by *working*.

- **Long container tasks must be deferrable.** Set `deferrable=True` on the KPO so the pod launches, the worker slot is released, and the triggerer does the waiting. A worker should never sit idle babysitting a pod for an hour.
- **Always set `on_finish_action` to delete succeeded pods.** A KPO left on `keep_pod` slowly fills the namespace with dead pods until the cluster refuses to schedule new ones — a different 2am page, same root cause: cleanup you forgot.
- **Pin the image tag, never `latest`.** A container task on `image=...:latest` runs *whatever got pushed last night* — the moment someone pushes a broken build, your pipeline runs it with no code change on your side. Pin the tag (R17) so a run is reproducible and a bad image is an explicit choice, not an accident.

---

## 10. Verify + commit

```bash
python dags/stage-6-scale/s29/s29_examples.py
airflow dags test s29_container_demo 2026-01-01
python dags/stage-6-scale/s29/s29_assignment.py
python -m pytest tests/ -v
```

Done when both DAGs parse and run with **no** provider installed, and the assignment clearly documents the real container operator its stub replaces. Tick Session 29 in `docs/course/README.md`.

**Pre-push habit:** `ruff check dags/ include/ tests/ --select E,F,AIR3 && python -m pytest tests/ -v`.

Sources:
[DockerOperator — apache-airflow-providers-docker](https://airflow.apache.org/docs/apache-airflow-providers-docker/stable/_api/airflow/providers/docker/operators/docker/index.html),
[KubernetesPodOperator — apache-airflow-providers-cncf-kubernetes](https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/stable/operators.html),
[Deferrable operators — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/deferring.html)
