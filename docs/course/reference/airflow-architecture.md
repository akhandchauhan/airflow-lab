# Airflow 3 architecture — how the whole thing actually runs

**Goal:** understand Airflow as a *system of separate processes*, not a single program — what each component (Scheduler, DAG Processor, API Server, Triggerer, Executor, Workers, Metadata DB) actually does, how a single task flows through all of them, and the one big Airflow 3 change that reshaped everything: **task code no longer touches the database directly.** This is GCP-independent — it's how Airflow itself works, wherever it runs. Knowing this is what lets you debug "why is my task stuck" and answer architecture questions in interviews.

---

## 1. The big picture

Airflow is **not one process**. It's a handful of independent services that talk through a central **metadata database**. No component calls another's Python directly; they coordinate through rows in the DB (and, in Airflow 3, through an API).

```
   DAG files (a "bundle")
        │  parse + serialize
        ▼
  ┌──────────────┐     writes serialized DAGs      ┌────────────────────┐
  │ DAG PROCESSOR│ ──────────────────────────────▶ │  METADATA DATABASE │
  └──────────────┘                                  │  (Postgres/MySQL)  │
                                                     │  state of every    │
  ┌──────────────┐     reads serialized DAGs        │  dag / task / var  │
  │  SCHEDULER   │ ◀──────────────────────────────▶ └────────────────────┘
  │ (+ executor) │      writes task state                 ▲        ▲
  └──────┬───────┘                                        │        │
         │ "run this task"                                │ UI/API │ Task Execution API
         ▼                                                │        │ (state, XCom, heartbeat)
  ┌──────────────┐   spawns a subprocess per task   ┌─────┴────┐   │
  │   WORKER     │ ── (task code runs isolated) ─────│API SERVER│◀──┘
  └──────────────┘                                  │ UI+REST+ │
  ┌──────────────┐   handles deferred/waiting tasks │ Exec API │
  │  TRIGGERER   │                                  └──────────┘
  └──────────────┘
```

**Running analogy — an airport.** The **DAG Processor** is check-in, reading each flight's manifest (DAG file) and posting it to the board. The **Metadata DB** is the board — the single source of truth. The **Scheduler** is air-traffic control, reading the board and clearing flights (tasks) for departure. **Workers** are the ground crew; but crucially each flight is flown by a *separate, sealed cockpit* (a subprocess) that can only radio the tower on one channel — it can never walk into the control room and edit the board itself. The **Triggerer** manages holding patterns (tasks waiting on something). The **API Server** is the terminal — the public information desk (UI + REST) *and* the radio tower (the channel crews use to report status).

---

## 2. The components, one by one

### DAG Processor — parses your code (always separate in Airflow 3)
Reads DAG files from a **DAG bundle**, parses them, and **serializes** them into the metadata DB. In Airflow 3 this is **always its own process** — the scheduler never parses DAG files and never touches the bundle. **Why:** so the scheduler can never execute arbitrary DAG-author code (a security boundary). The scheduler only reads the already-serialized result.

### Metadata Database — the single source of truth
Postgres or MySQL. Holds the **state of every DAG, task instance, variable, connection, XCom, and the serialized DAGs**. Every other component coordinates through it; none of them share memory. If you understand one thing, understand this: *the DB is how the parts talk.*

### Scheduler — decides what runs, and when
Reads the **serialized DAGs** from the DB (it does **not** parse files), works out which task instances are ready (dependencies met, schedule due), and hands them to the **executor**. The scheduler is the heartbeat of Airflow.

### Executor — dispatches tasks (lives *inside* the scheduler)
Not a separate service — it's a **configuration of the scheduler process** (`LocalExecutor`, `CeleryExecutor`, `KubernetesExecutor`, `EdgeExecutor`). It decides *where* a ready task actually runs: in a local subprocess, on a Celery worker, or in a fresh Kubernetes pod. (Covered in depth in Session 21.)

### Workers — run the task code (in isolation)
The process that actually executes a task. **Airflow 3's key move:** a worker **never runs your task code in its own process** — for every task instance it starts a **new subprocess**, supervises it, and tears it down when done. The worker holds the task's **short-lived credentials** and talks to the Execution API; the subprocess running *your* code never reaches the metadata DB. In a basic setup (LocalExecutor) the "worker" is part of the scheduler; in distributed setups it's separate machines.

### Triggerer — waits efficiently (optional)
Runs **deferred tasks** in a single **asyncio** event loop. When a deferrable operator or sensor is "waiting" (for a file, a time, an external event), it hands off to the triggerer and **frees its worker slot** — thousands of waits run in one triggerer process instead of one blocked worker each. Only needed if you use deferrable operators (Session 19).

### API Server — the UI, the REST API, *and* the task radio (new in Airflow 3)
**Replaces the old webserver.** It serves: (1) the **web UI**, (2) the **REST API** (trigger runs, clear tasks, read state), and (3) the **Task Execution API** — the channel tasks use to report back. The UI's "Code" tab reads the DAG source **from the metadata DB**, not the bundle — the API server has no access to your DAG files.

---

## 3. The one change that reshaped Airflow 3: tasks don't touch the DB

In Airflow 2, running task code connected **directly** to the metadata database to read variables/connections and write state. That coupled every worker to the DB and was a security hole.

**Airflow 3 severs that.** All runtime interaction — state transitions, heartbeats, XCom push/pull, fetching variables/connections — goes through the **Task Execution API** (served by the API server), mediated by the worker. The task subprocess gets a **short-lived token**, not DB credentials.

```
AIRFLOW 2                          AIRFLOW 3
task code ──▶ metadata DB          task code ──▶ Task SDK ──▶ Task Execution API ──▶ (DB)
(direct connection, full creds)    (no DB access, short-lived token, via the worker)
```

Why you should care:
- **Security:** DAG-author code can't read or corrupt the whole metadata DB.
- **Remote/edge execution:** because tasks only need an API endpoint + token (not DB access), workers can run anywhere — a different network, an edge device.
- **Debugging:** "task can't reach the DB" is now *by design* — task problems show up as Execution-API calls, not raw SQL.

---

## 4. Trace of a single run (end to end)

1. You push a DAG file into the **bundle** (a folder or Git repo).
2. The **DAG Processor** parses it and writes the **serialized DAG** into the metadata DB.
3. The **Scheduler** reads the serialized DAG, sees a run is due, creates task instances, and marks the first one ready.
4. The **Executor** (inside the scheduler) dispatches that task to a **worker** (local subprocess / Celery / a new K8s pod).
5. The **worker** starts a **fresh subprocess** for the task, hands it a short-lived token, and supervises it.
6. The task code runs; to read a Variable or push an XCom it calls the **Task Execution API** (via the Task SDK) — never the DB directly.
7. If the task **defers** (waits on something), it's handed to the **Triggerer**, freeing the worker; when the trigger fires, it's rescheduled.
8. Final state (success/failed) is written back through the API to the **metadata DB**.
9. You open the **API Server** UI, which reads that state (and the DAG code) from the DB.

---

## 5. DAG bundles & serialized DAGs

- **DAG bundle** = the storage the DAG Processor reads from. Default is a **local folder**; **versioned backends like Git** are supported. With a versioned bundle, the **scheduler pins a specific bundle version when it dispatches each task**, so a task always runs against the code version its run started with (this is what DAG versioning in Session 22 builds on).
- **Serialized DAG** = the parsed DAG stored as JSON in the DB (`serialized_dag` table). The scheduler, API server, and triggerer all read *this*, never the raw `.py`. That's why a syntax error in a DAG shows up in the **DAG Processor** logs, not the scheduler.

---

## 6. Deployment models

| Model | What it looks like | When |
|---|---|---|
| **Basic** | one machine, `LocalExecutor`; scheduler + DAG processor + API server + workers coexist | dev, learning (your Codespace) |
| **Distributed** | components spread across machines with distinct roles (Deployment Manager, DAG author, Ops); Celery/K8s workers | production at scale |
| **Separate DAG Processor** | the DAG processor always runs as its own service — **mandatory in Airflow 3** | every Airflow 3 deployment |

In distributed setups the API server has **no** access to the DAG bundles — the UI's Code tab reads from the DB. This is the same security boundary as the scheduler.

---

## 7. What runs where (process cheat-sheet)

| Component | Separate process? | Needed always? | Talks to |
|---|---|---|---|
| DAG Processor | ✅ always (Airflow 3) | yes | bundle (read) → DB (write) |
| Metadata DB | ✅ (Postgres/MySQL) | yes | everyone |
| Scheduler (+executor) | ✅ | yes | DB (read serialized DAGs, write state) |
| Workers | separate in distributed; in-scheduler for LocalExecutor | yes | Execution API; spawn task subprocesses |
| Triggerer | ✅ | only if you use deferrable ops | Execution API |
| API Server | ✅ | yes (UI/REST + Execution API) | DB (read); serves task radio |

---

## 8. Why this matters for you

- **Debugging:** "DAG not showing up" → DAG Processor problem. "Task stuck in queued" → scheduler/executor. "Task failed instantly with no logs" → worker/subprocess. "UI shows old code" → serialized DAG not refreshed. Knowing the map tells you *which log to open*.
- **Interviews (FAANG):** "walk me through what happens from saving a DAG file to a task finishing" is a standard data-platform question. Section 4 is your answer.
- **Scale:** every scaling lever (Session 21 executors/concurrency, Session 22 versioning, Session 19 triggerer) is a knob on one of these components. You can't tune what you can't name.

Sources:
[Architecture Overview — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/overview.html),
[Task Execution API / Task SDK — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/task-sdk.html),
[Upgrading to Airflow 3 (what changed)](https://airflow.apache.org/docs/apache-airflow/stable/installation/upgrading_to_airflow3.html),
[Airflow Security Model](https://airflow.apache.org/docs/apache-airflow/stable/security/security_model.html)
