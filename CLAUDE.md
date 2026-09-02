# airflow-lab — authoring rules for the Airflow 3 weekend course

This repo is a learning course. Notes live in `docs/course/`; the DAGs the user
builds live in `dags/`. These rules govern how course **notes** (`.md`) and their
**reference DAGs** are written. They override default behavior.

## Audience

Strong data engineer (SQL, pandas, Python, GCP/BigQuery), **new to Airflow** and
its ecosystem libraries. Learns by understanding the machinery, not by memorizing
API surface. Skip beginner buildup; go deep on *why*.

## The five hard rules (never break)

1. **Deep conceptual theory, not shallow bullets.** Explain the underlying
   mechanism — parse-time vs run-time, what an object like `XComArg` actually is,
   why a design exists — before the API surface. The Concept section must be
   substantial.

2. **Explain every non-stdlib library the first time it appears.** What it is and
   why Airflow uses it (e.g. `pendulum`, `fsspec`, `celery`). Do not assume the
   user knows a tool. (They did not know `pendulum`.)

3. **One complete, runnable reference DAG BEFORE the build spec** — in the exact
   style the spec expects. Fragments/snippets are not enough. The build spec may
   only require *varying* something already shown end-to-end, never inventing a
   structure the user has never seen whole.

4. **The build spec is a PROBLEM STATEMENT, not a solution walkthrough.** State
   WHAT to build — requirements, structure in prose, constraints, acceptance
   criteria — and let the user design the HOW. Never give numbered
   "define task X returning Y, then wire `stage(run_checks(...))`, then loop with
   `.override(...)`" steps. Full code lives only in the reference (a *different*
   example), never in the spec.

5. **Function/variable name ≠ Airflow id string.** In every example keep the
   Python name distinct from the `dag_id` / `group_id` / `task_id` it carries.
   Naming both the same (e.g. `def ingest()` with `group_id="ingest"`) makes it
   impossible to tell what `ingest.override(...)` attaches to. Use e.g. function
   `load_source`, `group_id="src"`. `.override`/`.expand` are called on the Python
   **variable**; the id is the **string** passed in.

## Session ritual

- **Concept sessions:** concept → API + example → **complete runnable reference**
  → **build spec (problem statement)** → **production tip** → verify & push.
- **Practical sessions (🔷 P1, P2, …):** every 3 concept sessions, one practical
  applying all three on a real **BigQuery public dataset** (`bigquery-public-data.*`).
  Ritual: real dataset → setup → complete reference → build spec → run against
  BigQuery → **production tip** → verify (rows + bytes billed) → push.
- **Every session ends with ONE production tip** — concept *and* practical. (A
  practical once shipped without one; that was a miss.)

## Airflow conventions in this repo

- **Airflow 3 only.** No Airflow 2 comparisons. Public authoring API is
  `airflow.sdk` (`@dag`, `@task`, `@task_group`). Default to **TaskFlow** style
  unless a session is specifically about classic operators.
- **DAGs live in `dags/<session>/`** subfolders (e.g. `dags/task-3/`, `dags/p1/`).
  Airflow parses `dags/` recursively. Always give correct paths in notes.
- **Every DAG must pass the integrity gates** in `tests/dags/test_dag_integrity.py`:
  real `owner`, `retries >= 1`, non-empty `tags`. CI (`.github/workflows/ci.yml`)
  runs ruff (`E,F,AIR3`) + pytest on push; keep it green.
- Use `pendulum.datetime(..., tz=...)` for `start_date`; `catchup=False`,
  `schedule=None` for manual course DAGs.

## BigQuery / GCP rules (practicals)

- **Never commit the service-account key.** It stays outside the repo (`~/.gcp/`),
  git-ignored. A leaked key = someone billing your project.
- **Connection:** `google_cloud_default` must be **defined** (DB via
  `airflow connections add`, or `AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT`). Setting
  `GOOGLE_APPLICATION_CREDENTIALS` alone raises `AirflowNotFoundException` in
  Airflow 3 — the operator looks up the connection first.
- **Cost safety, always:** cap every query with `maximumBytesBilled`; no
  `SELECT *` on big tables; prefer `COUNT(*)`/aggregates (0 bytes); pin providers.
  **Never** create a Cloud Composer environment — the Codespace is the runtime.
- **Orchestrate, don't compute.** `BigQueryInsertJobOperator` is for side-effecting
  SQL; its XCom is the **job id**, not rows. Keep data in BigQuery; pull only small
  scalars back (via a `@task` + `BigQueryHook.get_first`) when orchestration must
  decide on a value. XCom is a control-signal store, not a data pipe.

## Note format

Follow the `Structured` output style: answer first, headers/tables over prose,
numbered concrete steps for procedures, no filler.
