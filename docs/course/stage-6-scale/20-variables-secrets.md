# Session 20 · Variables & Secrets backends

**Goal:** get configuration and credentials **out of your DAG code** and into a place Airflow can serve them from — safely. Two ideas to nail: (1) a **Variable** is Airflow's key-value store for *non-secret config* (a project id, a threshold, a table name) you want to change without editing code; (2) a **secrets backend** is the ordered lookup chain Airflow walks to resolve Variables *and* Connections — **secrets backend → environment variable → metadata DB** — so real secrets can live in Vault/Secret Manager and never touch your repo or the UI. The rule underneath everything: **secrets never go in code, params, or committed files.** This is a config/infra topic; the reference DAG reads a Variable and feeds it into a cost-capped BigQuery `COUNT` on the Stack Overflow spine. *(Stage 6 — scale & harden.)*

---

## 1. Why this session exists, and the running analogy

Every pipeline has two kinds of "stuff that isn't logic": **config** (which project, which dataset, how many days back, an alert threshold) and **secrets** (a service-account key, an API token, a DB password). Both are murder when hard-coded — config because you have to edit and redeploy code to change a number, secrets because a committed password is a breach, a leaked token, a 2am incident report. Airflow separates both concerns: **Variables** for config, **Connections + a secrets backend** for credentials.

**The running analogy: a hotel front desk.** Your DAG is a guest who needs a room key and the wifi password. The guest doesn't carry the master keyring (that's the secret) — they ask the **front desk** (Airflow's lookup chain). The desk checks, in order: the **hotel safe** (a secrets backend like Vault — most trusted), then a **note taped under the desk** (environment variables), then the **guest registry binder** (the metadata DB). The guest never sees where it came from — they just get the value. That ordered "safe → note → binder" walk is the entire secrets-backend mechanism, and §5 is nothing but that order.

---

## 2. Variables — the key-value config store

A **Variable** is a named value Airflow stores (by default, encrypted in the metadata DB with Fernet) and serves to your tasks at run time. Use it for anything you'd otherwise hard-code and later wish you hadn't: a project id, a dataset name, a lookback window, a feature flag.

```python
from airflow.sdk import Variable            # ← the public Airflow 3 import (not airflow.models)

project = Variable.get("gcp_project")                       # returns a string
lookback = int(Variable.get("lookback_days", default_var=7))  # default if key missing
cfg = Variable.get("so_pipeline_cfg", deserialize_json=True)  # parse a JSON value into a dict
Variable.set("lookback_days", 14)                          # write (rarely from a DAG; usually CLI/UI)
```

| Call | What it does |
|---|---|
| `Variable.get("k")` | fetch the value as a **string**; raises `KeyError` if missing and no default |
| `Variable.get("k", default_var=...)` | fetch, or return the default if the key isn't found |
| `Variable.get("k", deserialize_json=True)` | parse a JSON-typed value into a Python `dict`/`list` |
| `Variable.set("k", v)` / `Variable.delete("k")` | write / remove (prefer CLI `airflow variables set` or the UI) |

The mechanism worth internalising: **`Variable.get` is a lookup against the secrets chain (§5), not just a DB read.** The same call transparently returns a value from Vault, an env var, or the DB depending on what's configured — your code doesn't change.

**Parse-time trap:** don't call `Variable.get` at the *top level* of a DAG file. The scheduler parses every DAG file constantly, and a top-level `Variable.get` hits the metadata DB **on every parse** — a well-known way to hammer the DB and slow parsing. Read Variables **inside a task** (run time), or, if you must template one, use Jinja (§4), which is fetched lazily at render time.

---

## 3. Connections — where credentials actually belong

A **Connection** is Airflow's structured record for "how to reach an external system": host, login, password, port, and an `extra` JSON blob for the rest (a service-account key, a region). Operators and hooks look them up **by `conn_id`** — you already use `google_cloud_default` for BigQuery. The point for this session: a Connection is the *right home for a secret*, because Airflow encrypts it, masks it in logs, and — with a secrets backend — can keep it entirely outside the DB.

```python
from airflow.sdk import BaseHook          # resolve a connection object in a task
conn = BaseHook.get_connection("google_cloud_default")   # host/login/password/extra
```

You almost never read passwords by hand — you pass the `conn_id` to an operator/hook and let it fetch. The credential's value never appears in your code either way.

---

## 4. Jinja access — Variables and Connections in templated fields

Templated operator fields (SQL, `bash_command`, paths) can pull Variables and Connections **without any Python import**, resolved lazily when the task renders:

```python
"{{ var.value.gcp_project }}"            # a plain (string) Variable
"{{ var.json.so_pipeline_cfg.dataset }}" # a JSON Variable, then a key inside it
"{{ conn.google_cloud_default.host }}"   # a field off a Connection
```

| Jinja | Resolves to |
|---|---|
| `{{ var.value.<key> }}` | the string Variable `<key>` |
| `{{ var.json.<key> }}` | the JSON Variable `<key>`, parsed — index into it: `{{ var.json.<key>.field }}` |
| `{{ conn.<conn_id>.host }}` | a field (`host`, `login`, `password`, `extra_dejson.x`) of a Connection |

Prefer Jinja for values that go straight into a templated field — it's lazy (no parse-time DB hit) and keeps the value out of your Python entirely. Reach for `Variable.get` when you need the value in Python logic (a loop count, a branch condition).

---

## 5. The secrets backend and the search order (the core mechanism)

A **secrets backend** is a pluggable resolver Airflow consults *before* the DB for Variables and Connections. Configure one (HashiCorp Vault, AWS Secrets Manager, GCP Secret Manager, etc.) and Airflow walks a **fixed, ordered chain** for every `Variable.get`/connection lookup:

```
1. Secrets backend      (Vault / Secret Manager, if configured)   ← checked FIRST
2. Environment variables (AIRFLOW_VAR_* / AIRFLOW_CONN_*)          ← checked SECOND
3. Metadata database    (what the UI writes)                       ← checked LAST
```

**First match wins for reads.** If the same key exists in Vault and the DB, a read returns the **Vault** value. (Writes — `Variable.set`, the UI — always go to the **metadata DB**, so you can shadow a DB value with an env var or backend value that silently takes precedence — a real gotcha.)

**Environment-variable form** (the middle tier — great for local dev and CI, needs no DB):

```bash
export AIRFLOW_VAR_LOOKBACK_DAYS=14                 # → Variable.get("lookback_days")  (key is lower-cased)
export AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT='google-cloud-platform://...'   # → conn_id google_cloud_default
```

- `AIRFLOW_VAR_<KEY>` supplies a Variable; `AIRFLOW_CONN_<CONN_ID>` supplies a Connection (URI or JSON form).
- Env-var Variables/Connections **do not show in the UI** but are usable in DAGs, and they **take precedence over the DB** (tier 2 beats tier 3).

**Configuring a backend** (in `airflow.cfg` or env):

```ini
[secrets]
backend = airflow.providers.hashicorp.secrets.vault.VaultBackend
backend_kwargs = {"connections_path": "connections", "variables_path": "variables", "url": "https://vault:8200"}
```

The mechanism to remember: **the backend chain is why the same `Variable.get("x")` works identically in prod (from Vault), in CI (from an env var), and in your local UI (from the DB) with zero code changes.** That portability *is* the feature.

---

## 6. Why secrets never go in code, params, or committed files

- **Code / repo:** a password in a `.py` is in git history forever, readable by everyone with repo access, and scraped by bots the moment it hits a public remote. Even a private repo is the wrong trust boundary. (This is why R18 git-ignores the service-account key.)
- **`params` / DAG defaults:** DAG `params` and `default_args` are rendered into the DAG's serialized form and **shown in the UI / logs** — a secret there is a secret on a dashboard. `params` are for *user-supplied run config*, never credentials.
- **Where they go instead:** a Connection or a Variable, resolved through the secrets backend (§5). Airflow then **masks** known-sensitive values (password fields, keys whose name matches `password`/`secret`/`token`/`api_key`) in task logs automatically — but only if the value came through the Variable/Connection machinery, not if you `print()` a literal.

The one-line test: **if leaking this value is an incident, it goes in a Connection/secret backend — never in code, never in `params`, never in a committed file.**

---

## 7. Complete runnable DAG (your reference)

Read a Variable for the lookback window and the byte cap, then feed both into a **cost-capped** BigQuery `COUNT` over Stack Overflow questions. Config (the Variable) is separate from code; the credential is the `google_cloud_default` **Connection**, never a literal (R16/R17/R20).

```python
from __future__ import annotations

import pendulum
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.sdk import Variable, dag, task


@dag(
    dag_id="s20_variables_demo",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-20", "variables"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():

    @task
    def count_recent_questions() -> int:
        # Variables read at RUN TIME (never top-level) — config out of code:
        lookback = int(Variable.get("so_lookback_days", default_var=30))     # ← config, not a literal
        max_bytes = int(Variable.get("so_max_bytes_billed", default_var=100_000_000))  # ← cost cap
        sql = f"""
            SELECT COUNT(*) AS n
            FROM `bigquery-public-data.stackoverflow.posts_questions`
            WHERE creation_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {lookback} DAY)
        """
        hook = BigQueryHook(gcp_conn_id="google_cloud_default", use_legacy_sql=False)
        # cap the scan so a fat table can't run up a bill (R17):
        row = hook.get_first(sql, job_config={"maximumBytesBilled": max_bytes})
        n = int(row[0])
        print(f"questions in last {lookback} days: {n}")
        return n

    count_recent_questions()


pipeline()
```

```bash
python dags/stage-6-scale/s20/s20_examples.py
airflow variables set so_lookback_days 30
airflow variables set so_max_bytes_billed 100000000
airflow dags test s20_variables_demo 2026-01-01
```

`COUNT(*)` over a `WHERE` on a partition-friendly column keeps the scan tiny, and `maximumBytesBilled` (from a Variable, so ops can tune it without a code change) is the hard stop. Change the lookback with `airflow variables set so_lookback_days 7` and re-run — **the number changes, the code doesn't.** That is the whole point of Variables. Requires `google_cloud_default` defined (R19) and `apache-airflow-providers-google` pinned.

---

## 8. Build spec — your challenge (no solution)

**File:** `dags/stage-6-scale/s20/s20_assignment.py` · **dag_id:** `s20_assignment`

Build a **config-driven, cost-capped** question-tag report where every knob is a Variable.

**The problem:**

- Store a **JSON Variable** (e.g. `so_report_cfg`) holding `{"tag": "python", "lookback_days": 30, "max_bytes": 100000000}`.
- One `@task` reads it with `Variable.get(..., deserialize_json=True)` and runs a **capped** BigQuery `COUNT` of `posts_questions` where `tags LIKE '%<tag>%'` in the lookback window, using `google_cloud_default`.
- Prove the config path both ways: read one field via **Python** (`Variable.get`) and reference the same Variable once via **Jinja** (`{{ var.json.so_report_cfg.tag }}`) in a templated field or a logged string.

**Constraints:**

- Credentials come **only** from the `google_cloud_default` Connection — no keys, tokens, or paths in code or `params`.
- Every query capped with `maximumBytesBilled`; no `SELECT *`; use `COUNT`/aggregates.
- Read Variables **inside tasks / via Jinja**, never at DAG top level.
- Passes the integrity gates: `tags`, real `owner`, `retries >= 1`.

**Acceptance criteria:**

- `python dags/stage-6-scale/s20/s20_assignment.py` parses (prints nothing) with **no** `Variable.get` at import time.
- After `airflow variables set so_report_cfg '{"tag":"python","lookback_days":30,"max_bytes":100000000}'`, `airflow dags test s20_assignment 2026-01-01` prints a count.
- Changing the Variable's `tag` and re-running changes the result **without editing the DAG**.
- `python -m pytest tests/ -v` stays green.

**One nudge (only if stuck):** the JSON Variable is one key holding a dict — `Variable.get("so_report_cfg", deserialize_json=True)["tag"]` in Python, `{{ var.json.so_report_cfg.tag }}` in Jinja. If a top-level `Variable.get` slows parsing, move it inside the task.

---

## 9. Production tip — the env var that silently shadowed prod

The bug that pages you at 2am: someone set `AIRFLOW_VAR_SO_LOOKBACK_DAYS=1` on a worker weeks ago while debugging, and forgot it. Every night the report quietly used a **1-day** lookback instead of the 30-day value sitting right there in the UI — because **environment variables (tier 2) beat the metadata DB (tier 3)** in the search order (§5). The UI showed `30`. The DB held `30`. The report used `1`, and nobody could see why the numbers looked "low but not broken" for a month. It only surfaced when a stakeholder compared totals.

- **Know the order, or you'll debug a value the UI insists is correct.** Reads return the **first** hit: backend → env var → DB. If a Variable's effective value disagrees with the UI, something higher in the chain (an env var, a Vault key) is shadowing it. Check `airflow variables get <key>` and the worker's environment, not just the UI.
- **Secrets never in code, params, or committed files — no exceptions.** A password in `default_args` renders into the UI; a key in the repo is in git forever. Put it in a Connection or a real secrets backend, and let Airflow mask it in logs. If you must log a config value, log the *non-secret* Variable, never a credential.
- **Don't `Variable.get` at parse time.** A top-level call hits the DB on every scheduler parse; under a secrets backend it can also hammer Vault. Read inside tasks or via Jinja.

---

## 10. Verify + commit

```bash
python dags/stage-6-scale/s20/s20_assignment.py
airflow variables set so_report_cfg '{"tag":"python","lookback_days":30,"max_bytes":100000000}'
airflow dags test s20_assignment 2026-01-01
python -m pytest tests/ -v
git add -A && git commit -m "session 20: variables & secrets backends" && git push
```

Done when changing the Variable changes the result with no code edit, and no secret appears in code, `params`, or the repo. Tick Session 20 in `docs/course/README.md`.

**Pre-push habit:** `ruff check dags/ include/ tests/ --select E,F,AIR3 && python -m pytest tests/ -v`.

Sources:
[Managing Variables — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/howto/variable.html),
[Secrets Backend — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/security/secrets/secrets-backend/index.html),
[Managing Connections — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html),
[Variables & Connections in templates — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/variables.html),
[airflow.sdk API Reference (Task SDK)](https://airflow.apache.org/docs/task-sdk/stable/api.html)
