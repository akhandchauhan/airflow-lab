# Session 15 · The Universal Adapter

**Hooks in depth** — how a DAG talks to the outside world without ever holding a
password.

> ## 📟 Cold open — 09:14 Monday, the pipeline is red
> Your **Product Health** report also posts its daily summary to a reporting
> Postgres box. Whoever wrote that task hardcoded the DB password *and* the
> BigQuery project id straight into the DAG file. Over the weekend security
> **rotated** the credential. Monday morning every run 500s with an auth error —
> and worse, the *old* password is now sitting in your git history forever.
>
> **Today's session:** learn the one abstraction that makes this whole class of
> bug impossible. A **Hook** reads its credentials from a named **Connection** at
> run time, so secrets live outside your code, and a rotation is a one-line change
> in the UI — nothing to redeploy, nothing to leak.

Think of a hook as a **universal travel power adapter**. Your appliance (the
pipeline logic) never changes. You just plug in the right wall socket — the
**Connection** — for whatever country (system / environment) you're in. You reach
for the adapter by name; that name is the `conn_id`.

```
your @task  ──uses──▶  Hook  ──get_connection(conn_id)──▶  Connection  ──▶  live client
 (no creds)          (adapter)        (resolves secret)       (the socket)     (BigQuery/DB/API)
```

---

## 1. What a hook actually is

A **hook** is a thin wrapper around an external system's client SDK. It does two
jobs, and only two:

1. **Fetch credentials** for a named `conn_id` (it never hardcodes them).
2. **Expose tidy methods** — `get_first`, `get_records`, `load_file`, `run` — so
   your task never touches the raw SDK or a secret.

Two facts worth internalizing:

- **Operators are built out of hooks.** `BigQueryInsertJobOperator` uses
  `BigQueryHook` internally. When you write a `@task` that calls a hook directly
  (as you already did in Session 04), you're doing exactly what the operator does
  — with full control over the Python around it.
- **There are 300+ provider hooks.** Any time a task talks to something *outside*
  Airflow — a warehouse, object storage, a database, an HTTP API — there's almost
  certainly a hook for it. You reach for a hook, not the vendor SDK.

```python
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

@task
def count_questions() -> int:
    hook = BigQueryHook(gcp_conn_id="google_cloud_default", use_legacy_sql=False)   # ← THE MECHANIC: a hook, not a raw client
    row = hook.get_first("SELECT COUNT(*) FROM `bigquery-public-data.stackoverflow.posts_questions`")
    return int(row[0])
```

No project id, no key path, no password anywhere in the task — the hook pulls all
of that from the `google_cloud_default` Connection.

---

## 2. `get_connection` and the Connection object

Every hook, under the hood, makes one call: **`BaseHook.get_connection(conn_id)`**.
It's a classmethod that returns a **`Connection`** object — the resolved bundle of
credentials for that name.

```python
from airflow.hooks.base import BaseHook

@task
def inspect_conn() -> str:
    conn = BaseHook.get_connection("google_cloud_default")   # ← THE MECHANIC: name → resolved Connection
    project = conn.extra_dejson.get("project")               # GCP creds live in `extra`
    print(f"type={conn.conn_type} project={project}")
    return project
```

The `Connection` object exposes exactly these attributes — this is the whole
surface you ever read:

| Attribute          | What it holds                                                        |
| ------------------ | -------------------------------------------------------------------- |
| `conn_id`          | the name you looked up                                               |
| `conn_type`        | the kind of system (`google_cloud_platform`, `postgres`, `http`…)    |
| `host`             | hostname / endpoint                                                  |
| `schema`           | database / schema name                                               |
| `login`            | username                                                             |
| `password`         | secret (never print it)                                              |
| `port`             | port number                                                          |
| `extra`            | free-form **JSON string** for anything type-specific                 |
| `extra_dejson`     | that `extra` **parsed into a dict** — what you actually use          |
| `get_uri()`        | the whole connection rebuilt as one URI string                       |

For GCP there's no host/login/password — the key path and project sit in `extra`,
so you read `conn.extra_dejson["project"]` / `["key_path"]`. For a Postgres box
you'd use `host` / `login` / `password` / `schema` / `port`.

---

## 3. Where a connection comes from — the resolution order

This is the part that trips people up. A `conn_id` is **just a name**. Airflow
resolves it **at run time** by walking a search path and taking the **first
match**:

| Order | Source                        | How you set it                                              | Shows in UI? |
| ----- | ----------------------------- | ----------------------------------------------------------- | ------------ |
| 1     | **Secrets backend**           | Vault / GCP Secret Manager / AWS SSM (if configured)        | ❌            |
| 2     | **Environment variable**     | `AIRFLOW_CONN_<CONN_ID>` (URI or JSON)                      | ❌            |
| 3     | **Metastore DB**              | `airflow connections add …`, or the UI                      | ✅            |

Consequences that matter:

- **First match wins.** An `AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT` env var *shadows*
  the DB row of the same name — change the DB in the UI and nothing happens,
  because the env var is found first.
- **The UI only shows metastore connections.** A connection provided by an env var
  or a secrets backend is invisible in the UI — a classic "but I defined it!"
  confusion.
- This is exactly why P1 defined `google_cloud_default` with
  `airflow connections add` (source 3, metastore) — reliable and visible — and
  offered the `AIRFLOW_CONN_*` env var (source 2) as the alternative.

The env-var form (no DB row needed):

```bash
# name pattern: AIRFLOW_CONN_ + CONN_ID uppercased
export AIRFLOW_CONN_REPORTING_DB='postgresql://user:pass@db.internal:5432/reports'
```

**None of these three is your source code.** That's the whole point: rotate the
secret in the backend or the UI, and every DAG picks it up on the next run with no
redeploy.

---

## 4. Writing your own hook

You write a custom hook when a system has **no provider hook**, or to bottle up
domain queries you repeat across DAGs. The pattern is fixed:

```python
from airflow.hooks.base import BaseHook
from google.cloud import bigquery

class StackOverflowHook(BaseHook):                       # ← inherit BaseHook → get_connection() for free
    default_conn_name = "google_cloud_default"

    def __init__(self, conn_id: str = default_conn_name):
        super().__init__()                               # ← never skip this
        self.conn_id = conn_id
        self._client: bigquery.Client | None = None

    def get_conn(self) -> bigquery.Client:               # ← THE MECHANIC: Connection → cached live client
        if self._client is None:
            conn = self.get_connection(self.conn_id)     # resolves via backend → env → DB
            self._client = bigquery.Client(project=conn.extra_dejson.get("project"))
        return self._client

    def unanswered(self) -> int:                         # a domain method built on get_conn()
        sql = "SELECT COUNT(*) FROM `bigquery-public-data.stackoverflow.posts_questions` WHERE answer_count = 0"
        return next(self.get_conn().query(sql).result())[0]
```

Three rules baked into that shape:

- **`get_conn()` builds and caches the client**; every domain method goes through
  it — you never call `get_connection` twice.
- **Instantiate the hook inside the task, never at the top level or in the `@dag`
  body.** The constructor runs **every time Airflow parses the file** (every
  ~30 s) — build a hook there and you open a connection on every parse, hammering
  the external system. Hooks are cheap to create *at run time*, ruinous *at parse
  time*.
- **You rarely hand-roll a GCP/AWS hook** — the provider's `BigQueryHook` already
  wires credentials from the Connection's `key_path` (this toy skips that). Custom
  hooks earn their keep for systems *without* a provider, or for a house-style API
  wrapper.

---

## 5. Complete runnable reference DAG (BigQuery · Stack Overflow)

One DAG, three ways to use hooks: inspect a Connection, read a scalar, read many
rows — all on the real Stack Overflow tables, all cheap.

```python
# dags/s15/answer_rate_demo.py
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task
from airflow.hooks.base import BaseHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

CONN_ID = "google_cloud_default"
QUESTIONS = "bigquery-public-data.stackoverflow.posts_questions"
TAGS = "bigquery-public-data.stackoverflow.tags"


@dag(
    dag_id="s15_answer_rate_demo",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-15", "hooks", "stackoverflow"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():

    @task
    def show_connection() -> str:
        # BaseHook.get_connection resolves the Connection: backend → env → metastore
        conn = BaseHook.get_connection(CONN_ID)
        project = conn.extra_dejson.get("project")
        print(f"conn_id={conn.conn_id} type={conn.conn_type} project={project}")
        return project

    @task
    def answer_rate() -> float:
        # get_first runs the query and hands back ONE row as a tuple
        hook = BigQueryHook(gcp_conn_id=CONN_ID, use_legacy_sql=False, location="US")
        answered, total = hook.get_first(f"""
            SELECT COUNTIF(answer_count > 0) AS answered, COUNT(*) AS total
            FROM `{QUESTIONS}`
        """)
        rate = round(100 * answered / total, 2)
        print(f"answered={answered:,} of {total:,} → answer_rate={rate}%")
        return rate

    @task
    def top_tags() -> list[str]:
        # get_records returns MANY rows (a list of tuples); the tags table is tiny
        hook = BigQueryHook(gcp_conn_id=CONN_ID, use_legacy_sql=False, location="US")
        rows = hook.get_records(f"SELECT tag_name, count FROM `{TAGS}` ORDER BY count DESC LIMIT 5")
        for tag_name, cnt in rows:
            print(f"{tag_name:<12} {cnt:,}")
        return [r[0] for r in rows]

    show_connection() >> [answer_rate(), top_tags()]


pipeline()
```

Run it (needs the P1 BigQuery connection):

```bash
python dags/s15/answer_rate_demo.py
airflow dags test s15_answer_rate_demo 2026-01-01
```

You'll see the resolved project printed by `show_connection`, the answer-rate
percentage from `get_first` (one row), and the top-5 tags from `get_records`
(many rows). Check **BigQuery Job history**: `answer_rate` scans only the
`answer_count` column, `top_tags` scans the tiny `tags` table — both a few MB.

> **`get_first` / `get_records` are uncapped.** They're the right tool for small
> aggregate reads (one column, or a tiny dimension table). For anything that would
> scan a big table, drive the job through a **capped** `BigQueryInsertJobOperator`
> (`maximumBytesBilled`) — Session 04 — and read the result table, rather than
> pulling raw rows through the hook.

---

## 6. Your build (no solution)

**File:** `dags/s15/answer_rate.py` · **dag_id:** `s15_answer_rate`

Ship the real answer-rate layer of Product Health — driven entirely through hooks,
with no credential anywhere in the file.

**The job:**

- A task uses **`BaseHook.get_connection`** to resolve `google_cloud_default` and
  logs the `conn_type` and project (proves the DAG holds no secret itself).
- A task uses **`BigQueryHook.get_first`** to compute the **answer rate** —
  `answered / total` — on `posts_questions`, returning the percentage.
- A task uses **`BigQueryHook.get_records`** to pull a **small multi-row** result
  (e.g. top-N tags from the tiny `tags` table, or answer rate split by year) and
  logs it.
- A final `@task` logs a one-line summary built from the values above.

**Rules of engagement:**

- All external access via **hooks** — no raw `google.cloud.bigquery.Client`, no
  hardcoded project/key/password.
- Hooks instantiated **inside** tasks, never at module top level or in the `@dag`
  body.
- Keep `get_first`/`get_records` to aggregates or the tiny `tags` table; anything
  heavier goes through a **capped** `BigQueryInsertJobOperator`.
- Names clearly distinct (R12): `task_id` a noun, function a verb form, variable
  its role.
- Passes the integrity gates: `tags`, real `owner`, `retries >= 1`.

**Done when:**

- `python dags/s15/answer_rate.py` parses (prints nothing).
- `airflow dags test s15_answer_rate 2026-01-01` runs green.
- The connection task prints the resolved project — and `grep` finds **no**
  password/project literal in the file.
- **BigQuery Job history** shows every query scanning only what it needs.
- `python -m pytest tests/ -v` stays green.

---

## 7. War story — the tip that would've saved Monday

- **A credential in code is a credential in git forever.** The Monday incident —
  a hardcoded password that broke on rotation and leaked into history — dies the
  moment creds live in a **Connection** instead of the DAG. Rotation becomes a UI
  edit; the DAG file never changes, so there's nothing to leak. Rule: if you can
  `grep` a secret out of `dags/`, you have a production incident waiting.
- **A hook built at parse time is a self-inflicted DDoS.** Instantiate a hook at
  module top level and its constructor fires on *every* DAG parse — every ~30 s,
  for every worker — quietly opening connections to your database until it tips
  over. The fix is one indent: build the hook **inside** the task body. This is the
  #1 way a "harmless" refactor takes down a shared Postgres box at 2am.

---

## 8. Verify + commit

```bash
mkdir -p dags/s5
python dags/s15/answer_rate.py
airflow dags test s15_answer_rate 2026-01-01
python -m pytest tests/ -v
git add -A && git commit -m "session 05: hooks in depth (answer-rate layer)" && git push
```

Done when the DAG runs green, the connection task prints the project, and no secret
is grep-able in the file. Then update the scoreboard in `README.md`.

Sources:
[Connections & Hooks — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/connections.html),
[Secrets Backend (resolution order) — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/security/secrets/secrets-backend/index.html),
[What is a hook — Astronomer](https://www.astronomer.io/docs/learn/what-is-a-hook),
[Custom hooks & operators — Astronomer](https://www.astronomer.io/docs/learn/airflow-importing-custom-hooks-operators/)
