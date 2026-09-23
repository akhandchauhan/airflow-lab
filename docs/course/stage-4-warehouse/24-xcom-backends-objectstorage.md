# Session 24 · Custom XCom Backends & ObjectStorage

**Goal:** stop cramming large data through XCom's metadata-DB storage and learn the two clean ways out — a **custom XCom backend** that transparently spills big values to object storage, and **`ObjectStoragePath`** for deliberately passing a *reference* (a path) instead of the payload itself. The one idea to get right: XCom is a **messaging channel, not a data store**, and by default every message you send is a row in Airflow's operational database. Understand *why that row is expensive*, and both fixes below become obvious rather than magic.

---

## 1. Why the metadata DB is the wrong place for large XCom

Every `return` from a `@task`, and every `ti.xcom_push`, writes a row into the `xcom` table of Airflow's **metadata database** — the same Postgres/MySQL instance that stores every DAG run, every task instance, every scheduler heartbeat. That database is the beating heart of the scheduler: it is read and written thousands of times a minute to decide what runs next. It was sized and tuned to be a fast **operational** store of small control-plane records, not a **data lake**.

Now picture what a large XCom does to it. A task pulls a 200 MB dataframe, serializes it, and returns it. Airflow writes 200 MB into a single `xcom` row. Concretely, this hurts in four compounding ways, and the reason each one bites is worth internalizing rather than memorizing:

1. **Row-size limits reject it outright.** The `value` column is a bounded type; a payload past that ceiling raises a database error and the task fails. You did not choose a size limit — the DB did, and it is small.
2. **Every read drags the whole blob across the wire.** The scheduler and workers connect to that DB over the network. A fat XCom row means large result sets streamed repeatedly — the DB does not know you only wanted a filename out of that JSON.
3. **It bloats backups, replication, and vacuum.** Operational DBs are backed up and replicated constantly. Your 200 MB is now copied on every backup and every replica, forever, until the run is purged.
4. **It competes with scheduling for the same connections and buffers.** The pool the scheduler needs to make timely decisions is the pool your blob is saturating. Slow XCom I/O shows up as *scheduler lag* — the most confusing symptom, because it looks like Airflow is slow, not like your data is too big.

**The running analogy for the whole session:** XCom is the **office mail system**. It is built for envelopes — a note saying "batch 4213 is ready, it's in warehouse aisle 12." It is emphatically *not* built for shipping the pallet itself through the internal mail. When you stuff a pallet into the envelope slot, you jam the mail room that everyone else depends on. The two fixes below are (a) a mail room that **automatically diverts anything heavier than an envelope to the loading dock** (custom backend), and (b) a discipline of **only ever mailing the aisle number, never the pallet** (`ObjectStoragePath`).

> **The rule that makes everything else make sense:** XCom moves *pointers and small scalars* between tasks. The moment a value is "big" (rule of thumb: past a few hundred KB), the payload belongs in object storage and only its *location* belongs in XCom. The two mechanisms in this session are just two ways to enforce that rule.

---

## 2. What a custom XCom backend actually is

An **XCom backend** is the pluggable class that decides *how a pushed value is turned into bytes for storage and back*. Airflow ships one default and lets you swap it wholesale for the entire deployment. It is not per-task and not per-DAG — it is a single deployment-wide setting, because XCom is a shared protocol and both the producing and consuming sides must agree on how to read a value back.

You select the backend with **one config key**, `xcom_backend`, in the `[core]` section:

```ini
[core]
xcom_backend = my_company.xcom.ObjectStorageXComBackend
```

or, equivalently, the environment variable form Airflow uses for every config key:

```bash
AIRFLOW__CORE__XCOM_BACKEND=my_company.xcom.ObjectStorageXComBackend
```

The value is the **dotted import path to your class**, which must be importable on the scheduler, the workers, *and* the API server. The default value is the built-in backend that stores everything in the metadata DB. Point this key at your own class and *every* XCom in the deployment flows through your code — which is exactly why the interesting pattern is "small values stay in the DB, big values get diverted to object storage," handled inside that one class so no DAG author has to think about it.

---

## 3. Writing the backend: subclass `BaseXCom`, override two methods

The base class lives in Airflow core:

```python
from airflow.models.xcom import BaseXCom          # ← the class you subclass
```

You override exactly two methods (a third, `purge`, is optional):

- **`serialize_value`** — called on *push*. It receives the Python value (plus context like `key`, `dag_id`, `task_id`, `run_id`, `map_index`) and must return something the DB column can hold. In a spill-to-storage backend, this is where you write the big payload to object storage and return the *path string* to be stored in the row.
- **`deserialize_value`** — called on *pull*. It receives the stored XCom record and must reconstruct the original Python value. In a spill-to-storage backend, this reads the path back out of the record, fetches the object, and returns the real data — so the DAG author gets their dataframe back and never knew it took a detour.
- **`purge`** *(optional)* — called when Airflow deletes an XCom (run cleanup, `clear`). Override it so deleting the XCom row also deletes the object you wrote, otherwise you leak files forever.

Here is the shape of a backend that diverts anything above a threshold to object storage and delegates small values to the default DB behavior. `serialize_value`/`deserialize_value` are **static methods** — Airflow calls them on the class, not an instance:

```python
import uuid
from airflow.models.xcom import BaseXCom
from airflow.sdk import ObjectStoragePath

BASE = ObjectStoragePath("s3://xcom-overflow@my-bucket/xcoms")
THRESHOLD = 100_000          # bytes; anything bigger spills to object storage


class ObjectStorageXComBackend(BaseXCom):
    @staticmethod
    def serialize_value(value, **kwargs):                 # ← THE MECHANIC: push-side hook
        data = BaseXCom.serialize_value(value, **kwargs)  # default JSON/bytes encoding
        if len(data) < THRESHOLD:
            return data                                   # small → stays in the DB row
        target = BASE / f"{uuid.uuid4()}.bin"             # big → write to the loading dock
        target.write_bytes(data)
        return BaseXCom.serialize_value({"xcom_ref": str(target)})  # DB row holds the path

    @staticmethod
    def deserialize_value(result):                        # ← pull-side hook
        raw = BaseXCom.deserialize_value(result)
        if isinstance(raw, dict) and "xcom_ref" in raw:
            return ObjectStoragePath(raw["xcom_ref"]).read_bytes()
        return raw
```

The beauty of this pattern is that **no DAG changes.** Authors keep writing `return df`; the backend silently decides envelope-vs-pallet. That is the difference between a backend (deployment-wide, transparent) and the `ObjectStoragePath` discipline in §5 (per-DAG, explicit).

> **Verify the backend is actually loaded** — a misconfigured import path fails *silently* back to the default. In a worker/scheduler container:
> ```python
> from airflow.sdk.execution_time.xcom import XCom
> print(XCom.__name__)          # should print YOUR class name, not the default
> ```

---

## 4. Why `airflow.io` and `ObjectStoragePath` exist

Before the second fix, the library it is built on. **`airflow.io`** is Airflow's built-in abstraction over object storage, layered on top of **`fsspec`** — a well-established Python library that gives one uniform filesystem API over dozens of backends (local disk, S3, GCS, Azure, HTTP). Airflow uses `fsspec` so that the same code path works whether your bytes land on `s3://`, `gs://`, or a local `file://` path; you write against one interface and the scheme in the URI picks the implementation. This is why you install `apache-airflow-providers-amazon` or `...-google` for the cloud filesystems but need nothing extra for `file://`.

**`ObjectStoragePath`** is the public, `pathlib`-style handle on top of that. It is exported from the SDK:

```python
from airflow.sdk import ObjectStoragePath      # ← the public API you use in DAGs
```

You construct it from a URI, and the part before `@` is the **Airflow connection id** that supplies credentials:

```python
base = ObjectStoragePath("s3://aws_default@my-bucket/staging")
# equivalent, connection passed explicitly:
base = ObjectStoragePath("s3://my-bucket/staging", conn_id="aws_default")
```

It behaves like `pathlib.Path`: `base / "subdir" / "file.csv"` to navigate, `.open()`, `.read_bytes()`/`.write_bytes()`, `.iterdir()`, `.is_file()`. The point is that reading and writing multi-gigabyte objects goes **task → object storage directly**, never through the metadata DB.

---

## 5. Passing large data by reference (the `ObjectStoragePath` discipline)

This is the explicit, per-DAG version of §3's automatic backend. Instead of returning the *data* from a task, you write the data to object storage and **return the `ObjectStoragePath`**. Because a path is a tiny string, *that* travels through XCom perfectly — it is exactly the "aisle number, not the pallet" envelope. `ObjectStoragePath` is XCom-serializable, so you can return it straight from one task and accept it as an argument in the next:

```python
@task
def stage(rows: list[dict]) -> ObjectStoragePath:
    target = ObjectStoragePath("file:///tmp/so/questions.json")
    target.write_bytes(json.dumps(rows).encode())     # ← pallet goes to the dock
    return target                                     # ← only the path rides XCom

@task
def summarize(path: ObjectStoragePath) -> int:
    rows = json.loads(path.read_bytes())              # consumer fetches by reference
    return len(rows)

summarize(stage(...))
```

The metadata DB now holds a ~40-byte string, no matter how large the dataset is. The two approaches compose: use the **backend** when you cannot or do not want to touch DAG code (transparent, deployment-wide), and use the **`ObjectStoragePath` discipline** when you are writing the DAG anyway and want the data flow to be explicit and auditable.

| Approach | Where it lives | DAG author aware? | Best when |
|---|---|---|---|
| **Custom XCom backend** | one deployment config key | No — transparent | you want *all* XCom safe without touching DAGs |
| **`ObjectStoragePath` by reference** | inside each DAG | Yes — explicit | you're writing the pipeline and want visible data flow |

---

## 6. Complete runnable reference DAG

On the Stack Overflow spine. It runs a **cost-capped** BigQuery aggregate via `google_cloud_default` (only a small scalar comes back to Python, per orchestrate-don't-compute), then stages a payload to an `ObjectStoragePath` and passes only the **path** downstream — demonstrating the reference pattern end to end. It uses a local `file://` path so it runs with no cloud-storage connection; swap the URI for `s3://…@bucket/…` or `gs://…@bucket/…` in production.

```python
# dags/stage-4-warehouse/s24/xcom_objectstorage_demo.py
from __future__ import annotations

import json

import pendulum
from airflow.sdk import ObjectStoragePath, dag, task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

CONN = "google_cloud_default"
QUESTIONS = "bigquery-public-data.stackoverflow.posts_questions"
CAP = "2000000000"                                   # 2 GB max bytes billed
STAGE = ObjectStoragePath("file:///tmp/so_stage")    # swap for s3://conn@bucket/... in prod


@dag(
    dag_id="s24_xcom_objectstorage_demo",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["session-24", "xcom", "objectstorage"],
    default_args={"owner": "akhand", "retries": 1},
)
def pipeline():

    @task
    def fetch_metric() -> int:
        # orchestrate, don't compute: a capped COUNT returns one small scalar
        hook = BigQueryHook(gcp_conn_id=CONN, use_legacy_sql=False, location="US")
        sql = f"SELECT COUNT(*) FROM `{QUESTIONS}` WHERE answer_count = 0"
        return int(hook.get_first(sql)[0])

    @task
    def stage_payload(unanswered: int) -> ObjectStoragePath:
        # the "pallet" — write the data to object storage, NOT to XCom
        target = STAGE / "metric.json"
        target.write_bytes(json.dumps({"unanswered": unanswered}).encode())
        return target                                # ← only this path rides XCom

    @task
    def publish(path: ObjectStoragePath) -> None:
        # the consumer fetches by reference; the DB never held the payload
        payload = json.loads(path.read_bytes())
        print(f"unanswered questions = {payload['unanswered']:,} (read from {path})")

    publish(stage_payload(fetch_metric()))


pipeline()
```

```bash
python dags/stage-4-warehouse/s24/xcom_objectstorage_demo.py
airflow dags test s24_xcom_objectstorage_demo 2026-01-01
```

Check the `xcom` table afterward: the `stage_payload` XCom holds a short `file:///tmp/so_stage/metric.json` string, not the JSON body — proof the payload bypassed the metadata DB.

---

## 7. Build spec — your challenge (no solution)

**File:** `dags/stage-4-warehouse/s24/xcom_staging.py` (scaffold: `s24_assignment.py`) · **dag_id:** `s24_xcom_staging`

Build a pipeline that moves a **non-trivial** result set out of the metadata DB and passes it by reference.

**The problem:**

- A task runs a **cost-capped** BigQuery query on the Stack Overflow spine that returns *more than one scalar* — e.g. the top 20 tags by question count, or per-year question counts (still small in bytes, but a real result set, not a single number).
- Instead of returning those rows through XCom, the task writes them to an **`ObjectStoragePath`** and returns the **path**.
- A downstream task accepts the path, reads the object back, and logs a derived figure (e.g. the leading tag, or the busiest year).

**Constraints:**

- Every BigQuery query carries `maximumBytesBilled`; no `SELECT *`; `"useLegacySql": False`.
- The only thing that crosses XCom is the `ObjectStoragePath` (or its string) — never the row data.
- Use a `file://` path so it runs locally; leave a comment showing the `s3://`/`gs://` swap.
- Passes the integrity gates: non-empty `tags`, real `owner`, `retries >= 1`.

**Acceptance criteria:**

- `python dags/stage-4-warehouse/s24/xcom_staging.py` parses (prints nothing).
- `airflow dags test s24_xcom_staging 2026-01-01` runs green.
- Inspecting the `xcom` table shows the staging XCom is a short path string, not the row payload.
- `python -m pytest tests/ -v` stays green.

**One nudge (only if stuck):** `ObjectStoragePath` is itself XCom-serializable — you can `return` it from one `@task` and take it as a typed argument in the next; you do not write any serialization code yourself for the *reference* path (that machinery is only needed inside a full custom *backend*, §3).

---

## 8. Production tip — the 2am page: "why is the scheduler crawling?"

The bug that pages you: a dashboard DAG starts timing out, then the *whole scheduler* goes sluggish — task instances sit in `queued` for minutes, unrelated DAGs lag. Nothing changed in the scheduler. What changed is that a well-meaning `@task` started `return`-ing a pandas dataframe that grew, week over week, from a few thousand rows to a few million. Every run now writes tens of MB into the `xcom` table and reads it back several times; that traffic is saturating the exact DB connections the scheduler needs to make decisions. The failure does not look like "my data is too big" — it looks like "Airflow is broken," which is why it eats hours at 2am.

- **The habit that prevents it:** treat XCom as an envelope, always. The instant a task returns a collection instead of a scalar or a small dict, it should be writing to object storage and returning an `ObjectStoragePath` — or the deployment should run a spill-to-storage **custom backend** so the mistake can't reach the DB in the first place.
- **Cap it structurally, not by discipline alone.** A custom backend with a size threshold (§3) turns "please remember not to send pallets" into "the mail room physically cannot accept a pallet." Discipline fails at 2am; a threshold doesn't.
- **When you add a backend, override `purge`.** Otherwise the DB stays healthy but object storage fills with orphaned XCom blobs no cleanup ever touches — a slower, quieter version of the same incident three months later.

---

Sources:
[XComs — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/xcoms.html),
[Object Storage — Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/objectstorage.html),
[ObjectStoragePath API — airflow.sdk](https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/sdk/index.html),
[BaseXCom — airflow.models.xcom](https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/models/xcom/index.html),
[Configuration reference — core.xcom_backend](https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html#xcom-backend)
