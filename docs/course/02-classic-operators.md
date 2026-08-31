# Session 02 - W1 Sun - Classic Operators & Dependency Helpers

**Goal:** build a DAG the *classic* way - instantiate operator objects and wire
them explicitly - and understand what an Operator actually is, how `>>` builds
edges, and when `chain`, `chain_linear`, and `cross_downstream` beat writing
arrows by hand.

Session 01 wired the graph *implicitly* through data (TaskFlow). This session
wires it *explicitly* through operators. Same DAG concept, opposite mechanism.

---

## 1. What is an Operator? (the concept people skip)

An **Operator is a Python class that is a template for one unit of work.** It is
not the work itself and not a running task - it is a blueprint. Airflow ships
many operator classes; each knows how to do one kind of thing:

- `BashOperator` - run a shell command
- `PythonOperator` - call a Python function
- `EmptyOperator` - do nothing (a structural placeholder)
- `SQLExecuteQueryOperator`, `KubernetesPodOperator`, ... - one per integration

Three words that are easy to confuse - keep them straight:

| Term | What it is | Example |
|---|---|---|
| **Operator** | the class (template) | `BashOperator` |
| **Task** | one *instance* of an operator inside a DAG | `copy_file = BashOperator(...)` |
| **Task Instance** | one *run* of that task for a specific DAG run | `copy_file` on 2026-01-01 |

So: instantiate an **Operator** -> you get a **Task** (a node in the DAG). When
the DAG runs, that task becomes a **Task Instance** (see Session 01's parse-time
vs run-time split - the same idea).

```python
from airflow.providers.standard.operators.bash import BashOperator

copy_file = BashOperator(          # instantiating the operator class...
    task_id="copy_file",           # ...creates a TASK named copy_file in the DAG
    bash_command="echo copying",
)
```

`BashOperator` lives in `apache-airflow-providers-standard` - in Airflow 3 the
basic operators moved out of core into the `standard` provider. Import path:
`airflow.providers.standard.operators.<bash|python|empty>`.

---

## 2. `BaseOperator` - the parent of every operator

Every operator class inherits from **`BaseOperator`** (`from airflow.sdk import
BaseOperator`). That is where the arguments common to *all* tasks come from -
you can pass these to any operator:

| Arg | Meaning |
|---|---|
| `task_id` | unique name within the DAG (required) |
| `retries`, `retry_delay` | retry-on-failure behavior |
| `trigger_rule` | when this task runs relative to upstream (Session 06) |
| `pool`, `priority_weight` | concurrency control (Session 13) |
| `execution_timeout` | kill the task if it runs too long |
| `depends_on_past` | only run if the previous run's same task succeeded |

`default_args` on the DAG is just a dict of these that Airflow applies to every
operator in the DAG - which is why setting `retries` there covers all tasks.

---

## 3. `>>` and `<<` - how the arrows actually work

In classic style you set dependencies with the **bitshift operators**:

```python
copy_file >> transform >> load        # copy_file, then transform, then load
```

This is not magic syntax - `BaseOperator` **overloads** Python's `>>` operator.
Normally `>>` is a bitwise right-shift on integers (`8 >> 1 == 4`). Airflow
redefines it on operators so that `a >> b` calls `a.set_downstream(b)` and
returns `b` (so the chain continues). Four equivalent ways to say "a before b":

```python
a >> b
a.set_downstream(b)
b << a
b.set_upstream(a)
```

Because `a >> b` returns `b`, you can chain: `a >> b >> c`. Lists fan out and
fan in:

```python
start >> [api_a, api_b, api_c]        # fan-out: start before all three
[api_a, api_b, api_c] >> merge        # fan-in: all three before merge
```

`>>` only ever creates **edges** (dependencies). It never passes data - that is
XCom's job. This is the key contrast with TaskFlow: there, passing a value drew
the edge for you; here, you draw the edge yourself and move data separately (via
XCom) if needed.

---

## 4. `chain()` - wire many tasks without a wall of arrows

`from airflow.sdk import chain`. `chain` turns a sequence of tasks (and lists of
tasks) into a dependency chain, so you avoid long `>>` ladders.

```python
chain(t1, t2, t3)                 # same as: t1 >> t2 >> t3
```

With lists, **consecutive lists are paired element-wise** (they must be the same
length):

```python
chain(t1, [t2, t3], [t4, t5], t6)
# t1 >> t2 ; t1 >> t3
# t2 >> t4 ; t3 >> t5      <-- element-wise: t2->t4, t3->t5 (NOT cross)
# t4 >> t6 ; t5 >> t6
```

A scalar next to a list "broadcasts" to every element; two lists pair up
position-by-position. If the lists differ in length, `chain` raises an error -
that is your signal you wanted `cross_downstream` instead.

---

## 5. `cross_downstream()` - every-to-every between two groups

`from airflow.sdk import cross_downstream`. Connects **every** task in the first
list to **every** task in the second (a full cross product):

```python
cross_downstream([a, b], [c, d])
# a >> c ; a >> d ; b >> c ; b >> d
```

Use it when a set of upstream tasks must all complete before any of a set of
downstream tasks. Note: `cross_downstream` returns `None` - you cannot keep
chaining off it, so it is usually a standalone statement.

---

## 6. `chain_linear()` - cross product across many groups, chainable

`from airflow.sdk import chain_linear`. Like `chain`, but between consecutive
groups it does the **full cross product** (every element of one group to every
element of the next), not element-wise pairing - and unlike `cross_downstream`
it accepts more than two groups:

```python
chain_linear([a, b], [c, d], [e])
# a >> c ; a >> d ; b >> c ; b >> d      (group1 x group2)
# c >> e ; d >> e                        (group2 x group3)
```

Rule of thumb:
- **`chain`** - element-wise pairing between equal-length lists.
- **`cross_downstream`** - full cross product, exactly two groups, terminal.
- **`chain_linear`** - full cross product, any number of groups.

---

## 7. `EmptyOperator` - a task that does nothing on purpose

`from airflow.providers.standard.operators.empty import EmptyOperator`. It runs
no code, but the scheduler still treats it as a real task. Uses:

- **start / end markers** - a single anchor everything hangs off.
- **fan-in / fan-out join points** - collapse many edges into one.

```python
start = EmptyOperator(task_id="start")
end   = EmptyOperator(task_id="end")
start >> [job_a, job_b, job_c] >> end
```

Without `end`, you'd draw three edges into whatever came next; with it, one.

---

## 8. Classic vs TaskFlow - when to use which

| | TaskFlow (`@task`) | Classic (operators) |
|---|---|---|
| Dependency | implicit - passing data draws the edge | explicit - you write `>>` / `chain` |
| Data passing | automatic via return values | manual via XCom |
| Best for | Python logic that moves values | non-Python work (Bash/SQL/containers), or operators with no decorator form |
| Reads like | function composition | a wiring diagram |

They mix freely in one DAG. Rule of thumb: **TaskFlow for Python data flow;
classic operators for pre-built integrations** (a `BashOperator`, a
`KubernetesPodOperator`, a SQL operator) where there's no value to thread.

---

## 9. Build spec - you write this (no solution)

Create `dags/02_classic_operators.py`, dag_id `02_classic_operators`. Build the
**same graph three different ways** to feel the difference between the helpers.
Use `EmptyOperator` for every task (no real work - this is about wiring).

**The target graph (a "quality gate" fan-out/fan-in):**

```
            +--> validate_schema --+
extract --> +--> validate_nulls  --+ --> load --> notify
            +--> validate_ranges --+
```
`extract` runs, then all three `validate_*` run in parallel, then `load` after
all three succeed, then `notify`.

**Do it three ways, each producing that exact graph** (put all three in the same
file, but give the tasks distinct `task_id`s per version, e.g. suffix `_a`,
`_b`, `_c`, so they don't collide):

1. **Version A - bare `>>`.** Use only `>>` and lists (`extract >> [..] >> load >> notify`).
2. **Version B - `chain`.** Reproduce it using `chain(...)` with a list in the middle.
3. **Version C - `cross_downstream` + `chain`.** Use `cross_downstream` for the extract->validators->load cross-wiring, and `chain` for the `load >> notify` tail.

**The thinking part:**
- In Version B, work out where the middle list goes and why `chain(extract, [v1,v2,v3], load, notify)` gives you the fan-out AND fan-in for free.
- In Version C, notice `cross_downstream` can't be chained - you'll need it as its own statement, then wire `load >> notify` separately.

**Constraints:**
- All tasks are `EmptyOperator`.
- Every DAG-level gate must pass: `tags`, real `owner`, `retries >= 1` via `default_args`.

**Acceptance criteria:**
- `python dags/02_classic_operators.py` parses cleanly.
- In the UI Graph, all three versions show the identical diamond shape (fan-out to 3, fan-in to load, then notify).
- `airflow dags test 02_classic_operators 2026-01-01` runs everything green.
- `python -m pytest tests/ -v` stays green.

---

## 10. Production tip - one dependency syntax, repo-wide

Pick **one** wiring style and stick to it across the repo. Mixing `>>`,
`set_downstream`, `chain`, and `chain_linear` in the same codebase makes graphs
hard to read in review. Most teams standardize on `>>` for simple lines and
`chain`/`chain_linear` only when the arrow ladder would exceed a few tasks.
Readability of the wiring is a real maintenance cost at 100+ DAGs.

---

## 11. Verify + commit

```bash
python dags/02_classic_operators.py
airflow dags test 02_classic_operators 2026-01-01
python -m pytest tests/ -v
git add dags/02_classic_operators.py && git commit -m "course: 02 classic operators" && git push
```

Done when CI is green. Tick session 02 in `docs/course/README.md`.
