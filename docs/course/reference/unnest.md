# Reference · `SPLIT` + `UNNEST` (exploding a delimited column)

How to turn one string that jams many values into one column
(`"python|pandas|airflow"`) into one row per value — the pattern the Stack
Overflow `posts_questions.tags` column forces on you.

## The one-line mental model

`SPLIT` makes an **array** out of one string; `UNNEST` turns that **array into
rows**; the **comma** glues those rows back to their parent row.

```
"python|pandas|airflow"   ──SPLIT──▶   ['python','pandas','airflow']   ──UNNEST──▶   3 rows
     one STRING                            one ARRAY<STRING>                         one tag each
```

---

## Step 1 — `SPLIT(tags, '|')` → an ARRAY

`SPLIT(string, delimiter)` returns `ARRAY<STRING>`: it cuts the string at every
delimiter.

| `tags` (STRING)             | `SPLIT(tags, '\|')` (ARRAY&lt;STRING&gt;)     |
| --------------------------- | ------------------------------------------- |
| `"python\|pandas\|airflow"` | `['python', 'pandas', 'airflow']`           |
| `"sql"`                     | `['sql']` — one element, no delimiter found |
| `""` (empty)                | `['']` — array with **one empty string**    |
| `NULL`                      | `NULL` — SPLIT of NULL is NULL, not an array |

The delimiter is `'|'` because Stack Overflow stores tags as one pipe-joined
string in a single column — there is no separate tags-per-question table. `SPLIT`
is how you recover the individual tags.

---

## Step 2 — `UNNEST(...)` → rows

`UNNEST(array)` is a **table-valued function**: give it an array, it returns a
table with one row per element.

```
UNNEST(['python','pandas','airflow'])
        │
        ▼
   ┌──────────┐
   │  python  │
   │  pandas  │   ← 3 rows, one column
   │  airflow │
   └──────────┘
```

`AS tag` names that column, so downstream you write `tag`, `GROUP BY tag`, etc.

Edge cases that bite:

- `UNNEST([])` (empty array) → **0 rows** → that parent row disappears from the result.
- `UNNEST(NULL)` → **0 rows** → a row with `NULL` in the column vanishes too.
- `UNNEST([''])` → **1 row** with an empty string → shows up as a blank value in a `GROUP BY`.

---

## Step 3 — the comma: a correlated CROSS JOIN

The comma is shorthand. Written out:

```sql
FROM posts_questions AS q
CROSS JOIN UNNEST(SPLIT(q.tags, '|')) AS tag
```

It's **correlated**: the `UNNEST` on the right references `q.tags` from the row on
the left. So for *each* question, BigQuery unnests *that question's own* tag array
and pairs every resulting tag back with the full question row.

```
question row                     tags exploded              joined result
id=102, answer_count=0,   ──▶   ['python','airflow']  ──▶   (102, 'python')
       tags="python|airflow"                                (102, 'airflow')
```

One row with N array elements becomes **N rows**, each carrying all the parent's
other columns (`id`, `answer_count`, `score`…) repeated. That is why you can still
filter on `answer_count = 0` *after* the unnest — every exploded row still has it.

---

## Full worked example

Input (2 questions):

```
id   answer_count  tags
201  0             python|pandas
202  0             python|airflow|etl
```

After `FROM posts_questions, UNNEST(SPLIT(tags,'|')) AS tag`:

| id  | answer_count | tag     |
| --- | ------------ | ------- |
| 201 | 0            | python  |
| 201 | 0            | pandas  |
| 202 | 0            | python  |
| 202 | 0            | airflow |
| 202 | 0            | etl     |

5 rows out of 2 questions. Now `GROUP BY tag` counts `python → 2`, everything else
`→ 1`. The row-fan-out is exactly what makes "count questions per tag" possible
from a column that jammed all tags into one string.

---

## Why not just `WHERE tags LIKE '%python%'`?

`LIKE` also matches `python-3.x`, `cpython`, `micropython` — substring false
positives. `SPLIT` + `UNNEST` gives you the **exact** tag tokens, so `tag =
'python'` matches only the real `python` tag. That precision is the whole reason
for the unnest.

---

## Practical guard

Because empty/blank tags can sneak in (`['']` from an empty or edge-piped string),
production queries usually add:

```sql
WHERE answer_count = 0 AND tag != ''
```

so a blank tag never tops your "most unanswered" list.

---

## The pattern, generalized

Any time a column holds a delimited list, this is the recipe:

```sql
SELECT item, COUNT(*) AS n
FROM my_table, UNNEST(SPLIT(delimited_column, '<delim>')) AS item
WHERE item != ''
GROUP BY item
ORDER BY n DESC
```

Swap `'<delim>'` for `','`, `';'`, `' '`, etc. Same three moves every time:
**split → unnest → join back.**

Sources:
[SPLIT — BigQuery string functions](https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#split),
[UNNEST — BigQuery array/query syntax](https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#unnest_operator)
