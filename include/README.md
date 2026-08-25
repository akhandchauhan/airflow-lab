# include/

Business logic lives here, **not** in `dags/`.

Airflow does not parse this directory, which means:

- Heavy imports (pandas, requests, SDK clients) never slow down DAG parsing.
- You can unit-test these modules with plain pytest, no Airflow runtime needed.

DAG files should only wire tasks together. The moment a DAG file contains a
`for` loop over an API response or a `pd.read_csv`, it belongs here instead.

Suggested layout:

```
include/
  extractors/    # API clients, DB readers
  transformers/  # pure functions: data in, data out
  sql/           # .sql files rendered by templated operators
```
