# sqlload-api

SQLLoad's Python API for describing data and HTTP requests, in pure Python with no compiled code: `http(...)` jobs and
their paginators, `json(...)` / `ndjson(...)`, `field`, `parent`, `key`, `checkpoint` and `transform`.

You rarely install it yourself: [`sqlload`](https://pypi.org/project/sqlload/) depends on it and re-exports it
(`import sqlload as sl; sl.http(...)`). Tools that run SQLLoad's engine in their own process use it to build the same jobs
without loading a second engine:

```python
import sqlload_api as sl

api = sl.http(connection="shop_api")
job = api.paginate("/orders", paginator=sl.next_link("$.next"), select="$.items")
```

A job built this way is identical to one built from a SQLLoad source with the same connection. Released together with
`sqlload`, at the same version; an engine reads the API of its own version.
