# sqlload

SQL data loading tooling: the `sqlload` command line and the Python API for custom sources.

> **Pre-release.** Not yet intended for general use; anything can change between releases.

```sh
uv tool install sqlload
```

The package is a native extension (CPython 3.10+, one abi3 wheel per platform for Linux x86_64 and
aarch64 and macOS arm64). Its `sqlload` command runs in-process, so Python sources defined with
`sqlload.source` and `sqlload.table` run in the same process as the loader. The standalone
executable without Python is the `sqlload-cli` package.

## Table data types

A table function's dicts are typed like JSON: numbers, booleans and text keep their types, nested
dicts and lists become JSON, and other values become text, so `date`, `datetime` and `Decimal`
values arrive as text (datetimes with a UTC offset become timestamps). Enums give their values,
dataclasses become JSON objects, and numpy numbers count as numbers. Arrow input (pyarrow, polars,
pandas) keeps its own types.

## Upgrading from 0.4

Both this package and `sqlload-cli` install a `sqlload` command. Remove the old one first:

```sh
pip uninstall sqlload-cli
```

## Selecting tables

`plan`, `apply` and `load` take selectors positionally or with `--select`/`-s`, and
`--exclude` to leave tables out: each is a source name, a destination table name or a source's
`schema.table`. `--start-cursor`/`--end-cursor` backfill a cursor range, typed from each selected
table's cursor (an integer, an RFC 3339 timestamp or a date).

## Python sources

List the folders holding them in `sqlload.toml` (`python_paths = ["sources"]`); every
`sqlload.source` and `@sqlload.table` found there joins `plan`, `apply` and `load`:

```python
import sqlload as sl

shop = sl.source("shop", connection="shop", write_strategy="merge")


@shop.table(unique_key="order_id", cursor_column="updated_at", lookback="1h")
def orders(ctx: sl.Context):
    yield from fetch_orders(ctx.connection["base_url"], updated_after=ctx.cursor_value)
```

This loads into the table `shop__orders`: like `[sources]` entries, tables are named
`{source}__{table}` unless the source or the table sets `table_name` (with the same `{source}`,
`{schema}` and `{table}` placeholders). A `@sqlload.table` declared without a source keeps its
own name.

Columns are inferred from what a table yields (declare types with `columns={...}`, which take
precedence). A table is created by its first load, never by `apply`; a first load that yields
no rows creates nothing unless columns are declared. Under `replace` (and `--reload`), a load
that yields no rows into a table that has rows fails rather than emptying it; set
`allow_empty=True` on the table or source when an empty result is real. Rows outside the cursor window are skipped,
so a function that ignores `ctx.cursor_value` still loads correctly.

`ctx.connection` holds the source's connection settings; `ctx.connection("name")` gives another
connection's, except connections that a `[targets]` entry writes to, which table functions never
get.

### Batches

A large load commits along the way instead of all at the end, so a failure keeps what committed
and the next run carries on from there. Tables load at the same time, up to `concurrency` in
`sqlload.toml` or `--concurrency N` (default 4); one table's batches commit one at a time.

How a table commits, as `plan` shows on its `batch` line:

- `batch_size = "1d"` (a duration, for date and timestamp cursors) or `batch_size = 1000` (a step, for integer
  cursors) reads and commits one cursor window at a time, up to 100,000 windows. A Python function
  is called once per window, with `ctx.cursor_value` and `ctx.cursor_end` set to it. A Postgres
  integer cursor without `--end-cursor` ends at its maximum when `plan` ran.
- Without `batch_size`, a Postgres table whose cursor leads an index is read in cursor order (NULLs
  first) and commits about every 1 GB, only between distinct cursor values. `batch_size = "none"`
  opts out, for tables where the ordered read is slower than a scan.
- Any other cursor table commits once per read, as do text cursors and `append` without a cursor
  or logged state, since a rerun could otherwise skip or duplicate rows.
- Files sources commit at 1,000 files or about 1 GB; under `delete_insert` a partition is never split.
- `replace` and `--reload` swap the whole table in once, at the end.

Windows on a Postgres table are read in one `REPEATABLE READ` snapshot, so a row moving between
windows is read once. That snapshot stays open on the source for the whole table load: it counts
towards `idle_in_transaction_session_timeout`, and vacuum cannot remove rows it can still see. Use
a larger `batch_size` or a narrower range if either matters.

Tables of a Python source with a `@source.client` run one at a time, because clients are rarely
thread-safe; set `parallel_tables=True` on the source when its client is.

### How rows reach Snowflake

A Snowflake connection needs `load_method`, with no default: it is how data gets into Snowflake.
A method the role cannot use fails at `plan`, and never falls back to another:

```toml
[connections.wh]
kind = "snowflake"
# ...
load_method = { type = "external_stage", url = "s3://lake/sqlload/", storage_integration = "LAKE_INT" }
# or an existing stage: name = "DB.SCHEMA.STAGE" instead of storage_integration
# or { type = "internal_user_stage" }, { type = "internal_table_stage" },
#    { type = "internal_named_stage", name = "DB.SCHEMA.STAGE" }, { type = "streaming" },
#    { type = "insert" } (very slow, warned in plan)
```

An `external_stage` needs SQLLoad's own write access to the bucket. `apply` creates `_SQLLOAD_STAGE` in the
loader schema when given a `storage_integration`. Staged files are deleted once their batch commits,
and the next load removes what failed runs left after a day; `keep_staged_files = true` on the
stage keeps them and leaves that stage out of the cleanup.

Files of a files source (Parquet, JSON, NDJSON or CSV) under that URL load directly: Snowflake
reads them itself and nothing passes through SQLLoad, with the same values SQLLoad's own reader
gives, which live tests check. A column the direct `COPY` cannot load exactly (`Float16`, a decimal
over 38 digits, a nested type) fails `plan`, or the load for a later batch, naming the column: files
never fall back to SQLLoad. Files no external stage covers fail `plan` too, unless the source sets
`copy_to_internal_stage = true`.

A `hash`-masked column on this route reaches Snowflake in plain text, in a temporary table gone with
its session, and is hashed there. `hmac` is the one exception: its key must never appear in SQL, so
those files go through SQLLoad, which `plan` warns is much slower.

Files sources add `_sqlload_source_row`, a row's number in its file from 1.

### Logged state

A table whose position the data cannot carry (an API sync token, say) sets `state="logged"` and
keeps it in `ctx.state`:

```python
@shop.table(write_strategy="append", state="logged")
def events(ctx: sl.Context):
    page = fetch_events(since=ctx.state)
    yield from page.items
    ctx.state = page.next_token
```

A large run commits along the way, each commit with the value `ctx.state` had one assignment
before its current one, so whether a function sets the state before or after the rows it covers,
a run that fails part-way never loses rows; under `append`, the next run may load up to about
two pages again.
Assign `ctx.state` once per page; assigning more than once per page can lose rows after a crash.

Each successful run appends a record to `_sqlload_runs` in the table's schema (or one object under
`<table folder>/_sqlload/runs/` in a files target), in the same transaction as the data where the
warehouse allows it, so a failed load never moves the position. A table with a `cursor_column`
also logs the data's cursor; when the log and the data disagree, the data wins and the load appends
a visible reconciliation. `sqlload state show`, `verify`, `reconcile --yes` and
`reset --to VALUE --yes` (or `--clear`) inspect and correct the log; `--reload` starts over.
`reset` needs the tables or sources to reset named. On a table with a `cursor_column`,
`reset --to` sets the cursor and keeps `ctx.state`; on one without, it sets `ctx.state` (JSON).
`--clear` wipes both. A record SQLLoad cannot read (corrupt, or written by a newer version)
blocks only its table; `state show` lists it and a `reset` starts a new position after it.
Deleting the log is safe: the next run reads as a stateless one would.

