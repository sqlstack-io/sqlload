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

