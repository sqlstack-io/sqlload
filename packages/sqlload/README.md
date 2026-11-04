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
