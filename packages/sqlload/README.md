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
def shop_orders(ctx: sl.Context):
    yield from fetch_orders(ctx.connection["base_url"], updated_after=ctx.cursor_value)
```

Columns are inferred from what a table yields (declare types with `columns={...}`). Rows outside
the cursor window are skipped, so a function that ignores `ctx.cursor_value` still loads
correctly.
