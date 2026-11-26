# sqlload-python-host

The Python host of [SQLLoad](https://github.com/sqlstack-io/sqlload)'s engine, for a program that runs the engine in a
pyo3 extension of its own. It runs Python tables and turns what they yield into the engine's batches exactly as SQLLoad's
`sqlload` package does: dicts and lists of dicts, Arrow data (pyarrow, polars, pandas), `sqlload_api.json` / `ndjson`,
`sqlload_api.http` jobs (run in Rust, page transforms included) and `sqlload_api.checkpoint`s. The generator runs on its
own thread, and its rows are parsed on others without the GIL.

- `host::main::module_host::module_host(runtime)` is the engine's `PythonHost` over a Python runtime module of the
  host's own; SQLLoad's is `sqlload._runtime`.
- `host::main::table_stream::table_stream` and `host::main::transform_stream::transform_stream` cross one table's
  yields, or one transform's result, for a host that implements `PythonHost` itself.

```rust,ignore
use sqlload_python_host::host::main::module_host::module_host;

let host = module_host("my_tool._sqlload_runtime");
```

## The runtime module

| Function | What it does |
| --- | --- |
| `discover(root, python_paths) -> str` | The sources found, as the JSON list of `PythonSource`s: `name`, `module`, `settings` (TOML keys, tables under `tables`) and an optional `host` value of the host's own, which the engine copies unread into each of the source's reads. |
| `start_table(read) -> Iterable` | Call the table `read` (a `TableRead`, as JSON) names and return what it yields. |
| `current_state(table)`, `table_state(table) -> str \| None` | A logged table's `ctx.state` as JSON while its rows cross, and once its function has finished; `sqlload_api._host.current_state` and `final_state` answer both for a `Context`. |
| `rename(name, columns, context) -> (str \| None, str \| None)` | A table's `rename`: the names, or why there are none. |
| `finish_run() -> None` | The run is over: close clients. |

`sqlload_api.Context.from_read(read, **extras)` builds a table's `ctx` as SQLLoad does; a host with fields of its own
subclasses `sqlload_api.Context`. Transforms and the crossing's callbacks are `sqlload_api._host`'s, never the
runtime's, so one transform registry serves the whole process.

## Versions

- **pyo3 0.29** (any patch), with `abi3-py310` (a host may raise the floor). pyo3 types are part of this crate's
  interface, so your extension uses the same pyo3. Cargo refuses a build with two pyo3 minors, as `pyo3-ffi` links the
  `python` library; `sqlload_python_host::pyo3` re-exports the one in use.
- **`sqlload-api`** at exactly this crate's version in the interpreter. A different version fails the first read, naming
  the version to install.
- This crate, `sqlload` and `sqlload-api` are released together, at one version.
