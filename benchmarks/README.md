# SQLLoad benchmarks

Two things live here: the comparison suite (`sqlload_bench`), run on demand, and the comparison
script of the CI performance gate (`perf/compare.py`). The gate's benchmarks are the Rust crate
`crates/sqlload-benches`.

## Comparison suite

```sh
make bench-compare BENCH_OUT=/some/folder              # full size: takes hours
make bench-compare BENCH_ARGS='--size reduced'         # 1M narrow rows
make bench-compare BENCH_ARGS='--size wide --scenarios pg_wide,pg_wide_nokey'   # 1M wide rows
make bench-compare BENCH_ARGS='--scenarios pg_narrow,files --competitors sqlload,ingestr'
```

Needs Docker, uv and the Rust toolchain. `--out` receives `results.json`, `results.md`, and appends
one line per suite run to `history.jsonl`, so improvement over time stays visible. The
`Benchmarks` workflow runs the suite on GitHub (`workflow_dispatch`, reduced size by default) and
uploads the results.

### Loaders

| Loader | Version | How it runs |
|---|---|---|
| sqlload | this tree | the extension wheel with release settings; its `sqlload load` command |
| dlt | 1.32.0 (DuckDB 1.5.6) | `sql_database` with the pyarrow and with the connectorx backend; `filesystem` for files; plain resources for Python data |
| ingestr | 1.1.82 | its release binary, downloaded by the pinned wrapper |

Each loader has its own pinned uv project (`envs/`), because dlt requires `duckdb<2` while sqlload
bundles a newer DuckDB, whose files the older one cannot open. The harness itself, in `pyproject.toml`,
uses DuckDB 2.0.0 and reads every loader's output. Each loader therefore writes with its own DuckDB.

### Scenarios

| Scenario | What loads | Who |
|---|---|---|
| `pg_narrow` | Postgres, 10M rows, 6 columns | sqlload, dlt (2 backends), ingestr |
| `pg_wide` | Postgres, 1M rows, 40 columns including jsonb | same |
| `pg_wide_nokey` | the wide rows in a table with no primary key (ingestr loads such tables differently) | same |
| `pg_incremental` | load, append 1% new rows, load again; the second load is timed | same |
| `pg_multi` | the 10M narrow rows split over 8 tables | sqlload, default concurrency and `--concurrency 1` |
| `files_parquet`, `files_csv`, `files_ndjson` | the same rows in 10 local part files | sqlload, dlt, ingestr |
| `py_flat`, `py_nested` | a generator of pages of dicts, 1M rows each | sqlload, dlt |
| `py_bytes` | the flat rows as 4 MiB chunks of NDJSON | sqlload (`sl.ndjson`), dlt |
| `py_arrow` | the flat rows as one pyarrow table | sqlload, dlt |
| `py_nested_flatten` | the nested rows with `flatten=["customer"]` | sqlload (dlt flattens by default, see `py_nested`) |
| `http_json` | skipped until sqlload's HTTP helpers land; then a local stub API, also measuring requests | |
| `pg_snowflake`, `files_snowflake` | see [Snowflake](#snowflake) | |

Differences a reader should know:

- `py_nested`: sqlload keeps nested values as JSON columns by default; dlt normalises them into
  child tables. The check covers dlt's `nested__lines` too. The two do different work.
- `py_bytes`: dlt takes no bytes, so its resource parses each line with orjson, as a dlt user would.
- ingestr's `csv://` and `jsonl://` sources take one file and no globs, so it reads the same rows
  from one concatenated file. Its Parquet source takes the 10 parts.
- `pg_incremental`: sqlload and dlt use `id` as the cursor and find their own position. ingestr
  keeps no position, so it is given `--interval-start` on `created_at`.
- Defaults otherwise: no loader is tuned.

### Method

- Data comes from a seed (`--seed`, default 42): DuckDB generates the narrow and wide rows, and
  `pydata.py`, standard library only, generates the Python rows in every environment.
- Postgres runs in its own `postgres:17` container on a free port, started and stopped by
  the harness, with durability off and tables vacuumed before any run.
- Every run gets a new destination, `HOME` and XDG folders. A warm-up comes first, then the median of 3.
  Runs go round-robin across loaders, so drift in machine load spreads over all of them.
- Metrics: wall time, user and system CPU of the loader's process tree (`RUSAGE_CHILDREN`), peak
  summed RSS of the tree (sampled every 50 ms) and rows/s. Postgres's own CPU and memory are not counted.
- Correctness: after each run the destination's row count and an order-independent checksum over
  key columns must match the source, or the run is marked invalid and has no figures.
- The 1-minute load average before each run is recorded. On a shared machine, prefer CPU seconds,
  and pass `--wait-for-builds` to start runs only while no compiler is running.

## Snowflake

These scenarios run only when selected, with these variables set. None runs in CI.

| Variable | For |
|---|---|
| `SQLLOAD_BENCH_SF_ACCOUNT`, `_USER`, `_WAREHOUSE`, `_DATABASE`, `_SCHEMA` | all; `_SCHEMA` is where tables load |
| `SQLLOAD_BENCH_SF_TOKEN` or `SQLLOAD_BENCH_SF_PRIVATE_KEY_FILE` | all; a PAT, or a key-pair file |
| `SQLLOAD_BENCH_SF_ROLE` | optional |
| `SQLLOAD_BENCH_S3_BUCKET`, `SQLLOAD_BENCH_S3_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | `files_snowflake` |
| `SQLLOAD_BENCH_SF_STORAGE_INTEGRATION` or `SQLLOAD_BENCH_SF_STAGE_NAME` | `files_snowflake`; an integration SQLLoad creates its stage with, or an existing stage on `s3://<bucket>/sqlload/` |

```sh
make bench-snowflake BENCH_OUT=/some/folder BENCH_ARGS='--size reduced'
```

BENCH_ARGS can add `--scenarios` to narrow it to one scenario, as the last `--scenarios` wins.

- `pg_snowflake` loads the Postgres narrow table with sqlload, both dlt backends and ingestr;
  `pg_wide_snowflake` and `pg_wide_nokey_snowflake` load the wide table, with and without its primary key.
  `sqlload (external stage)` runs them through the S3 external stage instead of the user stage `@~`.
- `files_snowflake` also runs ingestr, reading the same Parquet parts from S3.
- `files_snowflake` loads 10 Parquet parts with sqlload's direct route (Snowflake reads the files
  itself). Each run first checks `sqlload plan` names the expected route, and is invalid if not.

What it touches: test data goes only under `s3://<bucket>/sqlload/bench/`, and the stage's URL is
`s3://<bucket>/sqlload/`. In Snowflake it drops only its `BENCH_` tables before each
run, and leaves them after the last. Objects the loaders make themselves stay (`_SQLLOAD_*`; dlt's
`_DLT_*` tables and its staging schema). Remove those by hand if the schema is shared.

## CI performance gate

`crates/sqlload-benches` measures the hot paths in Callgrind instruction counts with
[gungraun](https://github.com/gungraun/gungraun): Postgres `COPY` to Arrow, Arrow to Parquet (the
stage's parts and a files target), text-file inference and reading, row inference and schema merge,
and the Rust side of JSON bytes. Inputs are built in setup functions, which are not counted.

```sh
make bench-perf   # needs valgrind and gungraun-runner 0.20
```

The `perf` job in `verify.yml` builds and runs them for the merge base and the head in one job, and
`perf/compare.py` writes the table to the job summary. Any benchmark over 5% more instructions fails
the job unless the pull request has the `perf-regression-accepted` label. Runs on `main` upload their
counts as an artifact.

A small end-to-end timing (Postgres to DuckDB, 1M narrow rows, sqlload alone) reports to the summary
of the `containers` job and never fails it.

## Tests

```sh
make test-bench-harness
```
