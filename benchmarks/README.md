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
| `files_s3_parquet` | the same 10 Parquet parts in S3 (runs only when selected, with `SQLLOAD_BENCH_S3_BUCKET`, `SQLLOAD_BENCH_S3_REGION` and AWS keys set) | sqlload, dlt, ingestr |
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
- `files_parquet` and `files_s3_parquet`: sqlload's plan must name DuckDB's direct route (DuckDB
  reads the files itself), or the run is invalid. DuckDB's `httpfs` extension is installed once into
  the output folder's cache and shared by later runs, as on a machine that has loaded from S3 before.
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

## Results

These are the published figures. A scenario is listed only where SQLLoad's median wall time is the
lowest of every loader measured in the same run, or within 5% of the lowest, which is labelled
parity. The `Wall vs SQLLoad` column is relative to the `sqlload` row (the user stage for
Snowflake; `sqlload (direct)` for `files_snowflake`).
Scenarios where another loader is faster are not listed here; they are tracked as work to do.
Every table shows all loaders that ran, with CPU and memory as well as wall time, and SQLLoad
does not win on every column: it uses more memory than ingestr in some of them.

Common to every table:

- **Date:** 6 November 2026.
- **Hardware:** a shared cloud VM with 4 vCPUs (Intel Xeon @ 2.60 GHz) and 8 GB of RAM, Linux 6.1.
  The 1-minute load average before each run was 0.0 to 3.4.
- **Method:** as described under [Method](#method): one warm-up, then the median of 5 runs, with
  the starting loader rotated each round; a fresh destination, `HOME` and XDG folders per run; wall
  time, user plus system CPU of the loader's process tree, and peak summed RSS. Every run's row
  count and checksum matched the source.
- **Versions:** dlt 1.32.0 (writing with DuckDB 1.5.6), ingestr 1.1.82, harness DuckDB 2.0.0,
  Python 3.12.14, Postgres `postgres:17`. SQLLoad is the release wheel built from the commit each
  table names: `07a6510` is the code released as 0.11.0, `5ab2b9e` as 0.11.1.
- **Data:** seed 42. The Parquet scenarios load the narrow rows (6 columns) as 10 part files.
- **S3:** the bucket was in eu-west-2 and the machine in us-west-2, so every request crossed
  regions (about 400 ms to first byte).

dlt is not in the 10M-row tables: one 10M-row run from a local folder took over 20 minutes, so it
was measured at 1M rows instead, under the same method.

### Parquet files into DuckDB

SQLLoad loads these through DuckDB's direct route (DuckDB reads the files itself).

Local folder, commit `07a6510`:

`files_parquet`, 10,000,000 rows

| Loader | Wall | CPU (user+sys) | Peak RSS | Wall vs SQLLoad |
|---|---:|---:|---:|---:|
| sqlload | 3.20 s | 10.58 s | 1,072 MiB | 1.00x |
| ingestr | 5.85 s | 17.55 s | 864 MiB | 1.83x |

At 10M rows SQLLoad's peak memory is about 24% above ingestr's.

`files_parquet`, 1,000,000 rows

| Loader | Wall | CPU (user+sys) | Peak RSS | Wall vs SQLLoad |
|---|---:|---:|---:|---:|
| sqlload | 0.92 s | 1.69 s | 379 MiB | 1.00x |
| dlt | 138.45 s | 139.65 s | 1,376 MiB | 150.96x |
| ingestr | 2.12 s | 3.36 s | 593 MiB | 2.32x |

S3, commit `5ab2b9e` (reads the schema sample's footers concurrently):

`files_s3_parquet`, 10,000,000 rows

| Loader | Wall | CPU (user+sys) | Peak RSS | Wall vs SQLLoad |
|---|---:|---:|---:|---:|
| sqlload | 8.92 s | 11.18 s | 1,582 MiB | 1.00x |
| ingestr | 10.01 s | 19.53 s | 824 MiB | 1.12x |

SQLLoad's peak memory here is about twice ingestr's. At 1M rows from S3 ingestr is faster
(4.33 s against 5.58 s), so that size is not listed.

### Postgres wide table into DuckDB

1M rows, 40 columns including 2 `jsonb`, `replace`; commit `07a6510`, which reads large tables
over two connections in one snapshot.

`pg_wide`, 1,000,000 rows

| Loader | Wall | CPU (user+sys) | Peak RSS | Wall vs SQLLoad |
|---|---:|---:|---:|---:|
| sqlload | 6.84 s | 10.10 s | 777 MiB | 1.00x |
| dlt (pyarrow) | 47.82 s | 49.92 s | 1,221 MiB | 6.99x |
| dlt (connectorx) | 27.24 s | 44.84 s | 788 MiB | 3.98x |
| ingestr | 8.29 s | 12.09 s | 1,118 MiB | 1.21x |

`pg_wide_nokey`, 1,000,000 rows

| Loader | Wall | CPU (user+sys) | Peak RSS | Wall vs SQLLoad |
|---|---:|---:|---:|---:|
| sqlload | 6.75 s | 9.72 s | 730 MiB | 1.00x |
| dlt (pyarrow) | 47.29 s | 49.71 s | 1,234 MiB | 7.00x |
| dlt (connectorx) | 26.89 s | 44.26 s | 785 MiB | 3.98x |
| ingestr | 8.75 s | 12.88 s | 1,499 MiB | 1.30x |

### Into Snowflake

An X-Small warehouse in AWS eu-west-2, run from the same machine as above. Size `reduced`: the
narrow table at 1M rows, the wide table at 100,000 rows, and 1M rows of Parquet in S3 for
`files_snowflake`. Commit `5ab2b9e`. `sqlload` loads Postgres rows through the user stage `@~`,
`sqlload (external stage)` through an S3 external stage, and `sqlload (direct)` lets Snowflake
read the Parquet files itself. Wall time includes Snowflake's own `COPY` time.

`pg_wide_snowflake`: SQLLoad through the user stage is at parity with ingestr (3.9% apart);
through the external stage it is faster.

`pg_snowflake`, 1,000,000 rows

| Loader | Wall | CPU (user+sys) | Peak RSS | Wall vs SQLLoad |
|---|---:|---:|---:|---:|
| sqlload | 14.56 s | 1.01 s | 131 MiB | 1.00x |
| sqlload (external stage) | 15.57 s | 1.06 s | 132 MiB | 1.07x |
| dlt (pyarrow) | 31.02 s | 5.56 s | 305 MiB | 2.13x |
| dlt (connectorx) | 29.36 s | 3.77 s | 341 MiB | 2.02x |
| ingestr | 16.38 s | 3.99 s | 495 MiB | 1.12x |

`pg_wide_snowflake`, 100,000 rows

| Loader | Wall | CPU (user+sys) | Peak RSS | Wall vs SQLLoad |
|---|---:|---:|---:|---:|
| sqlload | 13.86 s | 0.90 s | 176 MiB | 1.00x |
| sqlload (external stage) | 11.60 s | 0.86 s | 181 MiB | 0.84x |
| dlt (pyarrow) | 31.48 s | 6.58 s | 828 MiB | 2.27x |
| dlt (connectorx) | 28.76 s | 3.50 s | 315 MiB | 2.07x |
| ingestr | 14.42 s | 4.07 s | 583 MiB | 1.04x |

`pg_wide_nokey_snowflake`, 100,000 rows

| Loader | Wall | CPU (user+sys) | Peak RSS | Wall vs SQLLoad |
|---|---:|---:|---:|---:|
| sqlload | 12.66 s | 0.87 s | 178 MiB | 1.00x |
| sqlload (external stage) | 12.25 s | 0.85 s | 180 MiB | 0.97x |
| dlt (pyarrow) | 31.47 s | 6.46 s | 727 MiB | 2.49x |
| dlt (connectorx) | 30.12 s | 3.36 s | 313 MiB | 2.38x |
| ingestr | 14.61 s | 4.06 s | 586 MiB | 1.15x |

`files_snowflake`, 1,000,000 rows

| Loader | Wall | CPU (user+sys) | Peak RSS | Wall vs SQLLoad |
|---|---:|---:|---:|---:|
| sqlload (direct) | 10.30 s | 0.17 s | 46 MiB | 1.00x |
| ingestr | 19.47 s | 4.41 s | 510 MiB | 1.89x |

## Snowflake

These scenarios run only when selected, with these variables set. None runs in CI.

| Variable | For |
|---|---|
| `SQLLOAD_BENCH_SF_ACCOUNT`, `_USER`, `_WAREHOUSE`, `_DATABASE`, `_SCHEMA` | all; `_SCHEMA` is where tables load |
| `SQLLOAD_BENCH_SF_TOKEN` or `SQLLOAD_BENCH_SF_PRIVATE_KEY_FILE` | all; a PAT, or a key-pair file |
| `SQLLOAD_BENCH_SF_ROLE` | optional |
| `SQLLOAD_BENCH_S3_BUCKET`, `SQLLOAD_BENCH_S3_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | `files_snowflake` (and `files_s3_parquet`, which needs no Snowflake) |
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

## S3 into DuckDB

`python -m sqlload_bench.s3_duckdb` measures how S3 Parquet should reach DuckDB. It loads the narrow
rows, as 10 Parquet parts under `s3://<bucket>/sqlload/s3bench/`, into a new DuckDB file in two
ways: `sqlload load`, where SQLLoad reads the files and appends Arrow, and DuckDB's own
`read_parquet` over httpfs. Runs alternate between the two, and both must match the same checksum.
It needs AWS credentials in the environment:

```sh
uv run python -m sqlload_bench.s3_duckdb --sqlload "$(which sqlload)" --bucket B --region R --rows 10000000
```

It compares SQLLoad with DuckDB's own `read_parquet` alone, not with another loader; it informed the
choice of route (DuckDB reads Parquet itself), and its figures are kept in the tracking issue.

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
