<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/sqlstack-io/sqlload/main/.github/sqlload-logo-dark.png">
    <source media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/sqlstack-io/sqlload/main/.github/sqlload-logo-light.png">
    <img src="https://raw.githubusercontent.com/sqlstack-io/sqlload/main/.github/sqlload-logo-light.png" alt="SQLLoad" width="320">
  </picture><br>
  <sub>by <a href="https://sqlstack.io">SQLStack</a></sub>
</p>

<p align="center">
  <strong>Load data into your warehouse. Fast.</strong><br>
  <strong>See the plan before any data moves.</strong>
</p>

<p align="center">
  <a href="https://sqlload.io">Website</a> ·
  <a href="https://sqlload.io/docs/">Docs</a> ·
  <a href="https://sqlload.io/docs/quickstart/">Quickstart</a> ·
  <a href="https://sqlload.io/docs/benchmarks/">Benchmarks</a>
</p>

SQLLoad loads Postgres tables, files and Python sources into Snowflake, Databricks, BigQuery
(preview), DuckDB or Parquet files. `sqlload plan` shows every table, column type, write strategy and route before
anything is created or loaded. Where Snowflake or DuckDB can load your files exactly by itself,
the files never pass through SQLLoad. It is written in Rust and is open source under Apache-2.0;
Python is needed only for Python sources.

<p align="center">
  <img src="https://raw.githubusercontent.com/sqlstack-io/sqlload/main/.github/demos/plan-load.gif" alt="sqlload plan lists the two tables it will create from Postgres, their column types in DuckDB, and how each loads; sqlload load then creates them and loads 1,050 rows" width="100%">
</p>

`sqlload plan` on the [quickstart](https://sqlload.io/docs/quickstart/) project shows the tables it
would create from Postgres, each column's DuckDB type and how each table loads. `sqlload load`
then creates and fills them.

## Quick start

```sh
uv tool install sqlload
sqlload copy orders/ duckdb://shop.duckdb#raw.orders
```

`sqlload copy` loads one table once and needs no configuration file. Here a folder of Parquet
files becomes a DuckDB table; the plan prints first:

```console
$ sqlload copy orders/ duckdb://shop.duckdb#raw.orders
Plan ready  1 selected, 1 object to create, 1 to load

Objects (1)                                         sqlload apply
└── raw.orders                                      create table
    ├── order_id   int            -> BIGINT
    ├── customer   text           -> VARCHAR
    ├── total      float          -> DOUBLE
    ├── ordered_at timestamp_tz   -> TIMESTAMP WITH TIME ZONE
    └── + _sqlload_source_file, _source_version, _source_row, _load_id, _loaded_at

Loads (1)                                           sqlload load
└── orders                                          replace
    ├── source  files  orders/  parquet
    ├── files   1 to read of 1 listed
    ├── route   DuckDB reads directly from orders/ (nothing through the loader)
    └── batch   1 batch of whole partitions, up to 1,000 files or about 1 GB per commit; each batch's footers are combined, so later batches may add columns

✓ Plan complete  1 selected, 1 object to create, 1 to load
created  raw.orders  9 columns
loaded   raw.orders  3 rows  0.0s

✓ Completed  1 loaded, 3 rows, 0.1s
```

Add `--print-toml` to print the same copy as a `sqlload.toml` project, which loads its tables with
`sqlload plan`, `sqlload apply` and `sqlload load`. The
[quickstart](https://sqlload.io/docs/quickstart/) walks through one.

## What it loads

| Sources | Targets |
|---------|---------|
| Postgres tables, and Postgres changes from a logical replication slot | Snowflake |
| Parquet, CSV, JSON and NDJSON files in S3 or a local folder | Databricks |
| Python sources, including HTTP APIs | BigQuery (preview) |
| | DuckDB |
| | Parquet files in S3 or a local folder |

See [Supported systems](https://sqlload.io/docs/supported-systems/) for which combinations and
write strategies each supports.

**Postgres change data capture.** A `postgres_cdc` source copies every table under a snapshot on
the first `sqlload load`. Each later load applies the inserts, updates and deletes committed since
the last one, from a logical replication slot, as `merge` or `changelog` into DuckDB, Snowflake or
Databricks, or as a changelog into Parquet files.
From 0.18.0, columns added, dropped or widened while streaming carry through. From 0.20.0,
`sqlload load app --follow` keeps streaming, writing a batch every 10 seconds by default.
See [Postgres CDC](https://sqlload.io/docs/sources/postgres-cdc/).

**Kafka exporter.** A separate mode, `sqlload run`, streams Kafka topics into Snowflake. Its
configuration is in [sqlload.example.toml](sqlload.example.toml); see the
[Kafka exporter](https://sqlload.io/docs/kafka/) docs.

## Install

The `sqlload` package is a native Python extension for CPython 3.10 or newer, and runs Python
sources in the same process as the loader. `sqlload-cli` is the same command as a standalone
executable, without Python and without Python sources. Install only one of them, because both
provide a `sqlload` command (`uv tool install sqlload-cli` for the executable). Both are released
for Linux (x86_64 and aarch64, glibc 2.28 or newer) and macOS 11 or newer (arm64). See
[Install](https://sqlload.io/docs/install/).

## Learn more

- [Documentation](https://sqlload.io/docs/): install, quickstart, concepts, the CLI and Python
  references, configuration and supported systems.
- [The `sqlload` package README](packages/sqlload/README.md): Python and HTTP sources, batches,
  logged state, change data capture, and how rows reach Snowflake, Databricks,
  BigQuery and DuckDB.
- [Benchmarks](benchmarks/README.md): SQLLoad, dlt and ingestr loading the same data into DuckDB
  and Snowflake, with the method, versions and figures.
- [Changelog](CHANGELOG.md).

## Development

```sh
make check            # fmt, clippy, fensu
make test-unit
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for the container and Python tests and the rules the checks
enforce.

## License

Apache-2.0.
