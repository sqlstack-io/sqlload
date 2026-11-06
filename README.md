# SQLLoad

SQLLoad loads data into your warehouse, and shows you how before it changes anything.

It reads Postgres tables, files (Parquet, CSV, JSON and NDJSON) in S3 or a local folder, and Python
sources, including HTTP APIs. It writes to Snowflake, DuckDB, or Parquet files in S3 or a folder.
`sqlload plan` shows each table's columns, write strategy and route before `apply` and `load`
run. Where Snowflake or DuckDB can load files exactly by itself, the files never pass through
SQLLoad. SQLLoad is written in Rust. Python is needed only for Python sources.

A separate exporter (`sqlload run`) streams Kafka topics into Snowflake. Its configuration is in
[sqlload.example.toml](sqlload.example.toml).

**Status: pre-release.** It is not yet intended for general use. Commands, configuration and
behaviour can change between releases, and there is no support or stability guarantee.

## Copy one table

`sqlload copy` loads one table once and needs no configuration file. Here, a folder of Parquet
files is loaded into a DuckDB table:

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

✓ Completed  1 loaded, 3 rows, 0.0s
```

Add `--print-toml` to print the same copy as a `sqlload.toml` project. A project loads its tables
with `sqlload plan`, `sqlload apply` and `sqlload load`.

## Install

```sh
uv tool install sqlload
```

The `sqlload` package is a native Python extension for CPython 3.10 or newer, and runs Python
sources in the same process as the loader. `sqlload-cli` is the same command as a standalone
executable, without Python and without Python sources. Install only one of them, because both
provide a `sqlload` command. Both are released for Linux (x86_64 and aarch64) and macOS (arm64).

## Learn more

- [The `sqlload` package README](packages/sqlload/README.md): Python and HTTP sources, batches,
  logged state, and how rows reach Snowflake and DuckDB.
- [Benchmarks](benchmarks/README.md): SQLLoad compared with dlt and ingestr, loading into DuckDB and
  Snowflake. The published results list only the scenarios where SQLLoad is fastest or within 5%.
  Scenarios where another loader is faster are tracked as work to do.
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
