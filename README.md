<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/sqlstack-io/sqlload/main/.github/sqlload-logo-dark.png">
    <source media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/sqlstack-io/sqlload/main/.github/sqlload-logo-light.png">
    <img src="https://raw.githubusercontent.com/sqlstack-io/sqlload/main/.github/sqlload-logo-light.png" alt="SQLLoad" width="420">
  </picture>
  <br>
  <sub>by <a href="https://sqlstack.io">SQLStack</a></sub>
</p>

<h2 align="center">The fastest open-source data loader.</h2>

<p align="center">
  <strong>Up to 2.3× faster than ingestr and 150× faster than dlt. 10 million Parquet rows into DuckDB in 3.2 seconds.</strong> <a href="https://sqlload.io/docs/benchmarks/">Benchmarks</a>
  <br>
  <a href="https://sqlload.io">Website</a> ·
  <a href="https://sqlload.io/docs/">Docs</a> ·
  <a href="https://sqlload.io/docs/quickstart/">Quickstart</a> ·
  <a href="https://sqlload.io/docs/benchmarks/">Benchmarks</a>
</p>

Load Postgres, files, Snowflake, BigQuery, Google Sheets and Python sources into your warehouse.
SQLLoad is Rust and Arrow end to end, and when your warehouse reads the files itself, rows never
pass through SQLLoad, so there's no loader bottleneck as your data grows.

```bash
uv tool install sqlload        # or: pip install sqlload
```

<p align="center">
  <img src="https://raw.githubusercontent.com/sqlstack-io/sqlload/main/.github/demos/plan-load.gif" alt="sqlload plan lists the two tables it will create from Postgres, their column types in DuckDB, and how each loads; sqlload load then creates them and loads 1,050 rows" width="100%">
</p>

`sqlload plan` shows the tables it will create from Postgres, each column's DuckDB type and how
each table loads. `sqlload load` then creates and fills them.

## Quick start

```bash
sqlload plan     # every table, column type and route, before anything is created
sqlload apply    # create the tables
sqlload load     # load them
```

A `sqlload.toml` names your sources and targets; the
[quickstart](https://sqlload.io/docs/quickstart/) builds one. To load one table once with no
configuration file:

```bash
sqlload copy orders/ duckdb://shop.duckdb#raw.orders
```

## Speed

The same data loaded by each tool on a 4-vCPU machine, median of 5 runs:

| Scenario | SQLLoad | ingestr | dlt |
|---|---:|---:|---:|
| Parquet folder into DuckDB, 1M rows | **0.92 s** | 2.12 s | 138 s |
| Parquet folder into DuckDB, 10M rows | **3.20 s** | 5.85 s | over 20 min (one run) |
| Postgres (40 columns) into DuckDB, 1M rows | **6.84 s** | 8.29 s | 27.2 s |
| Postgres into Snowflake, 1M rows | **14.56 s** | 16.38 s | 29.4 s |

See the [benchmarks](https://sqlload.io/docs/benchmarks/) for every scenario, the method, the
versions and how to run them yourself.

## Why SQLLoad

- **See the plan before any data moves.** `sqlload plan` shows every table, column type, write
  strategy and route before anything is created or loaded, so there are no surprises in your
  warehouse.
- **Crash-safe loads.** Every commit lands whole, even if a load is killed halfway. A full refresh
  swaps in one go, so your dashboards see the old table or the new one, never a mix.
- **Postgres change data capture.** A `postgres_cdc` source copies your tables once, then applies
  every insert, update and delete from a logical replication slot, in whole source transactions.
  `sqlload load --follow` keeps streaming. See
  [Postgres CDC](https://sqlload.io/docs/sources/postgres-cdc/).

## What it loads

| System | Source | Target |
|---|---|---|
| Postgres | ✓ (incl. CDC) | ✓ |
| Snowflake | ✓ | ✓ |
| BigQuery | ✓ | preview |
| Databricks | | ✓ |
| ClickHouse | | preview |
| DuckDB | | ✓ |
| MotherDuck | | beta² |
| Files | ✓ Parquet, CSV, JSON (S3, local folder, SFTP) | ✓ Parquet, CSV (S3, local folder) |
| Google Sheets | ✓ | |
| HTTP APIs and Python | ✓ | |
| ODBC databases¹ | ✓ | |

¹ In builds with the `odbc` feature.

² Once MotherDuck accepts DuckDB 2.0 clients.

See [Supported systems](https://sqlload.io/docs/supported-systems/) for every combination and
write strategy.

---

Built in Rust. Free and open source. [Apache 2.0](https://github.com/sqlstack-io/sqlload/blob/main/LICENSE).
[Contributing](https://github.com/sqlstack-io/sqlload/blob/main/CONTRIBUTING.md) ·
[Changelog](https://github.com/sqlstack-io/sqlload/blob/main/CHANGELOG.md)

Part of [SQLStack](https://sqlstack.io/).
