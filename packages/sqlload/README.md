<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/sqlstack-io/sqlload/main/.github/sqlload-logo-dark.png">
    <source media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/sqlstack-io/sqlload/main/.github/sqlload-logo-light.png">
    <img src="https://raw.githubusercontent.com/sqlstack-io/sqlload/main/.github/sqlload-logo-light.png" alt="SQLLoad" width="420">
  </picture>
  <br>
  <sub>by <a href="https://sqlstack.io">SQLStack</a></sub>
</p>

<h2 align="center">Loads you can trust.</h2>

<p align="center">
  <strong>Exact types. Loud failures. Bounded memory.</strong>
  <br>
  <a href="https://sqlload.io">Website</a> ·
  <a href="https://sqlload.io/docs/">Docs</a> ·
  <a href="https://sqlload.io/docs/quickstart/">Quickstart</a> ·
  <a href="https://sqlload.io/docs/benchmarks/">Benchmarks</a>
</p>

Load Postgres, files, Snowflake, BigQuery, Google Sheets and Python sources into your warehouse.
SQLLoad is Rust and Arrow end to end, and when your warehouse reads the files itself, rows never
pass through SQLLoad, so there's no loader bottleneck as your data grows. It's fast, too: faster
than ingestr and dlt in every [benchmark](https://sqlload.io/docs/benchmarks/) we publish.

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

A `sqlload.toml` names your sources and targets. `sqlload playground` writes one that loads
sample CSV files into DuckDB, with no database or credentials; the
[quickstart](https://sqlload.io/docs/quickstart/) runs it. To load one table once with no
configuration file:

```bash
sqlload copy orders/ duckdb://shop.duckdb#raw.orders
```

## Speed

Faster, in a fraction of the memory. The same data loaded by each tool on one 4-vCPU, 8 GB machine;
lower is better:

| Scenario | SQLLoad | ingestr | dlt |
|---|---:|---:|---:|
| Parquet in S3 into Snowflake, 100M rows: wall time | **134.4 s** | 182.3 s | not run |
| The same runs: peak memory | **54 MiB** | 3,836 MiB + 5.7 GiB temp disk | not run |
| Postgres into Snowflake, 1M rows: wall time | **14.56 s** | 16.38 s | 29.36 s |

Parquet in S3 into Snowflake: wall time and peak memory from the same 100M-row runs, median of 3,
18 November 2026, sqlload 0.70.1 and ingestr 1.1.82. On SQLLoad's default route Snowflake copies the
files itself, so no row passes through SQLLoad and it uses no disk; ingestr streams every row
through its own process and spilled 5.7 GiB to disk. Postgres into Snowflake: median of 5 runs,
6 November 2026, sqlload 0.11.1, ingestr 1.1.82, dlt 1.32.0 (its connectorx backend for Postgres);
peak memory 131 MiB for SQLLoad, 495 MiB for ingestr and 341 MiB for dlt.

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

¹ In the published wheels from 0.39.0; before that, in builds with the `odbc` feature.

² From 0.34.0, through a separate DuckDB 1.5 process: install `sqlload[motherduck]`.

See [Supported systems](https://sqlload.io/docs/supported-systems/) for every combination and
write strategy.

---

Built in Rust. Free and open source. [Apache 2.0](https://github.com/sqlstack-io/sqlload/blob/main/LICENSE).
[Contributing](https://github.com/sqlstack-io/sqlload/blob/main/CONTRIBUTING.md) ·
[Changelog](https://github.com/sqlstack-io/sqlload/blob/main/CHANGELOG.md)

Part of [SQLStack](https://sqlstack.io/).
