# Changelog

## [0.9.0](https://github.com/sqlstack-io/sqlload/compare/v0.8.0...v0.9.0) (2026-11-06)


### Features

* **python:** sl.checkpoint commits exactly the rows yielded before it ([#44](https://github.com/sqlstack-io/sqlload/issues/44)) ([4f6fcc9](https://github.com/sqlstack-io/sqlload/commit/4f6fcc973445244108b9935d23cf36fdba1d3787))
* **python:** sl.http paginate and fan_out jobs run in Rust ([#43](https://github.com/sqlstack-io/sqlload/issues/43)) ([18c571f](https://github.com/sqlstack-io/sqlload/commit/18c571fc7cc06a9b06587a3df59c08e5013a419e))

## [0.8.0](https://github.com/sqlstack-io/sqlload/compare/v0.7.0...v0.8.0) (2026-11-06)


### Features

* **python:** flatten listed objects one level into typed columns ([#41](https://github.com/sqlstack-io/sqlload/issues/41)) ([752a452](https://github.com/sqlstack-io/sqlload/commit/752a452f060699bb0037fb8ac3f3e0406105f70c))

## [0.7.0](https://github.com/sqlstack-io/sqlload/compare/v0.6.0...v0.7.0) (2026-11-06)


### ⚠ BREAKING CHANGES

* **loader:** Python tables, files sources, [sources.*.tables] entries and tables named exactly in include need a description (a Python docstring counts), which becomes the table comment; discovered tables take the source's table comment
* **loader:** with new_tables = "skip" or "fail", a [sources.*.tables] entry no longer approves a new table; name it exactly in include
* **cli:** load --reload asks for confirmation and is refused off a terminal; scripts add --yes
* **python:** Python Decimal, date and naive datetime values load as decimal, date and timestamp_ntz instead of text
* **loader:** an inferred column (Python tables, JSON and CSV files) whose values outgrow the table fails instead of widening; set auto_widen = true to widen along bool -> int -> decimal and timestamp_ntz -> timestamp_tz

### Features

* **cli:** load --reload asks for confirmation and is refused off a terminal; scripts add --yes ([e9865d0](https://github.com/sqlstack-io/sqlload/commit/e9865d027b8421a98c5afbcca862e5340de8a19f))
* **loader:** an inferred column (Python tables, JSON and CSV files) whose values outgrow the table fails instead of widening; set auto_widen = true to widen along bool -&gt; int -&gt; decimal and timestamp_ntz -&gt; timestamp_tz ([e9865d0](https://github.com/sqlstack-io/sqlload/commit/e9865d027b8421a98c5afbcca862e5340de8a19f))
* **loader:** Python tables, files sources, [sources.*.tables] entries and tables named exactly in include need a description (a Python docstring counts), which becomes the table comment; discovered tables take the source's table comment ([e9865d0](https://github.com/sqlstack-io/sqlload/commit/e9865d027b8421a98c5afbcca862e5340de8a19f))
* **loader:** with new_tables = "skip" or "fail", a [sources.*.tables] entry no longer approves a new table; name it exactly in include ([e9865d0](https://github.com/sqlstack-io/sqlload/commit/e9865d027b8421a98c5afbcca862e5340de8a19f))
* **python:** Python Decimal, date and naive datetime values load as decimal, date and timestamp_ntz instead of text ([e9865d0](https://github.com/sqlstack-io/sqlload/commit/e9865d027b8421a98c5afbcca862e5340de8a19f))

## [0.6.0](https://github.com/sqlstack-io/sqlload/compare/v0.5.1...v0.6.0) (2026-11-06)


### ⚠ BREAKING CHANGES

* **cli:** plan, apply and load print a new text layout, and a failed plan or failed table is printed on stdout (exit code 1) instead of as `error: ...` on stderr; scripts should use `plan --json` or the exit code

### Features

* **cli:** plan, apply and load output in the sqb plan layout, plan --json, colour and a load tally ([61f5636](https://github.com/sqlstack-io/sqlload/commit/61f56360785384b72709a535ec6a972053fb9358))

## [0.5.1](https://github.com/sqlstack-io/sqlload/compare/v0.5.0...v0.5.1) (2026-11-05)


### Bug Fixes

* **cli:** refuse one cursor range for selected tables with cursors of different types ([#35](https://github.com/sqlstack-io/sqlload/issues/35)) ([da83cb2](https://github.com/sqlstack-io/sqlload/commit/da83cb211afbd2b7350579b7d0a8240a34f09bb3))

## [0.5.0](https://github.com/sqlstack-io/sqlload/compare/v0.4.0...v0.5.0) (2026-11-05)


### ⚠ BREAKING CHANGES

* **cli:** the typed --start-cursor-ts, --start-cursor-int, --end-cursor-ts and --end-cursor-int flags are removed; --start-cursor and --end-cursor take every cursor type
* **cli:** the check alias of sqlload debug is removed; use sqlload debug
* **config:** renamed options with no aliases: batch is now batch_size ("whole" is now "none"), threads and --threads are now concurrency and --concurrency, ${NAME} is now ${ENV:NAME}, and DuckDB path is now database; shaping = "loader" is removed
* **snowflake:** load_method is required on a Snowflake connection and replaces stage; with no load_method the plan fails and names the choices

### Features

* **benchmarks:** comparison suite against dlt and ingestr, and a CI gate on hot-path instruction counts ([#29](https://github.com/sqlstack-io/sqlload/issues/29)) ([0949100](https://github.com/sqlstack-io/sqlload/commit/09491002712511f0c002dd14a2db22e8f92d3fe5))
* **cli:** --select/-s, --exclude, and --start-cursor/--end-cursor typed per table at plan ([73b9fe0](https://github.com/sqlstack-io/sqlload/commit/73b9fe04cc921e906ccb5dcc60039df8d7e3e45d))
* **cli:** the check alias of sqlload debug is removed; use sqlload debug ([0a1fc72](https://github.com/sqlstack-io/sqlload/commit/0a1fc7246ecd99b9f019a8acd377c674dce9d351))
* **cli:** the typed --start-cursor-ts, --start-cursor-int, --end-cursor-ts and --end-cursor-int flags are removed; --start-cursor and --end-cursor take every cursor type ([0a1fc72](https://github.com/sqlstack-io/sqlload/commit/0a1fc7246ecd99b9f019a8acd377c674dce9d351))
* **config:** renamed options with no aliases: batch is now batch_size ("whole" is now "none"), threads and --threads are now concurrency and --concurrency, ${NAME} is now ${ENV:NAME}, and DuckDB path is now database; shaping = "loader" is removed ([73b9fe0](https://github.com/sqlstack-io/sqlload/commit/73b9fe04cc921e906ccb5dcc60039df8d7e3e45d))
* **loader:** per-batch publish for every source and destination ([#24](https://github.com/sqlstack-io/sqlload/issues/24)) ([fe39892](https://github.com/sqlstack-io/sqlload/commit/fe39892e1c5d7627635b4b99f397e89d68ea1ee3))
* **loader:** Snowflake external stage, direct COPY from files, full stage fallback chain ([#25](https://github.com/sqlstack-io/sqlload/issues/25)) ([69e4719](https://github.com/sqlstack-io/sqlload/commit/69e4719dbd5e450be4bba834a2845fea19714993))
* **postgres:** read ahead of the consumer, so Postgres keeps sending while a batch is written ([#27](https://github.com/sqlstack-io/sqlload/issues/27)) ([f455c11](https://github.com/sqlstack-io/sqlload/commit/f455c1161c4ef8c91a23244b30c78ff89b664804))
* **python:** logged state for Python tables (_sqlload_runs, sqlload state) ([#22](https://github.com/sqlstack-io/sqlload/issues/22)) ([d1a16f5](https://github.com/sqlstack-io/sqlload/commit/d1a16f571e9a159187085aec2de1e3a51d32ce5b))
* **python:** Python sources in plan, apply and load ([#21](https://github.com/sqlstack-io/sqlload/issues/21)) ([097a140](https://github.com/sqlstack-io/sqlload/commit/097a140f2984500c5027f34e7c044dc0d5ecfece))
* **python:** sqlload as a native extension package; Python data crosses into Arrow ([#19](https://github.com/sqlstack-io/sqlload/issues/19)) ([9468a60](https://github.com/sqlstack-io/sqlload/commit/9468a608960bc1c2ba07b5b5f72a4040f33f1602))
* **snowflake:** direct loads for every Parquet type, JSON, NDJSON and CSV; TIMESTAMP(9); smaller staged parts uploaded in parallel ([#32](https://github.com/sqlstack-io/sqlload/issues/32)) ([93f963c](https://github.com/sqlstack-io/sqlload/commit/93f963c6ce3ce49a2511b6857384a9a7ec8dc72b))
* **snowflake:** files Snowflake can read are loaded only by its own COPY (hmac masking is the one exception, warned in plan); copy_to_internal_stage uploads files no stage covers unchanged to @~ ([73b9fe0](https://github.com/sqlstack-io/sqlload/commit/73b9fe04cc921e906ccb5dcc60039df8d7e3e45d))
* **snowflake:** load_method is required on a Snowflake connection and replaces stage; with no load_method the plan fails and names the choices ([73b9fe0](https://github.com/sqlstack-io/sqlload/commit/73b9fe04cc921e906ccb5dcc60039df8d7e3e45d))
* **snowflake:** replace, --reload and batches too large for one request stay in SQL on the direct route ([0a1fc72](https://github.com/sqlstack-io/sqlload/commit/0a1fc7246ecd99b9f019a8acd377c674dce9d351))


### Bug Fixes

* **duckdb:** append with INSERT from Arrow, so DuckDB converts and checks JSON and UUIDs itself ([#28](https://github.com/sqlstack-io/sqlload/issues/28)) ([b2c996a](https://github.com/sqlstack-io/sqlload/commit/b2c996a4f9339500c214ed230057807cf4a41b11))
* **files:** a files source drops the loader's own columns from its files (case-insensitive) and notes it in plan ([#23](https://github.com/sqlstack-io/sqlload/issues/23)) ([e7a94b9](https://github.com/sqlstack-io/sqlload/commit/e7a94b9fc3055f64f811136ae8a7971e4953900f))
* **loader:** replace and reload build the table and swap it in, rows written once ([#26](https://github.com/sqlstack-io/sqlload/issues/26)) ([5607505](https://github.com/sqlstack-io/sqlload/commit/5607505bb8dd8a6cc32d8f8fe8eaa71d31fdc5e0))
* **snowflake:** JSON strings keep their quotes when a batch goes through a temporary table (hashed masking) ([0a1fc72](https://github.com/sqlstack-io/sqlload/commit/0a1fc7246ecd99b9f019a8acd377c674dce9d351))


### Performance Improvements

* **postgres:** read large whole tables over two connections in one snapshot, by page range ([#30](https://github.com/sqlstack-io/sqlload/issues/30)) ([a146bce](https://github.com/sqlstack-io/sqlload/commit/a146bce4554ab223942b9ba8fd8f7d9e31272ca3))


### Refactoring

* **adapters:** warehouse adapters modelled on sqlbuild, behind SqlAdapter and TableWriter ([#31](https://github.com/sqlstack-io/sqlload/issues/31)) ([9a7fc09](https://github.com/sqlstack-io/sqlload/commit/9a7fc095ed300209081ac9cd213e5ad9454ccd77))

## [0.4.0](https://github.com/sqlstack-io/sqlload/compare/v0.3.0...v0.4.0) (2026-11-04)


### Features

* **loader:** files sources read NDJSON, JSON and CSV, gzip or zstd compressed ([#18](https://github.com/sqlstack-io/sqlload/issues/18)) ([d12fd41](https://github.com/sqlstack-io/sqlload/commit/d12fd419800195234ca8e505e971483ff7b55141))
* **loader:** files targets - write Parquet to S3 or a directory ([#14](https://github.com/sqlstack-io/sqlload/issues/14)) ([f434e65](https://github.com/sqlstack-io/sqlload/commit/f434e65f25812deca6818f622fdf2b997cb29807))
* sqlload debug - validate config and test every connection ([#16](https://github.com/sqlstack-io/sqlload/issues/16)) ([8113e75](https://github.com/sqlstack-io/sqlload/commit/8113e751d17f2a8d1528f0a4ab1534ee2ad27a1b))

## [0.3.0](https://github.com/sqlstack-io/sqlload/compare/v0.2.0...v0.3.0) (2026-11-04)


### Features

* **loader:** files sources - load Parquet from S3 or a directory ([#12](https://github.com/sqlstack-io/sqlload/issues/12)) ([3978c0a](https://github.com/sqlstack-io/sqlload/commit/3978c0a0c507651ca11c38000e50e393a28f02d2))
* **loader:** hmac column masking keyed by mask_key; normalise cursor flags; guard restarts without a cursor column ([#11](https://github.com/sqlstack-io/sqlload/issues/11)) ([99c88a2](https://github.com/sqlstack-io/sqlload/commit/99c88a2c75fdecc27588ce7d391c2ae7d09d3678))
* **loader:** load Postgres tables into DuckDB and Snowflake ([#8](https://github.com/sqlstack-io/sqlload/issues/8)) ([92be7af](https://github.com/sqlstack-io/sqlload/commit/92be7afc5e87befa4d2878cf38dad70e03a5af62))
* **loader:** write SQL ported from sqlbuild - MERGE for merge, SWAP WITH for Snowflake replace ([#13](https://github.com/sqlstack-io/sqlload/issues/13)) ([18ce209](https://github.com/sqlstack-io/sqlload/commit/18ce2092dc48ada468004fccae536a3656d3879b))

## [0.2.0](https://github.com/sqlstack-io/sqlload/compare/v0.1.0...v0.2.0) (2026-11-03)


### Features

* one sqlload crate with the executable behind a default cli feature; publish it to crates.io ([#5](https://github.com/sqlstack-io/sqlload/issues/5)) ([03582d4](https://github.com/sqlstack-io/sqlload/commit/03582d4e24d3f48f2c5c1dd03c4321948d0a68d0))


### Bug Fixes

* commit Kafka offsets only after Snowpipe reports the files loaded ([#7](https://github.com/sqlstack-io/sqlload/issues/7)) ([efecf14](https://github.com/sqlstack-io/sqlload/commit/efecf142110ba7b56f1813bff7c0f7be065a4323))

## [0.1.0](https://github.com/sqlstack-io/sqlload/compare/v0.0.1...v0.1.0) (2026-11-03)


### Features

* Kafka to Snowflake exporter ([0ab6749](https://github.com/sqlstack-io/sqlload/commit/0ab67491e5b598b2e1d7028ea9d5a72abf9b676d))
