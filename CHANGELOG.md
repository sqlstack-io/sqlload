# Changelog

## [0.71.0](https://github.com/sqlstack-io/sqlload/compare/v0.70.4...v0.71.0) (2026-11-18)


### ⚠ BREAKING CHANGES

* **plan:** a project that pins a type narrower than a loaded table's column, or a widening type that doesn't hold every value of the column, now fails `plan` and `load` for that table until the pin is changed or the table rebuilt.

### Bug Fixes

* **plan:** refuse a column pin narrower than the table's column ([#325](https://github.com/sqlstack-io/sqlload/issues/325)) ([885ad93](https://github.com/sqlstack-io/sqlload/commit/885ad93b45908719b4b919c64a342d8e8429188a))


### Documentation

* **site:** retitle the home page's problem section ([#326](https://github.com/sqlstack-io/sqlload/issues/326)) ([f39a757](https://github.com/sqlstack-io/sqlload/commit/f39a75739ae78b4c5e8f9121c38e0c8a9f8787e4))

## [0.70.4](https://github.com/sqlstack-io/sqlload/compare/v0.70.3...v0.70.4) (2026-11-18)


### Documentation

* home page hero with tabs of real sqlload output ([#321](https://github.com/sqlstack-io/sqlload/issues/321)) ([479d475](https://github.com/sqlstack-io/sqlload/commit/479d4756eaa83ceacb0a0ee2fd7588635e435961))
* lead the README with the home page's message and its systems table ([#324](https://github.com/sqlstack-io/sqlload/issues/324)) ([cdf989a](https://github.com/sqlstack-io/sqlload/commit/cdf989aaa361a78fdf0b37592546764f0a80964a))

## [0.70.3](https://github.com/sqlstack-io/sqlload/compare/v0.70.2...v0.70.3) (2026-11-18)


### Bug Fixes

* **cli:** name every warehouse the exporter writes to in tables --help ([#316](https://github.com/sqlstack-io/sqlload/issues/316)) ([8f196eb](https://github.com/sqlstack-io/sqlload/commit/8f196eb7f9abc1add1e6516f8daeec9f048e0749))


### Documentation

* print the home page plan example's error reason on one line ([#320](https://github.com/sqlstack-io/sqlload/issues/320)) ([e004d4e](https://github.com/sqlstack-io/sqlload/commit/e004d4edf9dff61f719f49146ce3d80e13f9170f))

## [0.70.2](https://github.com/sqlstack-io/sqlload/compare/v0.70.1...v0.70.2) (2026-11-18)


### Documentation

* show table notes as small print ([#318](https://github.com/sqlstack-io/sqlload/issues/318)) ([a50e832](https://github.com/sqlstack-io/sqlload/commit/a50e83220aea285e851571a291d3db849b1a0730))

## [0.70.1](https://github.com/sqlstack-io/sqlload/compare/v0.70.0...v0.70.1) (2026-11-18)


### Documentation

* bring the docs up to date with main, and mark BigQuery and Databricks beta ([#314](https://github.com/sqlstack-io/sqlload/issues/314)) ([c57fab0](https://github.com/sqlstack-io/sqlload/commit/c57fab0bc6360e63f6ace142d7a7483ac77283ad))

## [0.70.0](https://github.com/sqlstack-io/sqlload/compare/v0.69.1...v0.70.0) (2026-11-18)


### Features

* **loader:** memory_limit = "off" runs without a budget, keeping DuckDB's row-group check against its own memory_limit ([e8c44f3](https://github.com/sqlstack-io/sqlload/commit/e8c44f38a475a2c593c4ff2bba7481babf52975c))
* **loader:** the memory watchdog stops the whole run at 95% when no table can be stopped ([e8c44f3](https://github.com/sqlstack-io/sqlload/commit/e8c44f38a475a2c593c4ff2bba7481babf52975c))

## [0.69.1](https://github.com/sqlstack-io/sqlload/compare/v0.69.0...v0.69.1) (2026-11-18)


### Bug Fixes

* **loader:** check newest_by against what each target can order, and in Python tables ([#309](https://github.com/sqlstack-io/sqlload/issues/309)) ([c824cf2](https://github.com/sqlstack-io/sqlload/commit/c824cf24fba05716548f60a52e092aa1dc3cf8e1))

## [0.69.0](https://github.com/sqlstack-io/sqlload/compare/v0.68.0...v0.69.0) (2026-11-18)


### Features

* **exporter:** export Kafka topics to BigQuery ([#300](https://github.com/sqlstack-io/sqlload/issues/300)) ([c001276](https://github.com/sqlstack-io/sqlload/commit/c0012765e4baa0b46bd79d157115967793a9c5ab))


### Bug Fixes

* **loader:** load stops before writing when the memory budget holds no table ([#307](https://github.com/sqlstack-io/sqlload/issues/307)) ([8c85c26](https://github.com/sqlstack-io/sqlload/commit/8c85c262f0383defddaed963e5628b70bd8e7346))

## [0.68.0](https://github.com/sqlstack-io/sqlload/compare/v0.67.0...v0.68.0) (2026-11-18)


### ⚠ BREAKING CHANGES

* **loader:** a run stays under one memory budget (memory_limit or --memory-limit, else 80% of a container's or Windows job object's limit, else 50% of the machine's memory), and plan fails when the budget can't hold one table; plan -v shows the budget on a memory line, which plain plan shows only when it asks for something, and plan no longer prints DuckDB's memory note per table. DuckDB and MotherDuck targets open with a memory_limit and thread count from the budget and without automatic checkpoints, and commit large rows in pieces. An ODBC source's default fetch buffer is at most a quarter of each table's share of the budget, and its plan warning compares each fetch with that share. A merge with newest_by cut into pieces counts the older rows it skips per piece, so the "older rows skipped" count can differ from one whole commit's. A merge that refuses repeated keys (on_duplicate_keys = "fail") and a table with checks it doesn't rebuild aren't cut into pieces, and stop, naming the memory_limit they need, when one commit's large values don't fit DuckDB's share. Every append into DuckDB or MotherDuck under a budget creates an _sqlload_unfinished table beside _sqlload_runs.

### Features

* **loader:** one memory budget per run, DuckDB commits in pieces, and a memory watchdog ([3437106](https://github.com/sqlstack-io/sqlload/commit/3437106088227628a9c025ebc05d2c832dc9f013))

## [0.67.0](https://github.com/sqlstack-io/sqlload/compare/v0.66.0...v0.67.0) (2026-11-17)


### ⚠ BREAKING CHANGES

* **loader:** a unique_key, replace_key, cursor_column or newest_by that names a column in neither its source spelling nor its loaded name, including in another case, now fails plan with the nearest column; use the spelling it suggests.

### Bug Fixes

* **loader:** a table whose column names were numbered loads again beside other table overrides ([#302](https://github.com/sqlstack-io/sqlload/issues/302)) ([8481e06](https://github.com/sqlstack-io/sqlload/commit/8481e0606a066e8c6fea789ea0abeecda62e8d9c))
* **loader:** plan checks the columns table settings name, and a load with plan errors no longer reads as completed ([#303](https://github.com/sqlstack-io/sqlload/issues/303)) ([fd20aec](https://github.com/sqlstack-io/sqlload/commit/fd20aec2a1c520f2f8ff53274f792c5f6d3075e5))

## [0.66.0](https://github.com/sqlstack-io/sqlload/compare/v0.65.0...v0.66.0) (2026-11-17)


### ⚠ BREAKING CHANGES

* **exporter:** staging.s3.auto_ingest needs destination.warehouse, which runs the COPY_HISTORY check; plan refuses the configuration without it. Add a warehouse, ideally a dedicated X-Small with a 60-second AUTO_SUSPEND.

### Features

* **cli:** sqlload playground, and a quickstart whose output comes from real runs ([#297](https://github.com/sqlstack-io/sqlload/issues/297)) ([91728f0](https://github.com/sqlstack-io/sqlload/commit/91728f08459336cf26b32b57b0fbe725bf1071d8))
* **exporter:** export Kafka topics to Databricks ([#299](https://github.com/sqlstack-io/sqlload/issues/299)) ([b389528](https://github.com/sqlstack-io/sqlload/commit/b389528ed47b2ffd043cd0ddf7f3208ce3990b4d))


### Bug Fixes

* **exporter:** name staged files after the exporter's run, so a replay after DELETE, a re-created table or a consumer group reset can no longer be skipped as already loaded while its offsets are committed (Snowflake COPY, Snowpipe insertFiles and auto-ingest, Databricks COPY INTO), and deployments sharing a bucket path no longer overwrite each other's files ([8f091d4](https://github.com/sqlstack-io/sqlload/commit/8f091d42e65daf0d1aae51b05ef2641694e5c9ea))
* **exporter:** Snowpipe auto-ingest commits offsets only once COPY_HISTORY shows the files loaded, instead of when the upload succeeds ([8f091d4](https://github.com/sqlstack-io/sqlload/commit/8f091d42e65daf0d1aae51b05ef2641694e5c9ea))

## [0.65.0](https://github.com/sqlstack-io/sqlload/compare/v0.64.0...v0.65.0) (2026-11-17)


### Features

* **load:** batch_concurrency reads several batch_size windows of a Postgres table at once, over connections sharing one snapshot, and commits them in window order ([53514f7](https://github.com/sqlstack-io/sqlload/commit/53514f7b8f155b5f64dc848257d1aa0ad5fecebb))


### Bug Fixes

* **files:** cursor_start reaches a files target's cursor windows as cursor text, so batch_size windows on a timestamp cursor plan with a {cursor:date} path ([53514f7](https://github.com/sqlstack-io/sqlload/commit/53514f7b8f155b5f64dc848257d1aa0ad5fecebb))

## [0.64.0](https://github.com/sqlstack-io/sqlload/compare/v0.63.1...v0.64.0) (2026-11-17)


### Features

* **databricks:** a databricks connection that only a source uses needs no volume ([ac8dc4b](https://github.com/sqlstack-io/sqlload/commit/ac8dc4bcfaa6c87b0e1352aef7884eb9aa1e47a2))
* **databricks:** read Databricks tables, views and queries as a source, as Arrow through the Statement Execution API ([ac8dc4b](https://github.com/sqlstack-io/sqlload/commit/ac8dc4bcfaa6c87b0e1352aef7884eb9aa1e47a2))

## [0.63.1](https://github.com/sqlstack-io/sqlload/compare/v0.63.0...v0.63.1) (2026-11-17)


### Bug Fixes

* **s3:** an empty AWS_ENDPOINT or AWS_REGION now counts as unset everywhere ([#292](https://github.com/sqlstack-io/sqlload/issues/292)) ([9c2113e](https://github.com/sqlstack-io/sqlload/commit/9c2113e198a720a1289a7ad7463ceb390fba210a))

## [0.63.0](https://github.com/sqlstack-io/sqlload/compare/v0.62.0...v0.63.0) (2026-11-17)


### ⚠ BREAKING CHANGES

* **files:** every process that writes a local files table must be on 0.63.0 or later, as an earlier release can still swap in a stale table.

### Bug Fixes

* **files:** a local replace or --reload stalled past its lease can no longer swap in a table missing rows ([486d778](https://github.com/sqlstack-io/sqlload/commit/486d77896429e62024081e348ffe6b03f3ec5eec))

## [0.62.0](https://github.com/sqlstack-io/sqlload/compare/v0.61.0...v0.62.0) (2026-11-17)


### ⚠ BREAKING CHANGES

* **files:** files targets record how each commit ended in _sqlload/decided/, which earlier releases ignore: their loads can undo a repair this release decided, deleting rows. Upgrade every process that writes a files table to 0.62.0 or later before any of them loads it again, and don't load it with an earlier release afterwards.

### Bug Fixes

* **files:** a load that stalled after its lease check can no longer commit over another load's repair and delete a partition's files ([dba6e21](https://github.com/sqlstack-io/sqlload/commit/dba6e211e65dcc4ace6a890392e8af56659f3966))

## [0.61.0](https://github.com/sqlstack-io/sqlload/compare/v0.60.0...v0.61.0) (2026-11-17)


### ⚠ BREAKING CHANGES

* **files:** files-target records of loaded source files use a new format. A plan or load that finds a record written by an earlier release blocks the table and names --reload, which rewrites its files and records.

### Features

* **files:** files-target records group loaded source files by partition, merge once a table has many, and stop a load planned before another committed ([c88fc08](https://github.com/sqlstack-io/sqlload/commit/c88fc08ee4eddcdcdd21cb5b0497b93f2cbc6800))


### Bug Fixes

* **files:** a load that lost its lease can no longer have files it stores after repair count as written ([c88fc08](https://github.com/sqlstack-io/sqlload/commit/c88fc08ee4eddcdcdd21cb5b0497b93f2cbc6800))

## [0.60.0](https://github.com/sqlstack-io/sqlload/compare/v0.59.0...v0.60.0) (2026-11-16)


### ⚠ BREAKING CHANGES

* **clickhouse:** on ClickHouse, a merge or append read in windows or cursor order commits at the end of its run instead of after each window or batch, and a merge also commits every 10 million waiting rows. A stopped run publishes nothing after its last commit point, and the next run reads those rows again; split long backfills with --end-cursor.

### Features

* **clickhouse:** publish a merge or append read in windows or cursor order once per run, a merge also every 10 million rows ([aa07ba3](https://github.com/sqlstack-io/sqlload/commit/aa07ba3302c0036b756f65ec9ab6c79c2c1e2d11))


### Performance Improvements

* **clickhouse:** fill a partitioned replace or --reload through one sorted insert, so a large first load makes about one part per partition ([aa07ba3](https://github.com/sqlstack-io/sqlload/commit/aa07ba3302c0036b756f65ec9ab6c79c2c1e2d11))


### Documentation

* **databricks:** ODBC, BigQuery and SFTP sources into Databricks tested live ([#281](https://github.com/sqlstack-io/sqlload/issues/281)) ([7f0d6d8](https://github.com/sqlstack-io/sqlload/commit/7f0d6d88e199ad68427c5854020043841a045171))

## [0.59.0](https://github.com/sqlstack-io/sqlload/compare/v0.58.0...v0.59.0) (2026-11-16)


### Features

* **bigquery:** BigQuery target out of preview, with deletes and deletes_from ([45cc570](https://github.com/sqlstack-io/sqlload/commit/45cc570c4821935e26bdde387fecbc57a95c1d9d))
* **databricks:** deletes and deletes_from load into Databricks ([45cc570](https://github.com/sqlstack-io/sqlload/commit/45cc570c4821935e26bdde387fecbc57a95c1d9d))


### Bug Fixes

* **bigquery:** replace and rebuilds keep NUMERIC(p,s) and STRING(n) column parameters ([45cc570](https://github.com/sqlstack-io/sqlload/commit/45cc570c4821935e26bdde387fecbc57a95c1d9d))

## [0.58.0](https://github.com/sqlstack-io/sqlload/compare/v0.57.0...v0.58.0) (2026-11-16)


### ⚠ BREAKING CHANGES

* **cli:** sqlload plan, and the plan summary load and apply print, show only the lines a table has of its own or that ask for action. Use plan -v for the previous output. The closing line no longer counts selected tables.

### Features

* **cli:** plan shows each table's own lines grouped by source; -v shows every line ([5c13a38](https://github.com/sqlstack-io/sqlload/commit/5c13a3892a9e40659117ac70ffc7d5e9ec17783e))

## [0.57.0](https://github.com/sqlstack-io/sqlload/compare/v0.56.0...v0.57.0) (2026-11-16)


### Features

* **motherduck:** mark deleted rows with deletes and deletes_from on MotherDuck ([32b17cc](https://github.com/sqlstack-io/sqlload/commit/32b17cc57d96a72ecc6ad422de4ca50fdfd69d94))

## [0.56.0](https://github.com/sqlstack-io/sqlload/compare/v0.55.0...v0.56.0) (2026-11-16)


### Features

* **snowflake:** mark deleted rows with deletes and deletes_from on Snowflake ([1eb5bc3](https://github.com/sqlstack-io/sqlload/commit/1eb5bc32578684a1114411dd44b8c11b4d76f270))


### Bug Fixes

* **files:** say a files source into a files target reads from the newest maximum, as it does ([1eb5bc3](https://github.com/sqlstack-io/sqlload/commit/1eb5bc32578684a1114411dd44b8c11b4d76f270))
* **postgres_cdc:** wait for a stopped load's session to release the slot ([1eb5bc3](https://github.com/sqlstack-io/sqlload/commit/1eb5bc32578684a1114411dd44b8c11b4d76f270))
* **python:** keep everything a run writes to stdout off the --json document ([1eb5bc3](https://github.com/sqlstack-io/sqlload/commit/1eb5bc32578684a1114411dd44b8c11b4d76f270))


### Documentation

* what SQLLoad loads, ODBC databases by name, every files format, Kafka, and ClickHouse out of preview ([#272](https://github.com/sqlstack-io/sqlload/issues/272)) ([aa6e839](https://github.com/sqlstack-io/sqlload/commit/aa6e839d763c4490b863d7b3a6827add4df5306b))

## [0.55.0](https://github.com/sqlstack-io/sqlload/compare/v0.54.0...v0.55.0) (2026-11-16)


### ⚠ BREAKING CHANGES

* **loader:** deletes and deletes_from fail plan on MotherDuck, Snowflake, Databricks, BigQuery, ClickHouse and files targets. The tombstone table loads by delete_insert instead of append. A tombstone cursor of a different kind from the table's cursor fails plan.
* **files:** on S3 and SFTP, on_empty = "skip" fails a table that has never loaded when its folder holds no files.
* **http:** key_path on a request that fills more than one placeholder fails plan. Rebuilt rows create their columns in the API's key order rather than alphabetically.
* **python:** a dict-row column of bytes values is created as binary instead of text, holding the bytes rather than their Python repr; in a text column they are base64. A table loaded before needs --reload, or columns = { name = "text" }.

### Features

* **loader:** refuse deletes on targets whose mark statements haven't run, read late tombstones with deletes_from lookback, and compare tombstone cursor types ([1aa31ca](https://github.com/sqlstack-io/sqlload/commit/1aa31cae7e61ddb120fee7dd3b56b8239e660a13))
* **postgres:** name the unique indexes of a table without a primary key when unique_key = "{primary_key}" fails ([1aa31ca](https://github.com/sqlstack-io/sqlload/commit/1aa31cae7e61ddb120fee7dd3b56b8239e660a13))


### Bug Fixes

* **files:** refuse an empty S3 or SFTP folder under on_empty = "skip" until its table exists ([1aa31ca](https://github.com/sqlstack-io/sqlload/commit/1aa31cae7e61ddb120fee7dd3b56b8239e660a13))
* **http:** count list indexes from the end in select and pointers, keep rebuilt rows in the API's key order, and refuse key_path over several placeholders ([1aa31ca](https://github.com/sqlstack-io/sqlload/commit/1aa31cae7e61ddb120fee7dd3b56b8239e660a13))
* **python:** load bytes values in dict rows as binary ([1aa31ca](https://github.com/sqlstack-io/sqlload/commit/1aa31cae7e61ddb120fee7dd3b56b8239e660a13))


### Documentation

* show measured paging speed with and without read_ahead ([1aa31ca](https://github.com/sqlstack-io/sqlload/commit/1aa31cae7e61ddb120fee7dd3b56b8239e660a13))

## [0.54.0](https://github.com/sqlstack-io/sqlload/compare/v0.53.0...v0.54.0) (2026-11-16)


### ⚠ BREAKING CHANGES

* **files:** Google Sheets tables gain a _sqlload_source_row column, and a header column of that name fails plan. A Sheets cell holding an empty text loads as NULL and no longer makes its column text.
* **files:** delete_insert tables on a partition cursor that hold rows loaded before _sqlload_source_version was recorded fail on a below-range file until rebuilt with --reload, and a new file below the range now fails when the table holds files of its partition that were removed from the folder.
* **loader:** dedup_sort is removed; use newest_by = "column" (a higher value is newer). A merge with newest_by no longer overwrites a loaded row with an older staged one.
* **python:** NaN and infinite floats in dict rows no longer load as NULL. They load as floats, or fail the table where the column can't hold them; set on_nonfinite = "null" to load them as NULL.
* **files:** text after a closing quote now fails the file instead of loading a misread value, and CSV files read as latin-1, cp1252 or iso-8859-15 whose bytes say they are UTF-8 or cp1252 fail unless encoding_check = false. Such files no longer load directly into Snowflake from an external stage while the check is on.

### Features

* **duckdb:** load json columns as VARIANT with json_type = "variant" ([c7ab321](https://github.com/sqlstack-io/sqlload/commit/c7ab3213f129fb092b071e29fbde61e371b98001))
* **files:** choose a files source's files by glob with pattern ([c7ab321](https://github.com/sqlstack-io/sqlload/commit/c7ab3213f129fb092b071e29fbde61e371b98001))
* **files:** CSV escape, quote = false, strict closing quotes and encoding checks ([c7ab321](https://github.com/sqlstack-io/sqlload/commit/c7ab3213f129fb092b071e29fbde61e371b98001))
* **files:** move or delete ZIP archives with after_load once every member read from them commits ([c7ab321](https://github.com/sqlstack-io/sqlload/commit/c7ab3213f129fb092b071e29fbde61e371b98001))
* **files:** order files by path with order = "path", skip existing empty folders with on_empty = "skip", number Google Sheets rows in _sqlload_source_row, and load an empty Sheets cell as NULL ([c7ab321](https://github.com/sqlstack-io/sqlload/commit/c7ab3213f129fb092b071e29fbde61e371b98001))
* **loader:** keep the newest row of a key across loads with newest_by, which replaces dedup_sort ([c7ab321](https://github.com/sqlstack-io/sqlload/commit/c7ab3213f129fb092b071e29fbde61e371b98001))
* **loader:** load every inferred column as text with infer_types = false ([c7ab321](https://github.com/sqlstack-io/sqlload/commit/c7ab3213f129fb092b071e29fbde61e371b98001))
* **loader:** mark the rows whose key the source no longer holds with deletes and load --sync-deletes, or that a tombstone table names with deletes_from ([c7ab321](https://github.com/sqlstack-io/sqlload/commit/c7ab3213f129fb092b071e29fbde61e371b98001))
* **plan:** state what a DuckDB table's peak memory scales with, in plan and the DuckDB docs ([c7ab321](https://github.com/sqlstack-io/sqlload/commit/c7ab3213f129fb092b071e29fbde61e371b98001))
* **python:** keep NaN and infinite floats from dict rows, or load them as NULL with on_nonfinite = "null" ([c7ab321](https://github.com/sqlstack-io/sqlload/commit/c7ab3213f129fb092b071e29fbde61e371b98001))
* **sources:** narrow a database table's reads with where, keeping its primary key and include patterns ([c7ab321](https://github.com/sqlstack-io/sqlload/commit/c7ab3213f129fb092b071e29fbde61e371b98001))


### Bug Fixes

* **files:** fail delete_insert on a below-range file held without a version or in a partition whose files left the folder ([c7ab321](https://github.com/sqlstack-io/sqlload/commit/c7ab3213f129fb092b071e29fbde61e371b98001))


### Performance Improvements

* **python:** run an sl.http job's page transform in one crossing instead of three threads per page ([c7ab321](https://github.com/sqlstack-io/sqlload/commit/c7ab3213f129fb092b071e29fbde61e371b98001))

## [0.53.0](https://github.com/sqlstack-io/sqlload/compare/v0.52.0...v0.53.0) (2026-11-16)


### ⚠ BREAKING CHANGES

* **names:** tables and columns load under snake_case names by default; set naming = "source" to keep the source's names, and rebuild existing tables with --reload. naming = "dlt" is removed: use compat = "dlt", which naming's default already matches. Repeated headers, which failed plan, are now numbered.
* **flatten:** under compat = "dlt", JSON and NDJSON files default to flatten = "all" and, on a warehouse, unnest = "all", so their lists load as child tables. A transform on such files gets objects as read and its output is flattened. A json pin, a mask, or an exclude_columns or allow_columns name on an object keeps it whole instead of failing plan, and a listed flatten path may now be dotted.
* **http:** sl.cells no longer takes key, names_from, name_key, id_key or header_row, and name is required: name=sl.listed("$.columns") names columns the response lists, sl.listed("$.columns", by_position=True) names them by position, and sl.header_row() takes the first row's cells.
* **parse:** sl.field(parse={"timestamp": format}) with a format without an offset loads timestamp_tz, the time read in the table's timezone or UTC, instead of timestamp_ntz. A %s format is refused; use epoch_s.
* **cursor:** rows a Python table's function returns outside the cursor window it was asked for fail the table by default; set on_cursor_out_of_range = "skip" to drop them. With timezone set, cursor_start and cursor flags without an offset are read in that zone rather than UTC, and ctx.cursor_value and ctx.cursor_end arrive in it.
* **odbc:** where an ODBC driver compares names case-sensitively, an include pattern whose own lookup finds a table no longer also finds a table whose name differs from it only in case (erp.Items beside erp.items); name each such table in include.
* **send:** MotherDuck, Snowflake and Databricks targets default to send = "at_commit", so their tables' rows reach the warehouse at each commit rather than batch by batch as they are read; set send = "per_batch" on the target, source or table to send each batch as it is read.

### Features

* **cursor:** check loaded rows against the cursor window asked for, and read times without a zone in the source's timezone ([144b4da](https://github.com/sqlstack-io/sqlload/commit/144b4da1395c0e340f97e4d83d4ce54abf19ad12))
* **flatten:** flatten nested objects on every source by dotted path or "all", list them in plan, and keep an object whole with a json pin ([144b4da](https://github.com/sqlstack-io/sqlload/commit/144b4da1395c0e340f97e4d83d4ce54abf19ad12))
* **http:** name cells from the data with sl.cells(name=...), name the repeated page a paginator returns, and fail on errors in 200 bodies with error_path ([144b4da](https://github.com/sqlstack-io/sqlload/commit/144b4da1395c0e340f97e4d83d4ce54abf19ad12))
* **names:** snake_case names by default, rename by map or [@sl](https://github.com/sl).rename function, number duplicate names, and compat = "dlt" for dlt's columns ([144b4da](https://github.com/sqlstack-io/sqlload/commit/144b4da1395c0e340f97e4d83d4ce54abf19ad12))
* **parse:** parse text columns on every source with a list of formats tried in turn, and tz for times without a zone ([144b4da](https://github.com/sqlstack-io/sqlload/commit/144b4da1395c0e340f97e4d83d4ce54abf19ad12))
* **plan:** show each connection and table discovery with its time as plan runs, with RUNNING rows for long steps and connected and discovered events ([144b4da](https://github.com/sqlstack-io/sqlload/commit/144b4da1395c0e340f97e4d83d4ce54abf19ad12))
* **send:** hold a commit's rows locally and send them in one burst, by default on targets billed while they run queries ([144b4da](https://github.com/sqlstack-io/sqlload/commit/144b4da1395c0e340f97e4d83d4ce54abf19ad12))


### Bug Fixes

* **names:** drop a Python table's excluded fields by the names they arrive under, so a camelCase or flattened field matching exclude_columns never loads ([144b4da](https://github.com/sqlstack-io/sqlload/commit/144b4da1395c0e340f97e4d83d4ce54abf19ad12))
* **odbc:** ask the driver only for the included tables, and read primary keys only when a table needs them ([144b4da](https://github.com/sqlstack-io/sqlload/commit/144b4da1395c0e340f97e4d83d4ce54abf19ad12))
* **website:** draw the SQLLoad mark at full size in the header and favicon ([#265](https://github.com/sqlstack-io/sqlload/issues/265)) ([0279211](https://github.com/sqlstack-io/sqlload/commit/02792112fb7c6535b8ea5188fc3cc1d0694c71db))


### Maintenance

* **website:** use the updated SQLStack mark in the footer credit ([#264](https://github.com/sqlstack-io/sqlload/issues/264)) ([81b8e26](https://github.com/sqlstack-io/sqlload/commit/81b8e26ac99836e4191170c7fa23041102af1cd7))

## [0.52.0](https://github.com/sqlstack-io/sqlload/compare/v0.51.0...v0.52.0) (2026-11-15)


### Features

* **cursor:** typed --start-cursor-ts/-int and --end-cursor-ts/-int flags as in SQLBuild, and an integer lookback on integer cursors ([e88a049](https://github.com/sqlstack-io/sqlload/commit/e88a049cc7d37e74cb5524bff3cb4e6f7d1feb79))
* **load:** load --follow prints the same plan line, table lines and live block as load ([e88a049](https://github.com/sqlstack-io/sqlload/commit/e88a049cc7d37e74cb5524bff3cb4e6f7d1feb79))
* **load:** report peak memory per run, and per table on Linux with --memory-per-table ([e88a049](https://github.com/sqlstack-io/sqlload/commit/e88a049cc7d37e74cb5524bff3cb4e6f7d1feb79))
* **odbc:** read long columns value by value under a per-table fetch buffer, and show it in plan ([e88a049](https://github.com/sqlstack-io/sqlload/commit/e88a049cc7d37e74cb5524bff3cb4e6f7d1feb79))
* **python:** plan checks sl.http field paths written as literals, so a bad path fails plan instead of load ([e88a049](https://github.com/sqlstack-io/sqlload/commit/e88a049cc7d37e74cb5524bff3cb4e6f7d1feb79))
* **python:** transform files, Sheets ranges and HTTP pages with a named [@sl](https://github.com/sl).transform function before they load, and --reload merge and move_to files tables ([e88a049](https://github.com/sqlstack-io/sqlload/commit/e88a049cc7d37e74cb5524bff3cb4e6f7d1feb79))


### Bug Fixes

* **cli:** write log lines without colour codes when stderr is not a terminal or NO_COLOR is set ([e88a049](https://github.com/sqlstack-io/sqlload/commit/e88a049cc7d37e74cb5524bff3cb4e6f7d1feb79))

## [0.51.0](https://github.com/sqlstack-io/sqlload/compare/v0.50.0...v0.51.0) (2026-11-15)


### ⚠ BREAKING CHANGES

* **merge:** merges match a NULL key part with a NULL. On a Postgres target, a merge table whose key column can be NULL (every Python and files table, and database columns without NOT NULL) fails plan until its unique index is recreated NULLS NOT DISTINCT, or null_keys = "fail" is set; Postgres 14 or older needs null_keys = "fail" or NOT NULL key columns. A key column an existing Postgres table declares NOT NULL needs neither. unique_key = "{primary_key}" with a nullable reported key column (including most BigQuery sources) fails plan; name the key columns. Repeated NULL keys in one commit collapse to one row, and on_duplicate_keys = "fail" fails on them; a files merge without _sqlload_source_row fails on a file repeating a key with NULLs in it. --reload of a merge with null_keys = "fail" fails on a NULL key. Tables that already hold copies from this bug keep them; see "NULLs in keys" under merge in the write strategies docs for the cleanup.
* **delete_insert:** delete_insert with replace_key and a cursor_column fails plan; drop cursor_column to read the whole source each run, or use merge. A files delete_insert by replace_key fails when a file holds a key value that another file still in the folder holds, in the same run or an earlier one; put each value's rows in one file, or remove the outdated file from the folder. Plan refuses such a table that excludes _sqlload_source_file. Under delete_insert on a partition cursor of a files source, a file that changed since it loaded, or a new file in a partition the table holds, below the range a load reads fails the table instead of being skipped (load it with --start-cursor or --reload, or switch to replace_key), and plan refuses such a table without _sqlload_source_file or _sqlload_source_version.
* **cursor:** an incremental load whose source has rows with a NULL cursor fails instead of skipping them, naming how many. Set null_cursor = "include" (merge) to read them on every run, list a column that is always set to coalesce with the cursor (cursor_column = ["updated_at", "created_at"]), or set null_cursor = "ignore" to keep skipping them. Under the default, each bounded read of a nullable cursor first counts NULL cursors: on BigQuery a query job, which needs bigquery.jobs.create and bills a scan of the cursor column; on Postgres a sequential scan when the column has no index.

### Features

* **cli:** plan and load print what matters for each table, and load streams events ([#253](https://github.com/sqlstack-io/sqlload/issues/253)) ([059a2cc](https://github.com/sqlstack-io/sqlload/commit/059a2ccdf3b28e5c5f893204d6fa39d159b15eaa))
* **delete_insert:** replace rows by replace_key, and fail instead of skipping a changed or new file below the cursor ([a451adf](https://github.com/sqlstack-io/sqlload/commit/a451adf451eab6340a8fc4ff904bed97dc689990))


### Bug Fixes

* **cursor:** fail a load whose source has rows with a NULL cursor, which never loaded after the first run; add null_cursor and coalesced cursor columns ([a451adf](https://github.com/sqlstack-io/sqlload/commit/a451adf451eab6340a8fc4ff904bed97dc689990))
* **merge:** match NULLs in a unique_key, so a row with a NULL key part no longer loads again on every run; null_keys = "fail" refuses NULL keys instead ([a451adf](https://github.com/sqlstack-io/sqlload/commit/a451adf451eab6340a8fc4ff904bed97dc689990))
* **odbc:** preload libstdc++ for MySQL Connector/ODBC on Linux, and name the fix for a driver registered under //odbc/ ([#251](https://github.com/sqlstack-io/sqlload/issues/251)) ([64743fb](https://github.com/sqlstack-io/sqlload/commit/64743fb89672373dc6f2f9a610ed02d38a757718))
* **reload:** take --start-cursor and --end-cursor, so a table with an integer batch_size can be reloaded ([a451adf](https://github.com/sqlstack-io/sqlload/commit/a451adf451eab6340a8fc4ff904bed97dc689990))
* **website:** keep docs tables inside the content column, check layout in CI, and give the Kafka exporter its own section ([#257](https://github.com/sqlstack-io/sqlload/issues/257)) ([afa6de9](https://github.com/sqlstack-io/sqlload/commit/afa6de95117f3e9e848550ddc184f4d7464a4bbf))


### Documentation

* **motherduck:** describe the separate process as users see it ([#258](https://github.com/sqlstack-io/sqlload/issues/258)) ([85ff26a](https://github.com/sqlstack-io/sqlload/commit/85ff26abc768b0a25766cc3d1fd387efa76fca8f))
* **orchestrators:** fix the per-table asset recipes when a table fails or the run can't start ([#256](https://github.com/sqlstack-io/sqlload/issues/256)) ([2570f8d](https://github.com/sqlstack-io/sqlload/commit/2570f8d334f234289ae76d88b100047419d1d850))
* **orchestrators:** give each table its own Prefect or Dagster asset, from the final --json report ([#255](https://github.com/sqlstack-io/sqlload/issues/255)) ([4f92aa8](https://github.com/sqlstack-io/sqlload/commit/4f92aa8978e959b68419695c7b44e6adaaaa5639))

## [0.50.0](https://github.com/sqlstack-io/sqlload/compare/v0.49.0...v0.50.0) (2026-11-14)


### ⚠ BREAKING CHANGES

* **http:** a field or key_path path using [*], .*, a filter, a slice, .. or a bracket in a key name now fails before any request instead of loading NULL (or reading a key literally named with brackets). Use [n] or [-1] for one item, or select the list's items as rows.

### Features

* **http:** read list items by index in field paths, and refuse path syntax that loaded NULL ([#249](https://github.com/sqlstack-io/sqlload/issues/249)) ([64f1025](https://github.com/sqlstack-io/sqlload/commit/64f1025a7825ca60f643e27d11f277d8c8772554))

## [0.49.0](https://github.com/sqlstack-io/sqlload/compare/v0.48.0...v0.49.0) (2026-11-14)


### Features

* **files:** Excel A1 ranges and column_names for headerless data; keep rotated OAuth refresh tokens ([#247](https://github.com/sqlstack-io/sqlload/issues/247)) ([d681cbc](https://github.com/sqlstack-io/sqlload/commit/d681cbc02362895aac852634ff65c5c2b85a0669))

## [0.48.0](https://github.com/sqlstack-io/sqlload/compare/v0.47.0...v0.48.0) (2026-11-14)


### Features

* HTTP read-ahead, field parsing and row context, and `sqll` as a short name ([#245](https://github.com/sqlstack-io/sqlload/issues/245)) ([d481e69](https://github.com/sqlstack-io/sqlload/commit/d481e69bd8bf1cdca19f1f51c0b999fbe6a9a4bd))

## [0.47.0](https://github.com/sqlstack-io/sqlload/compare/v0.46.0...v0.47.0) (2026-11-14)


### Features

* **odbc:** write cursor bounds as ODBC escapes, bind them at the column's digits, and check them at plan ([#243](https://github.com/sqlstack-io/sqlload/issues/243)) ([58f3674](https://github.com/sqlstack-io/sqlload/commit/58f3674708bdb6ab629d6ee8cba181ee58eded9c))

## [0.46.0](https://github.com/sqlstack-io/sqlload/compare/v0.45.0...v0.46.0) (2026-11-14)


### ⚠ BREAKING CHANGES

* **load:** a loader table without write_strategy, on the table or its source, fails plan; before, it took replace without a cursor_column and merge with one and a key. A merge table without unique_key fails plan; before, it merged on the primary key the source reported. Set write_strategy, and unique_key = "{primary_key}" or the key columns.

### Features

* **load:** require an explicit write_strategy and merge key on every loader table ([#241](https://github.com/sqlstack-io/sqlload/issues/241)) ([a476490](https://github.com/sqlstack-io/sqlload/commit/a476490d0485324bc20cca1155866129d631dc2d))

## [0.45.0](https://github.com/sqlstack-io/sqlload/compare/v0.44.0...v0.45.0) (2026-11-14)


### ⚠ BREAKING CHANGES

* **types,files,http:** a list column pinned array(int) or array(decimal(p,s)) over Parquet, Arrow or JSON-text lists fails on an element it would round or cut, instead of loading it changed; an append files table that excludes _sqlload_source_version fails plan; and an sl.http offset run whose first page is short but not empty makes one more request, failing when the API returns rows there.
* **load,kafka,http:** a Kafka stream whose committed offset can't be read stops instead of resuming from start_from; an append files table whose rows have an empty _sqlload_source_version fails until it is loaded with --reload; a pinned decimal or integer column on a Python, BigQuery or files source fails on a value with more decimal places than it keeps instead of rounding or truncating it; and an sl.http paginate or fan_out whose select path is missing from a response fails instead of reading no rows; set select_missing="empty" for APIs that leave out an empty list.

### Bug Fixes

* **load,kafka,http:** fail with clear errors instead of silent fallbacks ([#237](https://github.com/sqlstack-io/sqlload/issues/237)) ([c68a42e](https://github.com/sqlstack-io/sqlload/commit/c68a42e4485dc5b385ee466bf8f34891252ed1d1))
* **types,files,http:** refuse lossy list elements, require the source version, probe short offset pages ([#240](https://github.com/sqlstack-io/sqlload/issues/240)) ([4f3e22d](https://github.com/sqlstack-io/sqlload/commit/4f3e22d94a4ab1a4b7e79c5aae172bbee43da5dd))

## [0.44.0](https://github.com/sqlstack-io/sqlload/compare/v0.43.0...v0.44.0) (2026-11-14)


### ⚠ BREAKING CHANGES

* **load,odbc:** a table with a cursor_column and no key (no unique_key, and no primary key its source reports) no longer defaults to write_strategy = "delete_insert", whatever its source kind (postgres, odbc, snowflake, bigquery, files, python): plan refuses it. Set unique_key to merge on it, or set write_strategy = "delete_insert" (or "append") to keep loading it as before.

### Features

* **load,odbc:** refuse a cursor table without a key instead of defaulting to delete_insert ([#235](https://github.com/sqlstack-io/sqlload/issues/235)) ([97f149a](https://github.com/sqlstack-io/sqlload/commit/97f149a3bf5b548e01e71b338cbb5179d8d24a6c))

## [0.43.0](https://github.com/sqlstack-io/sqlload/compare/v0.42.1...v0.43.0) (2026-11-13)


### Features

* **http,load:** fan-out keys in POST bodies and batches of keys, load --fail-fast, and MySQL Connector/ODBC loader errors ([#233](https://github.com/sqlstack-io/sqlload/issues/233)) ([548f665](https://github.com/sqlstack-io/sqlload/commit/548f665110c4cd86347cb7cc17d1a74f2e9b3dbd))

## [0.42.1](https://github.com/sqlstack-io/sqlload/compare/v0.42.0...v0.42.1) (2026-11-13)


### Bug Fixes

* **files:** take flattened objects and child-table paths from the data, not from splitting names on __ ([#232](https://github.com/sqlstack-io/sqlload/issues/232)) ([01cf6de](https://github.com/sqlstack-io/sqlload/commit/01cf6de62fd0210667a007bfeeba9e967c061b98))
* **unnest:** keep list fields an earlier load chose wherever their child table names put them ([#230](https://github.com/sqlstack-io/sqlload/issues/230)) ([23b7a6c](https://github.com/sqlstack-io/sqlload/commit/23b7a6ccc785f87ee35f7a32caaa454043525678))

## [0.42.0](https://github.com/sqlstack-io/sqlload/compare/v0.41.1...v0.42.0) (2026-11-13)


### ⚠ BREAKING CHANGES

* **files:** under naming = "dlt", a JSON or NDJSON files source's object columns, which held JSON, stop receiving values, and its objects load into new parent__key columns instead. A child table whose list field holds __ is now named with each part (orders__a__b for a__B, not orders__a_b), so it loads into a new table.

### Features

* **files:** dlt naming safety: flatten objects, postgres_cdc under dlt names, drained after_load protection ([#229](https://github.com/sqlstack-io/sqlload/issues/229)) ([3ed131a](https://github.com/sqlstack-io/sqlload/commit/3ed131a3656b4cd852adb7e3242c4b4bde6078ea))
* **unnest:** child tables in more setups ([#227](https://github.com/sqlstack-io/sqlload/issues/227)) ([37dfdb4](https://github.com/sqlstack-io/sqlload/commit/37dfdb4e10cda701ae56bf0c2229da032fe41787))

## [0.41.1](https://github.com/sqlstack-io/sqlload/compare/v0.41.0...v0.41.1) (2026-11-13)


### Bug Fixes

* **unnest:** replace the child rows of merged roots whose ids their keys did not give ([#222](https://github.com/sqlstack-io/sqlload/issues/222)) ([af3f601](https://github.com/sqlstack-io/sqlload/commit/af3f601bd5951ebba8745e35dc9daa1c855c3af5))
* **unnest:** take only real child tables under unnest = "all" ([#223](https://github.com/sqlstack-io/sqlload/issues/223)) ([aaa6a9b](https://github.com/sqlstack-io/sqlload/commit/aaa6a9bb7985650bbb5d7a11dfb1411a01fee49a))

## [0.41.0](https://github.com/sqlstack-io/sqlload/compare/v0.40.0...v0.41.0) (2026-11-13)


### Features

* **unnest:** write child tables on ClickHouse targets ([#216](https://github.com/sqlstack-io/sqlload/issues/216)) ([292e611](https://github.com/sqlstack-io/sqlload/commit/292e6111d6fb710cb28115c4ae66521ab5b87e9d))


### Documentation

* 0.40.0 is released ([#221](https://github.com/sqlstack-io/sqlload/issues/221)) ([5ed4ae8](https://github.com/sqlstack-io/sqlload/commit/5ed4ae88be90dc733deb32e2d9120db5c71bdb18))
* add a guide for moving dlt pipelines to SQLLoad ([#220](https://github.com/sqlstack-io/sqlload/issues/220)) ([9fe8810](https://github.com/sqlstack-io/sqlload/commit/9fe88100a4c5f123ccf1e8f4380f9d2c81def27e))

## [0.40.0](https://github.com/sqlstack-io/sqlload/compare/v0.39.1...v0.40.0) (2026-11-13)


### Features

* **unnest:** child tables from list fields, row ids, and dlt's naming ([#215](https://github.com/sqlstack-io/sqlload/issues/215)) ([a928007](https://github.com/sqlstack-io/sqlload/commit/a9280072f1b2000897ecd8f0a120c109cc2ada5d))


### Documentation

* 0.39.1 is released ([#213](https://github.com/sqlstack-io/sqlload/issues/213)) ([6e798c0](https://github.com/sqlstack-io/sqlload/commit/6e798c068034abb004bb3b2f246acfb82f885f9d))

## [0.39.1](https://github.com/sqlstack-io/sqlload/compare/v0.39.0...v0.39.1) (2026-11-13)


### Bug Fixes

* **files:** a folder after_load emptied skips its table instead of failing the load ([#210](https://github.com/sqlstack-io/sqlload/issues/210)) ([5b74442](https://github.com/sqlstack-io/sqlload/commit/5b7444209e37e28bc737c2d296cb5a08f41fe08c))


### Documentation

* drop the not-released marks for 0.39.0 ([#209](https://github.com/sqlstack-io/sqlload/issues/209)) ([2596ea9](https://github.com/sqlstack-io/sqlload/commit/2596ea9049f5b283ecadee2fcc68bc95c2bf49f5))

## [0.39.0](https://github.com/sqlstack-io/sqlload/compare/v0.38.0...v0.39.0) (2026-11-12)


### Features

* **files:** after_load deletes or moves files once their rows commit ([#204](https://github.com/sqlstack-io/sqlload/issues/204)) ([c50a611](https://github.com/sqlstack-io/sqlload/commit/c50a611d431f8ad2786ea6d9d94964ff4ded0488))
* **merge:** keep one row per repeated key in a merge commit (on_duplicate_keys, dedup_sort) ([#207](https://github.com/sqlstack-io/sqlload/issues/207)) ([8fbeec1](https://github.com/sqlstack-io/sqlload/commit/8fbeec10ce36f36daf6f92be85b4ace2587d7128))
* **odbc:** ODBC in the release wheels, typed cursor bounds, live driver tests on Linux and Windows ([#198](https://github.com/sqlstack-io/sqlload/issues/198)) ([332fa27](https://github.com/sqlstack-io/sqlload/commit/332fa274518b9d0ee8cdc2b13ba7ee1172462854))


### Documentation

* drop the not-released marks for 0.37.0 and 0.38.0; ODBC wheels ship in 0.39.0 ([#205](https://github.com/sqlstack-io/sqlload/issues/205)) ([d55ff1e](https://github.com/sqlstack-io/sqlload/commit/d55ff1e6a6dda6a4dca9735a0ad794985031d184))

## [0.38.0](https://github.com/sqlstack-io/sqlload/compare/v0.37.0...v0.38.0) (2026-11-12)


### Features

* **files:** merge files sources file by file, oldest modified first ([#201](https://github.com/sqlstack-io/sqlload/issues/201)) ([bcc7cca](https://github.com/sqlstack-io/sqlload/commit/bcc7cca80a60fa21c7dcdc9556f44e7432a04d70))

## [0.37.0](https://github.com/sqlstack-io/sqlload/compare/v0.36.0...v0.37.0) (2026-11-12)


### Features

* **http:** cells= pivots rows of cells into typed columns ([#200](https://github.com/sqlstack-io/sqlload/issues/200)) ([2c1b298](https://github.com/sqlstack-io/sqlload/commit/2c1b298947cfe2f4f2ec785723d76248330b37be))
* **http:** multi-level fan-out for sl.http ([#196](https://github.com/sqlstack-io/sqlload/issues/196)) ([6376092](https://github.com/sqlstack-io/sqlload/commit/6376092503112025ebca95ee0f4225882a2e7fc2))
* **loader:** constant columns on any table, for several tenants in one table ([#199](https://github.com/sqlstack-io/sqlload/issues/199)) ([c3f997c](https://github.com/sqlstack-io/sqlload/commit/c3f997c30f88e7621ea760cdd33f838e0782b9ea))
* Windows and macOS Intel wheels and CLI, with Windows CI ([#181](https://github.com/sqlstack-io/sqlload/issues/181)) ([090077f](https://github.com/sqlstack-io/sqlload/commit/090077f4c555d7370ed6151b05ad258e4a813dfe))


### Documentation

* drop the not-released marks for 0.36.0 ([#197](https://github.com/sqlstack-io/sqlload/issues/197)) ([3039010](https://github.com/sqlstack-io/sqlload/commit/30390101e915f27d6378f2452b71c9bc3a873d82))
* **files:** mark 0.35.0 features released; ci(verify): stop cancelling main runs ([#194](https://github.com/sqlstack-io/sqlload/issues/194)) ([1e41a16](https://github.com/sqlstack-io/sqlload/commit/1e41a1614ab575d6c82e0749f66f02c186490d62))

## [0.36.0](https://github.com/sqlstack-io/sqlload/compare/v0.35.0...v0.36.0) (2026-11-12)


### Features

* **http:** OAuth2 password grant for sl.http ([#193](https://github.com/sqlstack-io/sqlload/issues/193)) ([f9960a7](https://github.com/sqlstack-io/sqlload/commit/f9960a7b0aebea5271e7f54c8189887ffa5c90a1))
* **http:** POST with a JSON body, and paging and cursors in the body ([#191](https://github.com/sqlstack-io/sqlload/issues/191)) ([e6ca559](https://github.com/sqlstack-io/sqlload/commit/e6ca559d89048c31323ab2127fde046b573ccfb1))
* **sheets:** read as a Google user through installed-app OAuth ([#190](https://github.com/sqlstack-io/sqlload/issues/190)) ([8f48b05](https://github.com/sqlstack-io/sqlload/commit/8f48b05490239efd8346447c47af0dd67e84d9dc))

## [0.35.0](https://github.com/sqlstack-io/sqlload/compare/v0.34.0...v0.35.0) (2026-11-12)


### Features

* **files:** read CSV in other encodings and skip leading lines ([#185](https://github.com/sqlstack-io/sqlload/issues/185)) ([4fdcd08](https://github.com/sqlstack-io/sqlload/commit/4fdcd08f989b7d1ddebdae08e8f012f4120f1e05))
* **files:** read Excel, XML and ZIP inputs ([#187](https://github.com/sqlstack-io/sqlload/issues/187)) ([2b496b6](https://github.com/sqlstack-io/sqlload/commit/2b496b69b97b38a55ad9b7465cefec95c076e208))


### Documentation

* **cli:** mark exit 75 and retryable as released in 0.34.0 ([#188](https://github.com/sqlstack-io/sqlload/issues/188)) ([121c005](https://github.com/sqlstack-io/sqlload/commit/121c00566b2c8494b7ea863967e665a64e1466ff))
* **motherduck:** drop the not-released-yet marks now that 0.34.0 is published ([#189](https://github.com/sqlstack-io/sqlload/issues/189)) ([9fce673](https://github.com/sqlstack-io/sqlload/commit/9fce673826f1be9df79473048824b0042a4f4d0a))

## [0.34.0](https://github.com/sqlstack-io/sqlload/compare/v0.33.6...v0.34.0) (2026-11-12)


### Features

* **load:** exit 75 and report retryable when a rerun may fix every failure ([#183](https://github.com/sqlstack-io/sqlload/issues/183)) ([74d1abe](https://github.com/sqlstack-io/sqlload/commit/74d1abeef9ea786bcf8a8ee83953b34ba518d6f0))
* **motherduck:** connect through a separate DuckDB 1.5 process (install sqlload[motherduck]), as MotherDuck doesn't accept DuckDB 2.0 clients yet ([#182](https://github.com/sqlstack-io/sqlload/issues/182)) ([2a11162](https://github.com/sqlstack-io/sqlload/commit/2a1116263935728bf6545d116444e69208e6fc82))

## [0.33.6](https://github.com/sqlstack-io/sqlload/compare/v0.33.5...v0.33.6) (2026-11-10)


### Documentation

* list each system once with its source and target support ([#179](https://github.com/sqlstack-io/sqlload/issues/179)) ([a3e319f](https://github.com/sqlstack-io/sqlload/commit/a3e319f7e07fb9ba9b4a689d1a71552faf587921))

## [0.33.5](https://github.com/sqlstack-io/sqlload/compare/v0.33.4...v0.33.5) (2026-11-09)


### Performance Improvements

* **clickhouse:** publish a delete_insert read in windows once per run; a stopped run publishes nothing, so split long backfills with --end-cursor ([#176](https://github.com/sqlstack-io/sqlload/issues/176)) ([164df2b](https://github.com/sqlstack-io/sqlload/commit/164df2b44ae6b04d245b6418a3a42476cbfa0036))

## [0.33.4](https://github.com/sqlstack-io/sqlload/compare/v0.33.3...v0.33.4) (2026-11-09)


### Performance Improvements

* **clickhouse:** send inserts zstd-compressed, and write each insert's rows before it returns ([#175](https://github.com/sqlstack-io/sqlload/issues/175)) ([a7476f0](https://github.com/sqlstack-io/sqlload/commit/a7476f0902a4a960631ebdd8d08ffdb9d651662f))

## [0.33.3](https://github.com/sqlstack-io/sqlload/compare/v0.33.2...v0.33.3) (2026-11-09)


### Performance Improvements

* **clickhouse:** insert the delta ordered by the partition key, so a load makes one part per partition ([#174](https://github.com/sqlstack-io/sqlload/issues/174)) ([7b2529e](https://github.com/sqlstack-io/sqlload/commit/7b2529e1e250be83f90046184ceea8fb52167db2))


### Documentation

* reorganise the docs into Getting Started, Guides, Concepts, Connectors and Reference ([#172](https://github.com/sqlstack-io/sqlload/issues/172)) ([22ea3bc](https://github.com/sqlstack-io/sqlload/commit/22ea3bc6a45f0ccc512492cf81e057da41fb67f6))

## [0.33.2](https://github.com/sqlstack-io/sqlload/compare/v0.33.1...v0.33.2) (2026-11-09)


### Bug Fixes

* drop the pre-release status from the package metadata ([#171](https://github.com/sqlstack-io/sqlload/issues/171)) ([28a274d](https://github.com/sqlstack-io/sqlload/commit/28a274db8005689bd3f712e45414a1a55f4c0f71))


### Documentation

* a short README in the SQLStack style, and the same page on PyPI ([#168](https://github.com/sqlstack-io/sqlload/issues/168)) ([cbb1344](https://github.com/sqlstack-io/sqlload/commit/cbb1344d5d82d942dd332409113f8cb22455ff88))
* **clickhouse:** drop the ReplacingMergeTree(_sqlload_loaded_at) example, which ClickHouse refuses ([#170](https://github.com/sqlstack-io/sqlload/issues/170)) ([e452d56](https://github.com/sqlstack-io/sqlload/commit/e452d56bed751154cdd3ce9a0ae5ff8455c18198))

## [0.33.1](https://github.com/sqlstack-io/sqlload/compare/v0.33.0...v0.33.1) (2026-11-09)


### Bug Fixes

* **clickhouse:** drop a partition the window empties instead of replacing it (ClickHouse 26.6 and later refuse the replace) ([#163](https://github.com/sqlstack-io/sqlload/issues/163)) ([36b45b4](https://github.com/sqlstack-io/sqlload/commit/36b45b45e4ee5c3d25cc092863f5c8f99a76cabc))
* **clickhouse:** keep rebuilding partitions from their kept rows on servers before 25.5 ([#166](https://github.com/sqlstack-io/sqlload/issues/166)) ([4ea0770](https://github.com/sqlstack-io/sqlload/commit/4ea0770425d570f437c45197027d5c93baf80e9a))
* **clickhouse:** read a NULL as NULL, so plan works on an empty table with a cursor ([#167](https://github.com/sqlstack-io/sqlload/issues/167)) ([238364e](https://github.com/sqlstack-io/sqlload/commit/238364e92dad73cc06c6ff20de64d811ea6f7929))


### Performance Improvements

* **clickhouse:** hard-link touched partitions and delete the replaced rows instead of copying kept rows (ClickHouse 25.5 and later) ([8437246](https://github.com/sqlstack-io/sqlload/commit/84372464ff8ae2232ac5a3c88a2367fe55a59155))

## [0.33.0](https://github.com/sqlstack-io/sqlload/compare/v0.32.0...v0.33.0) (2026-11-09)


### Features

* **motherduck:** a MotherDuck target (beta; MotherDuck doesn't accept DuckDB 2.0 clients yet, so it can't connect until it does) ([eb4587b](https://github.com/sqlstack-io/sqlload/commit/eb4587b5137b7d5c147d75b4df07b760a1519d93))


### Documentation

* 0.31.0 and 0.32.0 on sqlload.io: marks updated as released ([#160](https://github.com/sqlstack-io/sqlload/issues/160)) ([f109c3f](https://github.com/sqlstack-io/sqlload/commit/f109c3fe98215e73b3096f40c1dd65d0ead9fefb))

## [0.32.0](https://github.com/sqlstack-io/sqlload/compare/v0.31.0...v0.32.0) (2026-11-09)


### Features

* **files:** read files sources from an SFTP server ([ec28bb8](https://github.com/sqlstack-io/sqlload/commit/ec28bb8ec15c44d9552f6fdbbf79b342eca423c5))
* **ssh:** sign in to SSH servers with a password ([3a2ce6e](https://github.com/sqlstack-io/sqlload/commit/3a2ce6e25afef9a625cf5e182607ad84fb35de20))

## [0.31.0](https://github.com/sqlstack-io/sqlload/compare/v0.30.0...v0.31.0) (2026-11-09)


### Features

* **plan:** note what an added or widened column's type loses ([ab20236](https://github.com/sqlstack-io/sqlload/commit/ab20236bfcd1a2bb34b200cb646aee5a7663038d))


### Bug Fixes

* **plan:** word every array's loss note one way, and base Snowflake's on its element type ([d8c6acd](https://github.com/sqlstack-io/sqlload/commit/d8c6acd6796e568cf09d2cef1460e6150c861dc5))


### Documentation

* 0.29.0 and 0.30.0 on sqlload.io: marks updated as released ([#157](https://github.com/sqlstack-io/sqlload/issues/157)) ([28af43c](https://github.com/sqlstack-io/sqlload/commit/28af43ca267f4727b31e432ec82ecd0f784ce4f9))

## [0.30.0](https://github.com/sqlstack-io/sqlload/compare/v0.29.0...v0.30.0) (2026-11-09)


### Features

* **bigquery:** a BigQuery source through the Storage Read API ([9e71946](https://github.com/sqlstack-io/sqlload/commit/9e71946e95039b1742ad220f04b0604afb0e9770))


### Bug Fixes

* **loader:** numeric cursor bounds compare as bare numbers ([fc91743](https://github.com/sqlstack-io/sqlload/commit/fc91743f63d103695670e47ef38c03a9261b72eb))
* **odbc:** bind text cursor bounds as parameters (affects 0.27.0–0.29.0: on a database that reads backslash escapes, a text cursor maximum holding a backslash made later incremental loads of that table fail, and other escapes could skip rows or read them again) ([#153](https://github.com/sqlstack-io/sqlload/issues/153)) ([82678f2](https://github.com/sqlstack-io/sqlload/commit/82678f20ae743535cc6eb627171f1c01b9465646))
* **snowflake:** escape backslashes in source cursor bounds (affects 0.26.0–0.29.0: a text cursor maximum holding a backslash made later incremental loads of that table fail, and other escapes could skip rows or read them again) ([9c97c62](https://github.com/sqlstack-io/sqlload/commit/9c97c62e847ec36374c9c05248459c1dc09d1ad3))

## [0.29.0](https://github.com/sqlstack-io/sqlload/compare/v0.28.0...v0.29.0) (2026-11-09)


### Features

* **plan:** note that an integer array pin cuts float list elements ([0721349](https://github.com/sqlstack-io/sqlload/commit/07213496f667534539f29b57be02eeb200791e2b))
* **plan:** note what each column's destination type loses ([c669c3e](https://github.com/sqlstack-io/sqlload/commit/c669c3e10995870559c2aaba23cf47b25e31c3ca))


### Bug Fixes

* **files:** refuse a native type for partition, path and constant columns at plan ([b62459c](https://github.com/sqlstack-io/sqlload/commit/b62459c428742c83ae02ec47112684bd472f5f62))
* **plan:** keep the files loss note for native path columns, and let mask win ([#147](https://github.com/sqlstack-io/sqlload/issues/147)) ([1b03c8a](https://github.com/sqlstack-io/sqlload/commit/1b03c8a9ad5aaac4d88270c2131ada298e2c042e))
* **plan:** refuse a native type pinned for a files target at plan ([#146](https://github.com/sqlstack-io/sqlload/issues/146)) ([b0f90e5](https://github.com/sqlstack-io/sqlload/commit/b0f90e510f599601ce526c480f6524377cfdac35))
* **python:** keep NaN and infinities in Arrow float lists pinned as arrays ([dc85521](https://github.com/sqlstack-io/sqlload/commit/dc8552148180f39382f68f3cacf41b6a8815cd27))
* **python:** pinned Arrow list columns keep their values across batches ([674c068](https://github.com/sqlstack-io/sqlload/commit/674c0689e58e8f7f1c942f8203d154d2380eaeb0))
* **snowflake:** refuse an array pin over an existing VARIANT json column ([155384e](https://github.com/sqlstack-io/sqlload/commit/155384e3f7e10176f856e792ab7c90397c2dbb56))


### Documentation

* 0.28.0 on sqlload.io: browser sign-in marked as released ([c9117c6](https://github.com/sqlstack-io/sqlload/commit/c9117c62cfb3ef35f2a775febe9bb453b3e4da5f))
* CSV files targets in the target lists, and BigQuery arrays as shipped in 0.27.0 ([23efee0](https://github.com/sqlstack-io/sqlload/commit/23efee0f2495abc9b91665da1e2af6c736d6f71e))

## [0.28.0](https://github.com/sqlstack-io/sqlload/compare/v0.27.0...v0.28.0) (2026-11-09)


### Features

* **snowflake:** sign in in the browser with OAuth (CHI-1079 slice 1) ([#137](https://github.com/sqlstack-io/sqlload/issues/137)) ([026e2e2](https://github.com/sqlstack-io/sqlload/commit/026e2e21793fdeed89bc52ea378405f023ad86de))


### Documentation

* 0.27.0 on sqlload.io: release marks and the full source list ([#142](https://github.com/sqlstack-io/sqlload/issues/142)) ([f06191b](https://github.com/sqlstack-io/sqlload/commit/f06191bc26debe11b3c95953f7cb5fb8b691419b))

## [0.27.0](https://github.com/sqlstack-io/sqlload/compare/v0.26.0...v0.27.0) (2026-11-08)


### ⚠ BREAKING CHANGES

* **loader:** text cursors compare by code point on Postgres and Snowflake sources and targets. Keys that only increase in a linguistic order (apple1, Banana2, cherry3, Date4) can fall below the maximum and stop being read; keys that increase in either order are unaffected. On Postgres a plain index on a text cursor no longer serves the read, the maximum or the deletes: create it as (col COLLATE "C").

### Features

* **cli:** apply --json and load --json print the run's result ([#135](https://github.com/sqlstack-io/sqlload/issues/135)) ([dd64b11](https://github.com/sqlstack-io/sqlload/commit/dd64b11a418a1447789d8bbcc8ec1406caed1011))
* **files:** CSV output for files targets ([#113](https://github.com/sqlstack-io/sqlload/issues/113)) ([c2083b5](https://github.com/sqlstack-io/sqlload/commit/c2083b5ef805b4af5adc89198cc7ff216722ebae))
* **files:** files source into a files target ([#127](https://github.com/sqlstack-io/sqlload/issues/127)) ([3cbaf73](https://github.com/sqlstack-io/sqlload/commit/3cbaf73ef29bdb16c4e21bcc947d2afdf53401b8))
* **files:** lease a files target table so only one load writes it ([#124](https://github.com/sqlstack-io/sqlload/issues/124)) ([e28336d](https://github.com/sqlstack-io/sqlload/commit/e28336d41df601b3beb4236bad3810d708e5452d))
* **files:** load each JSON object or CSV line whole (shape = "record") ([#121](https://github.com/sqlstack-io/sqlload/issues/121)) ([683523a](https://github.com/sqlstack-io/sqlload/commit/683523ab5c7d55f377c3cb769655ca9ecc0bc225))
* **files:** load Parquet list, struct and map columns as json ([#123](https://github.com/sqlstack-io/sqlload/issues/123)) ([ca499c7](https://github.com/sqlstack-io/sqlload/commit/ca499c7c8a8f6534cfc5e9c5f4a698b93e24fc18))
* **http:** OAuth2 client credentials and refresh-token auth for sl.http ([#129](https://github.com/sqlstack-io/sqlload/issues/129)) ([73a4ebb](https://github.com/sqlstack-io/sqlload/commit/73a4ebb40a9185f1e428fd351635304dd3635a9b))
* **http:** project JSON rows with fields and keep_record ([#131](https://github.com/sqlstack-io/sqlload/issues/131)) ([1a80c47](https://github.com/sqlstack-io/sqlload/commit/1a80c470a6de2ae72d2897dd761efe0f8fd8f54a))
* **postgres:** SSH tunnels for Postgres connections ([#111](https://github.com/sqlstack-io/sqlload/issues/111)) ([61e3b35](https://github.com/sqlstack-io/sqlload/commit/61e3b35416e389e07fc641c80e0b53cc125957eb))
* **s3:** read the AWS shared credentials file and a profile per s3 connection ([#136](https://github.com/sqlstack-io/sqlload/issues/136)) ([c9adc1e](https://github.com/sqlstack-io/sqlload/commit/c9adc1ee86e15ac330ee6e0f4c3c5c0715d2d5c0))
* **snowflake:** let DuckDB load files unloaded to an S3 stage itself ([#125](https://github.com/sqlstack-io/sqlload/issues/125)) ([f8658ba](https://github.com/sqlstack-io/sqlload/commit/f8658ba94fb1e3a4a9eac34d63a466d8b203fe09))
* **sources:** Google Sheets source ([#117](https://github.com/sqlstack-io/sqlload/issues/117)) ([28b51ba](https://github.com/sqlstack-io/sqlload/commit/28b51baa45bef8a97cbd68a06ee50a98b7c64e1e))
* **sources:** ODBC source ([#122](https://github.com/sqlstack-io/sqlload/issues/122)) ([9077d6e](https://github.com/sqlstack-io/sqlload/commit/9077d6ed0eb2eca272f7cbb0cda2b0026a705a7a))
* **types:** array(&lt;scalar&gt;) column type across destinations and sources ([#128](https://github.com/sqlstack-io/sqlload/issues/128)) ([f312cf9](https://github.com/sqlstack-io/sqlload/commit/f312cf9bde0c232b617f05804f6d387593bd78da))


### Bug Fixes

* **build:** bring Cargo.lock in line with the manifests so --locked works ([#139](https://github.com/sqlstack-io/sqlload/issues/139)) ([8b4ff84](https://github.com/sqlstack-io/sqlload/commit/8b4ff849621ae0f36c08ea6c01669f9b57c9cd3b))
* **loader:** compare text cursors by code point on source and destination ([#126](https://github.com/sqlstack-io/sqlload/issues/126)) ([1f6b66c](https://github.com/sqlstack-io/sqlload/commit/1f6b66c962005df917548b117c6334fd9712a9d4))
* **loader:** plan says an append reads after the cursor's maximum ([#116](https://github.com/sqlstack-io/sqlload/issues/116)) ([f1b20fc](https://github.com/sqlstack-io/sqlload/commit/f1b20fc4aef42fc5e10fefe967e4b4034c5be557))


### Performance Improvements

* **files:** delete files in bulk instead of one request each ([#133](https://github.com/sqlstack-io/sqlload/issues/133)) ([532214b](https://github.com/sqlstack-io/sqlload/commit/532214b9b596c139d3079397a72238b18f036204))
* **files:** stream DuckDB's direct load and read Parquet footers in one request ([#138](https://github.com/sqlstack-io/sqlload/issues/138)) ([821c517](https://github.com/sqlstack-io/sqlload/commit/821c517ecd17f8b43a919c084c270b80fc07bcc2))
* **snowflake:** have DuckDB read internal-stage unloads downloaded to a local folder ([#140](https://github.com/sqlstack-io/sqlload/issues/140)) ([ded51ac](https://github.com/sqlstack-io/sqlload/commit/ded51ac4dd37f13842884ef80302efc7f53f92a3))
* **snowflake:** list source tables with SHOW and DESCRIBE instead of the catalog views ([#134](https://github.com/sqlstack-io/sqlload/issues/134)) ([ff6fdd1](https://github.com/sqlstack-io/sqlload/commit/ff6fdd1385a98ebcc026f770a99720d2ea97c178))


### Documentation

* 0.26.0 on sqlload.io: ClickHouse and Postgres targets, checks before publish ([#132](https://github.com/sqlstack-io/sqlload/issues/132)) ([11c8d82](https://github.com/sqlstack-io/sqlload/commit/11c8d82254f4e11e793dba9cb8be93209fb163c9))

## [0.26.0](https://github.com/sqlstack-io/sqlload/compare/v0.25.0...v0.26.0) (2026-11-08)


### Features

* **clickhouse:** load into ClickHouse with exchange swaps and partition publishes ([#107](https://github.com/sqlstack-io/sqlload/issues/107)) ([dd69626](https://github.com/sqlstack-io/sqlload/commit/dd6962688b40e48aa8c4767a19bc5fba60d4f134))
* **clickhouse:** read Parquet files from S3 with s3() ([#115](https://github.com/sqlstack-io/sqlload/issues/115)) ([6f2693c](https://github.com/sqlstack-io/sqlload/commit/6f2693ce5322b0232018cf1df54cdba35aee086a))
* **files:** columns from path templates and constant columns ([#108](https://github.com/sqlstack-io/sqlload/issues/108)) ([2b07f4a](https://github.com/sqlstack-io/sqlload/commit/2b07f4aea948f1ec9a1aa4dcd22cb7d01aa57dde))
* **loader:** checks before publish (unique, not_null, min_rows, max_row_drop) ([#119](https://github.com/sqlstack-io/sqlload/issues/119)) ([11b705b](https://github.com/sqlstack-io/sqlload/commit/11b705b185df9c2c67f4940f0f1915b1f4682f4f))
* **postgres:** load into Postgres targets ([#109](https://github.com/sqlstack-io/sqlload/issues/109)) ([6f430f7](https://github.com/sqlstack-io/sqlload/commit/6f430f789becea24fb100905c5e0b017ebbfeb8a))
* **snowflake:** Snowflake as a source, read by unload ([#110](https://github.com/sqlstack-io/sqlload/issues/110)) ([b704369](https://github.com/sqlstack-io/sqlload/commit/b704369b1bd0b01c3c6dbb75be3e9876bb0696a1))


### Documentation

* document Postgres TLS and the append cursor default in 0.25.0 ([#114](https://github.com/sqlstack-io/sqlload/issues/114)) ([977e8d4](https://github.com/sqlstack-io/sqlload/commit/977e8d422a0e63429aad03d106cca84c1f82ec95))
* **snowflake:** mark the Snowflake source as from 0.26.0, not released yet ([#120](https://github.com/sqlstack-io/sqlload/issues/120)) ([80d913c](https://github.com/sqlstack-io/sqlload/commit/80d913c9971581cc5c40aa355cbaf07b6ea64a17))

## [0.25.0](https://github.com/sqlstack-io/sqlload/compare/v0.24.0...v0.25.0) (2026-11-08)


### ⚠ BREAKING CHANGES

* **loader:** `append` with a cursor reads after the destination's maximum (`>`) instead of from it (`>=`), so re-runs no longer append the rows at the maximum again. Set `append_cursor_inclusive = true` to keep the old read. `append` with a `date` cursor is now refused; use `delete_insert` or `merge`. Python table functions see `ctx.cursor_inclusive = False` on re-runs.

### Features

* **postgres:** TLS for Postgres sources and replication ([#105](https://github.com/sqlstack-io/sqlload/issues/105)) ([d4d7b16](https://github.com/sqlstack-io/sqlload/commit/d4d7b16100c3ee8322b22b5b15f17231141f3ce3))


### Bug Fixes

* **loader:** append reads after the cursor's maximum by default ([#106](https://github.com/sqlstack-io/sqlload/issues/106)) ([00e8632](https://github.com/sqlstack-io/sqlload/commit/00e8632f49a148c96f25618d4547bfc30774c446))
* **python:** name a files-target table by its folder when refusing to empty it ([#102](https://github.com/sqlstack-io/sqlload/issues/102)) ([ce22a00](https://github.com/sqlstack-io/sqlload/commit/ce22a007b726824333f15ace5ddce02c49cd725e))


### Documentation

* document the BigQuery target and 0.24.0 ([#103](https://github.com/sqlstack-io/sqlload/issues/103)) ([91e911a](https://github.com/sqlstack-io/sqlload/commit/91e911aedd5a09ce1905f55cf23ae3dffe9abb4b))

## [0.24.0](https://github.com/sqlstack-io/sqlload/compare/v0.23.0...v0.24.0) (2026-11-08)


### Features

* **bigquery:** load rows through the Storage Write API ([#100](https://github.com/sqlstack-io/sqlload/issues/100)) ([d4565d9](https://github.com/sqlstack-io/sqlload/commit/d4565d9e7939ba6d9d92b7b0b53741872d9b857d))
* **bigquery:** plan, apply and diff BigQuery targets, with the Storage Write API transport ([#98](https://github.com/sqlstack-io/sqlload/issues/98)) ([314c555](https://github.com/sqlstack-io/sqlload/commit/314c55521d487440c08da5d1faa7338566630e25))


### Bug Fixes

* **cdc:** name files-target tables by their folder in the load tally ([#101](https://github.com/sqlstack-io/sqlload/issues/101)) ([c9cc953](https://github.com/sqlstack-io/sqlload/commit/c9cc9539122e039c03ae39e88a55662f0750438e))


### Documentation

* document Postgres CDC into Databricks in 0.23.0 ([#97](https://github.com/sqlstack-io/sqlload/issues/97)) ([9fcd64f](https://github.com/sqlstack-io/sqlload/commit/9fcd64ff1d2f141e4aa806fc42218b1c4d02d843))

## [0.23.0](https://github.com/sqlstack-io/sqlload/compare/v0.22.0...v0.23.0) (2026-11-07)


### Features

* **cdc:** apply Postgres changes to Databricks targets ([#96](https://github.com/sqlstack-io/sqlload/issues/96)) ([1b46ef7](https://github.com/sqlstack-io/sqlload/commit/1b46ef77492796f4e0fc5a0356ab855790352e62))


### Documentation

* document --reload refusing Postgres CDC tables in 0.22.0 ([#94](https://github.com/sqlstack-io/sqlload/issues/94)) ([4eaf794](https://github.com/sqlstack-io/sqlload/commit/4eaf7941eabc4c5038c65b9e1bf5b84a1dc122de))

## [0.22.0](https://github.com/sqlstack-io/sqlload/compare/v0.21.0...v0.22.0) (2026-11-07)


### ⚠ BREAKING CHANGES

* **cdc:** a bare `load --reload` on a project with a postgres_cdc source now refuses that source's tables and exits 1, while the project's other tables still rebuild. Use `--resnapshot` to copy captured tables again, or `--exclude` the CDC source to reload the rest.

### Bug Fixes

* **cdc:** refuse --reload for captured tables on every target ([#91](https://github.com/sqlstack-io/sqlload/issues/91)) ([e57f6d0](https://github.com/sqlstack-io/sqlload/commit/e57f6d0bfb29835ef94e8831d2e6cc71b6490844))
* **loader:** scope a source's problems to commands that select it ([#90](https://github.com/sqlstack-io/sqlload/issues/90)) ([afef55d](https://github.com/sqlstack-io/sqlload/commit/afef55d5d5f1558566c114828c0fc803bf2ac235))


### Documentation

* document Postgres CDC into files targets in 0.21.0 ([#93](https://github.com/sqlstack-io/sqlload/issues/93)) ([f298876](https://github.com/sqlstack-io/sqlload/commit/f298876a8faf79fb86e52ca9e08a4fc2d19444bb))

## [0.21.0](https://github.com/sqlstack-io/sqlload/compare/v0.20.0...v0.21.0) (2026-11-07)


### Features

* **cdc:** load a Postgres CDC changelog into files targets ([#88](https://github.com/sqlstack-io/sqlload/issues/88)) ([f283bcb](https://github.com/sqlstack-io/sqlload/commit/f283bcbaccc0e08de7fde684aec93737856b40c8))


### Bug Fixes

* **cdc:** say re-planning, not run it again, when --follow plans again ([#89](https://github.com/sqlstack-io/sqlload/issues/89)) ([adff9f4](https://github.com/sqlstack-io/sqlload/commit/adff9f458edabf7ef6f29c4b5efb205833222989))


### Documentation

* document load --follow for Postgres CDC in 0.20.0 ([#86](https://github.com/sqlstack-io/sqlload/issues/86)) ([e1b5b25](https://github.com/sqlstack-io/sqlload/commit/e1b5b253ef27a0d7f7a8e703ffc0a176550bda19))

## [0.20.0](https://github.com/sqlstack-io/sqlload/compare/v0.19.0...v0.20.0) (2026-11-07)


### Features

* **cdc:** keep streaming with load --follow ([#84](https://github.com/sqlstack-io/sqlload/issues/84)) ([0a58247](https://github.com/sqlstack-io/sqlload/commit/0a5824773ad9b979fa6ecc61c18eb57f1d21c739))


### Documentation

* **readme:** give the README a landing page like the other SQLStack tools ([#83](https://github.com/sqlstack-io/sqlload/issues/83)) ([08e2ec6](https://github.com/sqlstack-io/sqlload/commit/08e2ec6df4a4f9dcef503297b5715ae508113a5a))
* **site:** add a social preview image ([#80](https://github.com/sqlstack-io/sqlload/issues/80)) ([4d127ad](https://github.com/sqlstack-io/sqlload/commit/4d127ad4a9a89ac612b4f2ea2e4e1b77d19afc60))
* **site:** document Postgres CDC tables joining a source in 0.19.0 ([#85](https://github.com/sqlstack-io/sqlload/issues/85)) ([d0f6706](https://github.com/sqlstack-io/sqlload/commit/d0f67061f222cfd74beeeb1298746931580337a2))
* **site:** set the wordmark in Space Grotesk Bold and adopt the new tagline ([#82](https://github.com/sqlstack-io/sqlload/issues/82)) ([1967ddb](https://github.com/sqlstack-io/sqlload/commit/1967ddb61a1de1ca4bc9d2cbb32dc4e1d7aaaea2))

## [0.19.0](https://github.com/sqlstack-io/sqlload/compare/v0.18.0...v0.19.0) (2026-11-07)


### Features

* **cdc:** copy tables that join a streaming source alone ([#77](https://github.com/sqlstack-io/sqlload/issues/77)) ([3956117](https://github.com/sqlstack-io/sqlload/commit/395611710401c1661b316999633769d793e25835))


### Documentation

* **site:** document Postgres CDC schema changes in 0.18.0 ([#78](https://github.com/sqlstack-io/sqlload/issues/78)) ([ae3ea81](https://github.com/sqlstack-io/sqlload/commit/ae3ea8118a76590cf12c8e0d85589638d9ca55eb))

## [0.18.0](https://github.com/sqlstack-io/sqlload/compare/v0.17.0...v0.18.0) (2026-11-07)


### Features

* **cdc:** load source schema changes while streaming ([#76](https://github.com/sqlstack-io/sqlload/issues/76)) ([e5a551f](https://github.com/sqlstack-io/sqlload/commit/e5a551f501c06ab72f71b96717f522968daa8e56))


### Bug Fixes

* **postgres:** show what Postgres said when it refuses a statement ([#75](https://github.com/sqlstack-io/sqlload/issues/75)) ([64c7fc4](https://github.com/sqlstack-io/sqlload/commit/64c7fc40c400da1fb4c149c3ff959c97cb7826f5))


### Documentation

* **site:** add the Postgres CDC source page ([#73](https://github.com/sqlstack-io/sqlload/issues/73)) ([b5da2be](https://github.com/sqlstack-io/sqlload/commit/b5da2be560948c7ae66f6a5e3f9211e687cf5e7e))

## [0.17.0](https://github.com/sqlstack-io/sqlload/compare/v0.16.0...v0.17.0) (2026-11-07)


### Features

* **cdc:** spill large transactions and batches to local disk ([#70](https://github.com/sqlstack-io/sqlload/issues/70)) ([04dbe02](https://github.com/sqlstack-io/sqlload/commit/04dbe02835267a55497a015b5e2d796cbd343dee))


### Bug Fixes

* **cdc:** apply a relation message at its place in the transaction ([#72](https://github.com/sqlstack-io/sqlload/issues/72)) ([c4d1f71](https://github.com/sqlstack-io/sqlload/commit/c4d1f71abb0c00b74851f71cdca41fdfab335650))

## [0.16.0](https://github.com/sqlstack-io/sqlload/compare/v0.15.0...v0.16.0) (2026-11-07)


### Features

* **cdc:** Postgres change data capture into DuckDB and Snowflake (slice 1) ([#68](https://github.com/sqlstack-io/sqlload/issues/68)) ([efaede4](https://github.com/sqlstack-io/sqlload/commit/efaede4e335a12b581b41ebfc199cb413d7a3ca3))

## [0.15.0](https://github.com/sqlstack-io/sqlload/compare/v0.14.0...v0.15.0) (2026-11-07)


### ⚠ BREAKING CHANGES

* **cli:** sqlload state is now sqlload logged-state, with no alias. Replace sqlload state show|verify|reconcile|reset with sqlload logged-state show|verify|reconcile|reset.

### Features

* **cli:** rename sqlload state to sqlload logged-state ([#67](https://github.com/sqlstack-io/sqlload/issues/67)) ([78f7152](https://github.com/sqlstack-io/sqlload/commit/78f715213d9bb01451114ff21a0e231fa6cbc997))


### Documentation

* rewrite the README ([#55](https://github.com/sqlstack-io/sqlload/issues/55)) ([2df26d2](https://github.com/sqlstack-io/sqlload/commit/2df26d2027d6bac1ba1e5f545a022ffa62df23bc))
* **site:** add the Databricks target page and list Databricks as supported ([#66](https://github.com/sqlstack-io/sqlload/issues/66)) ([9a940f0](https://github.com/sqlstack-io/sqlload/commit/9a940f03ed97573f5a25bfa2b2948f9ae04c25f4))
* **site:** add the SQLLoad website and documentation ([#63](https://github.com/sqlstack-io/sqlload/issues/63)) ([6f232e2](https://github.com/sqlstack-io/sqlload/commit/6f232e269de9276e0c96914d1d9b756114ac2633))

## [0.14.0](https://github.com/sqlstack-io/sqlload/compare/v0.13.0...v0.14.0) (2026-11-06)


### Features

* **contract:** diff --source compares what each source would load with its live table ([#61](https://github.com/sqlstack-io/sqlload/issues/61)) ([c5a39d7](https://github.com/sqlstack-io/sqlload/commit/c5a39d7168cad47f884cec846e9fe5c7e908ccf1))
* **databricks:** Databricks SQL warehouse target through a Unity Catalog volume ([#64](https://github.com/sqlstack-io/sqlload/issues/64)) ([9e5dcfd](https://github.com/sqlstack-io/sqlload/commit/9e5dcfd32ec8cf973f21873704941d227b96b26f))

## [0.13.0](https://github.com/sqlstack-io/sqlload/compare/v0.12.0...v0.13.0) (2026-11-06)


### Features

* **contract:** compare files targets and report drift with --all-files ([#58](https://github.com/sqlstack-io/sqlload/issues/58)) ([6ff9802](https://github.com/sqlstack-io/sqlload/commit/6ff98029c3aaa0c9256ce6222866b9051d347573))

## [0.12.0](https://github.com/sqlstack-io/sqlload/compare/v0.11.1...v0.12.0) (2026-11-06)


### Features

* **cli:** sqlload contract generate proposes and writes declared columns from live tables ([#57](https://github.com/sqlstack-io/sqlload/issues/57)) ([d29f3a5](https://github.com/sqlstack-io/sqlload/commit/d29f3a5b53ac2cbaebe8ca7ad3b3de9185f1b237))


### Bug Fixes

* **snowflake:** copy_to_internal_stage matches staged files by their @~/ names ([#56](https://github.com/sqlstack-io/sqlload/issues/56)) ([804de24](https://github.com/sqlstack-io/sqlload/commit/804de2498cc46614badbbcda925169f015a51a2a))


### Documentation

* **bench:** publish benchmark results where SQLLoad wins or is at parity ([#53](https://github.com/sqlstack-io/sqlload/issues/53)) ([8a26279](https://github.com/sqlstack-io/sqlload/commit/8a2627981372571a591db25b00af0e4d96db4c38))

## [0.11.1](https://github.com/sqlstack-io/sqlload/compare/v0.11.0...v0.11.1) (2026-11-06)


### Performance Improvements

* **files:** read the schema sample's footers and records a few at a time ([#51](https://github.com/sqlstack-io/sqlload/issues/51)) ([5ab2b9e](https://github.com/sqlstack-io/sqlload/commit/5ab2b9ea263f6791b33740a9ccdcedae33aeb675))

## [0.11.0](https://github.com/sqlstack-io/sqlload/compare/v0.10.0...v0.11.0) (2026-11-06)


### Features

* **duckdb:** DuckDB reads Parquet in S3 or a local folder itself ([#49](https://github.com/sqlstack-io/sqlload/issues/49)) ([07a6510](https://github.com/sqlstack-io/sqlload/commit/07a65104e7d0410f41b1ba0b3b892631e8eab96d))

## [0.10.0](https://github.com/sqlstack-io/sqlload/compare/v0.9.0...v0.10.0) (2026-11-06)


### Features

* **cli:** sqlload contract diff compares declared columns with live tables ([#47](https://github.com/sqlstack-io/sqlload/issues/47)) ([6a097b7](https://github.com/sqlstack-io/sqlload/commit/6a097b7aca943f45072b0286542ed262004aff12))
* **cli:** sqlload copy runs one table between named connections or URIs ([#46](https://github.com/sqlstack-io/sqlload/issues/46)) ([b64c378](https://github.com/sqlstack-io/sqlload/commit/b64c37855a382b10a992ec9406257f4889e069a8))

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
