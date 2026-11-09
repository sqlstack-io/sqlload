# Changelog

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
