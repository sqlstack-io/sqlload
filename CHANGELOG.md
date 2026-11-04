# Changelog

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
