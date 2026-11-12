//! The MotherDuck bridge: SQL and batches run on Python's `duckdb` package in a separate process, as MotherDuck doesn't accept SQLLoad's own DuckDB yet; `website/src/content/docs/docs/targets/motherduck.mdx` says how to remove it.

pub(crate) mod _helpers;
pub mod constants;
pub mod errors;
pub mod main;
pub mod models;
