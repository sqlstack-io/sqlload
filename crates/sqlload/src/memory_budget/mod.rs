//! One memory budget for a run: where it comes from (`memory_limit`, `--memory-limit`, or a share of the limit the OS sets), how it is shared between DuckDB and the tables loading at once, and the watchdog that keeps the process under it.

pub(crate) mod _helpers;
pub mod constants;
pub mod main;
pub mod models;

#[cfg(test)]
mod tests;
