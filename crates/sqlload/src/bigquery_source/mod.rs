//! BigQuery as a source: tables and views that `include` finds, and query tables, read as Arrow through the Storage Read API.

pub(crate) mod _helpers;
pub mod constants;
pub mod errors;
pub mod main;
pub mod models;

#[cfg(test)]
mod tests;
