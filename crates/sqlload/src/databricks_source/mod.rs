//! Databricks as a source: tables and views that `include` finds in a Unity Catalog catalog, and query tables, read as Arrow through the Statement Execution API's result links.

pub(crate) mod _helpers;
pub mod constants;
pub mod errors;
pub mod main;
pub mod models;

#[cfg(test)]
mod tests;
