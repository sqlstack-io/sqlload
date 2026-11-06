//! The Databricks REST client: OAuth machine-to-machine tokens, the SQL Statement Execution API and the Files API for Unity Catalog volumes.

pub(crate) mod _helpers;
pub mod constants;
pub mod main;
pub mod models;

#[cfg(test)]
mod tests;
