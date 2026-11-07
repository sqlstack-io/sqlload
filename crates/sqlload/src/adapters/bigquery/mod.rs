//! BigQuery as a loader destination: its SQL dialect and type names, and SQL run as query jobs.

pub(crate) mod _helpers;
pub mod constants;
pub mod main;
pub mod models;

#[cfg(test)]
mod tests;
