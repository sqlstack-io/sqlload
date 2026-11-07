//! The BigQuery REST client: OAuth tokens from a service-account key, query jobs and table metadata.

pub(crate) mod _helpers;
pub mod constants;
pub mod main;
pub mod models;

#[cfg(test)]
mod tests;
