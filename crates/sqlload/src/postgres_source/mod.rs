//! The native Postgres reader: catalog discovery and binary `COPY` into Arrow batches.

pub(crate) mod _helpers;
pub mod constants;
pub mod errors;
pub mod main;
pub mod models;

#[cfg(test)]
mod tests;
