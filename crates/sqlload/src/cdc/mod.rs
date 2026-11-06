//! Postgres change data capture: pgoutput decoding, whole-transaction batches and the apply rules behind `kind = "postgres_cdc"`.

pub(crate) mod _helpers;
pub mod main;
pub mod models;

#[cfg(test)]
mod tests;
