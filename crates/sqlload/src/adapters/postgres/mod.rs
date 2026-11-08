//! Postgres as a loader destination: its SQL dialect and type names, Arrow batches sent by binary `COPY` into a temporary staging table on each table's own session, and every write applied from it in one transaction.

pub(crate) mod _helpers;
pub mod constants;
pub mod main;
pub mod models;

#[cfg(test)]
mod tests;
