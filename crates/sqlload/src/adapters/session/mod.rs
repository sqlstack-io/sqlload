//! A destination SQLLoad writes through a connection its caller owns. SQLLoad still plans the load, renders every statement in the warehouse's dialect and decides every write; the caller runs the statements on its own connection, with its own credentials, and takes rows in the files SQLLoad writes.

pub(crate) mod _helpers;
pub mod errors;
pub mod main;
pub mod models;
pub mod types;

#[cfg(all(test, feature = "duckdb"))]
mod tests;
