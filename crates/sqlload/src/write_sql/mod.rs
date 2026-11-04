//! SQL for creating, changing and writing destination tables, per warehouse dialect.

pub(crate) mod _helpers;
pub mod constants;
pub mod main;
pub mod models;

#[cfg(test)]
mod tests;
