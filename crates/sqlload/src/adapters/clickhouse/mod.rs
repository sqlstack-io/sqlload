//! ClickHouse as a loader destination: its SQL dialect, type names and table layout, statements sent over its HTTP interface, rows inserted as Arrow, and each write published by swapping whole tables or partitions in.

pub(crate) mod _helpers;
pub mod constants;
pub mod main;
pub mod models;

#[cfg(test)]
mod tests;
