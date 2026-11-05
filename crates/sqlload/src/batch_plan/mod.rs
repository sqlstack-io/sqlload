//! Where a load's commits fall: cursor windows, groups of a files source's partitions, and cut points between distinct cursor values.

pub mod constants;
pub mod main;
pub mod models;

#[cfg(test)]
mod tests;
