//! Per-stream batching and compressed request splitting.

pub(crate) mod _helpers;
pub mod main;
pub mod models;

#[cfg(test)]
mod tests;
